{-# LANGUAGE OverloadedStrings #-}

-- | SQL execution engine. Translates parsed 'Statement's into durable
-- database operations and returns structured 'QueryResult's.
module HsDb.SQL.Execute
  ( executeSQL
  , QueryResult(..)
  , ColumnInfo(..)
  ) where

import Control.Concurrent.STM (STM, atomically, readTVar, takeTMVar)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, throwE, except, withExceptT)
import Data.Bits (shiftR, (.&.))
import Data.Char (chr, ord, toLower)
import Data.Int (Int32, Int64)
import qualified Data.ByteString as BS
import Data.List (sortBy)
import qualified Data.Set as Set
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word8)

import HsDb.Types
import HsDb.Integration (Database(..), selectAllSTM, createTableSTM,
                          insertRowSTM, updateRowSTM, deleteRowSTM, dropTableSTM,
                          alterAddColumnSTM,
                          atomicallyE)
import HsDb.Table (Table(..), TableCatalog, validateRow)
import HsDb.Transaction
import HsDb.SQL.Types as SQL

-- | Column metadata for result sets.
data ColumnInfo = ColumnInfo
  { ciName :: !Text
  , ciType :: !ColumnType
  } deriving (Show, Eq)

-- | Result of executing a SQL statement.
data QueryResult
  = CommandComplete !Text
    -- ^ DDL/DML result tag (e.g., "CREATE TABLE", "INSERT 0 1", "DELETE 3")
  | RowResult [ColumnInfo] [[Maybe Text]]
    -- ^ SELECT result: column info + rows of text-formatted values (Nothing = NULL)
  deriving (Show, Eq)

-- | Bridge ExceptT DbError STM to ExceptT Text IO.
atomicallyDbError :: ExceptT DbError STM a -> ExceptT Text IO a
atomicallyDbError = withExceptT (T.pack . show) . atomicallyE

-- | Execute a parsed SQL statement against a Database.
-- The Maybe TxState tracks whether we're inside a BEGIN block.
executeSQL :: Database -> Maybe TxState -> Statement -> ExceptT Text IO QueryResult
executeSQL db mtx stmt = case stmt of
  CreateTable name colDefs -> execCreateTable db mtx name colDefs
  DropTable name           -> execDropTable db mtx name
  Insert name cols valRows -> execInsert db mtx name cols valRows
  Select dist targets name wh orderBy limit offset
                             -> execSelect db mtx dist name targets wh orderBy limit offset
  Update name assigns wh   -> execUpdate db mtx name assigns wh
  Delete name wh           -> execDelete db mtx name wh
  AlterTableAddColumn name colDef -> execAlterAddColumn db mtx name colDef
  Explain inner             -> execExplain inner
  Begin    -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"
  Commit   -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"
  Rollback -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"

-- CREATE TABLE

execCreateTable :: Database -> Maybe TxState -> Text -> [ColumnDef] -> ExceptT Text IO QueryResult
execCreateTable db Nothing name colDefs = do
  let schema = V.fromList (map columnDefToColumn colDefs)
  (_, callback) <- atomicallyDbError $ createTableSTM db name schema
  lift $ atomically $ takeTMVar callback
  return (CommandComplete "CREATE TABLE")
execCreateTable _db (Just tx) name colDefs = do
  let schema = V.fromList (map columnDefToColumn colDefs)
  lift $ addPendingOp tx (PendingCreate name schema)
  return (CommandComplete "CREATE TABLE")

-- DROP TABLE

execDropTable :: Database -> Maybe TxState -> Text -> ExceptT Text IO QueryResult
execDropTable db Nothing name = do
  callback <- atomicallyDbError $ dropTableSTM db name
  lift $ atomically $ takeTMVar callback
  return (CommandComplete "DROP TABLE")
execDropTable db (Just tx) name = do
  -- Verify table exists (committed or pending create)
  schema <- lift $ effectiveSchema (dbCatalog db) tx name
  case schema of
    Left err -> throwE err
    Right _  -> do
      lift $ addPendingOp tx (PendingDrop name)
      return (CommandComplete "DROP TABLE")

-- INSERT

execInsert :: Database -> Maybe TxState -> Text -> [Text] -> [[Literal]] -> ExceptT Text IO QueryResult
execInsert db Nothing name cols valRows = do
  schema <- getTableSchema (dbCatalog db) name
  colOrder <- except $ resolveColumns schema cols
  n <- insertRowsDirect db name schema colOrder valRows 0
  return (CommandComplete ("INSERT 0 " <> T.pack (show n)))
execInsert db (Just tx) name cols valRows = do
  schema <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
  colOrder <- except $ resolveColumns schema cols
  n <- insertRowsTx tx name schema colOrder valRows 0
  return (CommandComplete ("INSERT 0 " <> T.pack (show n)))

insertRowsDirect :: Database -> Text -> Schema -> [Int] -> [[Literal]] -> Int
                 -> ExceptT Text IO Int
insertRowsDirect _ _ _ _ [] n = return n
insertRowsDirect db name schema colOrder litRows n = do
  rows <- mapM (\lits -> except $ buildRow schema colOrder lits) litRows
  callbacks <- atomicallyDbError $ mapM (insertRowSTM db name) rows
  let cbs = map snd callbacks
  case cbs of
    [] -> return ()
    _  -> lift $ atomically $ takeTMVar (last cbs)
  return (n + length litRows)

insertRowsTx :: TxState -> Text -> Schema -> [Int] -> [[Literal]] -> Int
             -> ExceptT Text IO Int
insertRowsTx _ _ _ _ [] n = return n
insertRowsTx tx name schema colOrder (lits:rest) n = do
  row <- except $ buildRow schema colOrder lits
  except $ validateRowE schema row
  lift $ addPendingOp tx (PendingInsert name row)
  insertRowsTx tx name schema colOrder rest (n + 1)

-- SELECT

execSelect :: Database -> Maybe TxState -> Bool -> Text -> [SelectTarget] -> Maybe Expr
           -> [OrderByClause] -> Maybe Int -> Maybe Int
           -> ExceptT Text IO QueryResult
execSelect db mtx dist name targets wh orderBy mLimit mOffset = do
  (schema, rows) <- case mtx of
    Nothing -> do
      s <- getTableSchema (dbCatalog db) name
      r <- atomicallyDbError $ selectAllSTM db name
      return (s, r)
    Just tx -> do
      s <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
      r <- lift (effectiveRows (dbCatalog db) tx name) >>= liftEither
      return (s, r)
  let filtered = filterRows schema wh rows
  -- Check if we have aggregates
  let hasAgg = any isAggTarget targets
  if hasAgg
    then do
      (colInfos, aggRow) <- except $ computeAggregates schema targets filtered
      return (RowResult colInfos [aggRow])
    else do
      (colInfos, colIdxs) <- except $ resolveSelectTargets schema targets
      sorted <- case orderBy of
        [] -> return filtered
        _  -> do
          orderIdxs <- except $ resolveOrderByColumns schema orderBy
          return (sortRows orderIdxs filtered)
      let afterOffset = maybe id drop mOffset sorted
      let afterLimit  = maybe id take mLimit afterOffset
      let projected = map (\(_, row) -> projectRow colIdxs row) afterLimit
      let textRows = map (map formatValue) projected
      let deduped = if dist then ordNub textRows else textRows
      return (RowResult colInfos deduped)

-- UPDATE

execUpdate :: Database -> Maybe TxState -> Text -> [(Text, Expr)] -> Maybe Expr
           -> ExceptT Text IO QueryResult
execUpdate db Nothing name assigns wh = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  let matching = filterRows schema wh rows
  n <- updateRowsDirect db name schema assigns matching 0
  return (CommandComplete ("UPDATE " <> T.pack (show n)))
execUpdate db (Just tx) name assigns wh = do
  schema <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
  rowsResult <- lift $ effectiveRows (dbCatalog db) tx name
  rows <- liftEither rowsResult
  let matching = filterRows schema wh rows
  n <- updateRowsTx tx name schema assigns matching 0
  return (CommandComplete ("UPDATE " <> T.pack (show n)))

updateRowsDirect :: Database -> Text -> Schema -> [(Text, Expr)] -> [(RowId, Row)] -> Int
                 -> ExceptT Text IO Int
updateRowsDirect _ _ _ _ [] n = return n
updateRowsDirect db name schema assigns matching n = do
  updates <- mapM (\(rowId, row) -> do
    newRow <- except $ applyAssignments schema assigns row
    return (rowId, newRow)) matching
  cbs <- atomicallyDbError $ mapM (\(rowId, newRow) -> updateRowSTM db name rowId newRow) updates
  case cbs of
    [] -> return ()
    _  -> lift $ atomically $ takeTMVar (last cbs)
  return (n + length matching)

updateRowsTx :: TxState -> Text -> Schema -> [(Text, Expr)] -> [(RowId, Row)] -> Int
             -> ExceptT Text IO Int
updateRowsTx _ _ _ _ [] n = return n
updateRowsTx tx name schema assigns ((rowId, row):rest) n = do
  newRow <- except $ applyAssignments schema assigns row
  lift $ addPendingOp tx (PendingUpdate name rowId newRow)
  updateRowsTx tx name schema assigns rest (n + 1)

-- DELETE

execDelete :: Database -> Maybe TxState -> Text -> Maybe Expr -> ExceptT Text IO QueryResult
execDelete db Nothing name wh = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  let matching = filterRows schema wh rows
  n <- deleteRowsDirect db name matching 0
  return (CommandComplete ("DELETE " <> T.pack (show n)))
execDelete db (Just tx) name wh = do
  schema <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
  rowsResult <- lift $ effectiveRows (dbCatalog db) tx name
  rows <- liftEither rowsResult
  let matching = filterRows schema wh rows
  n <- deleteRowsTx tx name matching 0
  return (CommandComplete ("DELETE " <> T.pack (show n)))

deleteRowsDirect :: Database -> Text -> [(RowId, Row)] -> Int -> ExceptT Text IO Int
deleteRowsDirect _ _ [] n = return n
deleteRowsDirect db name matching n = do
  cbs <- atomicallyDbError $ mapM (\(rowId, _) -> deleteRowSTM db name rowId) matching
  case cbs of
    [] -> return ()
    _  -> lift $ atomically $ takeTMVar (last cbs)
  return (n + length matching)

deleteRowsTx :: TxState -> Text -> [(RowId, Row)] -> Int -> ExceptT Text IO Int
deleteRowsTx _ _ [] n = return n
deleteRowsTx tx name ((rowId, _):rest) n = do
  lift $ addPendingOp tx (PendingDelete name rowId)
  deleteRowsTx tx name rest (n + 1)

-- Shared helpers

filterRows :: Schema -> Maybe Expr -> [(RowId, Row)] -> [(RowId, Row)]
filterRows _ Nothing rows = rows
filterRows schema (Just expr) rows =
  filter (\(_, row) -> evalWhere schema expr row) rows

liftEither :: Either Text a -> ExceptT Text IO a
liftEither (Left e)  = throwE e
liftEither (Right a) = return a

validateRowE :: Schema -> Vector Value -> Either Text ()
validateRowE schema row = case validateRow schema row of
  Left err -> Left (T.pack (show err))
  Right () -> Right ()

columnDefToColumn :: ColumnDef -> Column
columnDefToColumn cd = HsDb.Types.Column
  { columnName     = cdName cd
  , columnType     = sqlTypeToColumnType (cdType cd)
  , columnNullable = cdNullable cd
  }

sqlTypeToColumnType :: SqlType -> ColumnType
sqlTypeToColumnType SqlInt    = TInt32
sqlTypeToColumnType SqlBigInt = TInt64
sqlTypeToColumnType SqlFloat  = TFloat64
sqlTypeToColumnType SqlText   = TText
sqlTypeToColumnType SqlBool   = TBool
sqlTypeToColumnType SqlBytea  = TBytea

getTableSchema :: TableCatalog -> Text -> ExceptT Text IO Schema
getTableSchema catalog name = do
  tables <- lift $ atomically $ readTVar catalog
  case Map.lookup name tables of
    Nothing    -> throwE ("Table '" <> name <> "' does not exist")
    Just table -> return (tableSchema table)

-- Map column names from INSERT to schema positions.
resolveColumns :: Schema -> [Text] -> Either Text [Int]
resolveColumns schema cols = mapM findCol cols
  where
    findCol col = case Map.lookup col indexed of
      Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      Just i  -> Right i
    indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]

-- Build a row vector from literals, mapped to schema column positions.
buildRow :: Schema -> [Int] -> [Literal] -> Either Text (Vector Value)
buildRow schema colOrder lits
  | length lits /= length colOrder =
      Left ("Expected " <> T.pack (show (length colOrder))
            <> " values, got " <> T.pack (show (length lits)))
  | otherwise = do
      let schemaLen = V.length schema
          base = V.replicate schemaLen VNull
          pairs = zip colOrder lits
      vals <- mapM (\(i, lit) -> do
        let col = schema V.! i
        v <- literalToValue (columnType col) lit
        return (i, v)) pairs
      return (V.accum (\_ v -> v) base vals)

literalToValue :: ColumnType -> Literal -> Either Text Value
literalToValue _ LitNull           = Right VNull
literalToValue TInt32 (LitInt n)   = Right (VInt32 (fromIntegral n :: Int32))
literalToValue TInt64 (LitInt n)   = Right (VInt64 (fromIntegral n :: Int64))
literalToValue TFloat64 (LitInt n) = Right (VFloat64 (fromIntegral n))
literalToValue TFloat64 (LitFloat d) = Right (VFloat64 d)
literalToValue TText (LitText t)   = Right (VText t)
literalToValue TBool (LitBool b)   = Right (VBool b)
literalToValue ty lit              = Left ("Cannot convert " <> T.pack (show lit)
                                           <> " to " <> T.pack (show ty))

resolveSelectTargets :: Schema -> [SelectTarget] -> Either Text ([ColumnInfo], [Int])
resolveSelectTargets schema [Star] =
  let infos = [ColumnInfo (columnName c) (columnType c) | c <- V.toList schema]
      idxs  = [0 .. V.length schema - 1]
  in Right (infos, idxs)
resolveSelectTargets schema targets =
  let indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]
      resolve (SQL.Column col) =
        case Map.lookup col indexed of
          Just i  -> let c = schema V.! i
                     in Right (ColumnInfo (columnName c) (columnType c), i)
          Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      resolve (ColumnAs col alias) =
        case Map.lookup col indexed of
          Just i  -> let c = schema V.! i
                     in Right (ColumnInfo alias (columnType c), i)
          Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      resolve Star = Left "* cannot be mixed with column names"
      resolve (Agg _ _) = Left "Aggregate functions handled separately"
  in do
      pairs <- mapM resolve targets
      return (map fst pairs, map snd pairs)

-- | O(n log n) deduplication preserving order.
ordNub :: Ord a => [a] -> [a]
ordNub = go Set.empty
  where
    go _ [] = []
    go seen (x:xs)
      | Set.member x seen = go seen xs
      | otherwise         = x : go (Set.insert x seen) xs

projectRow :: [Int] -> Row -> [Value]
projectRow idxs row = map (\i -> row V.! i) idxs

formatValue :: Value -> Maybe Text
formatValue (VInt32 n)   = Just (T.pack (show n))
formatValue (VInt64 n)   = Just (T.pack (show n))
formatValue (VFloat64 d) = Just (T.pack (show d))
formatValue (VText t)    = Just t
formatValue (VBool True) = Just "t"
formatValue (VBool False)= Just "f"
formatValue (VBytea bs)  = Just ("\\x" <> T.pack (concatMap byteToHex (BS.unpack bs)))
formatValue VNull        = Nothing

byteToHex :: Word8 -> String
byteToHex w = [hexDigit (w `shiftR` 4), hexDigit (w .&. 0xf)]
  where
    hexDigit n
      | n < 10    = chr (fromIntegral n + ord '0')
      | otherwise = chr (fromIntegral n - 10 + ord 'a')

-- WHERE clause evaluation

evalWhere :: Schema -> Expr -> Row -> Bool
evalWhere schema expr row = case evalExpr schema row expr of
  VBool b -> b
  _       -> False

evalExpr :: Schema -> Row -> Expr -> Value
evalExpr _ _ (ExprLit lit) = litToValue lit
evalExpr schema row (ExprColumn col) =
  case Map.lookup col indexed of
    Just i  -> row V.! i
    Nothing -> VNull
  where indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]
evalExpr schema row (ExprIsNull e) =
  VBool (evalExpr schema row e == VNull)
evalExpr schema row (ExprIsNotNull e) =
  VBool (evalExpr schema row e /= VNull)
evalExpr schema row (ExprIn e es) =
  let v = evalExpr schema row e
      vs = map (evalExpr schema row) es
  in VBool (any (valEq v) vs)
evalExpr schema row (ExprNot e) =
  case evalExpr schema row e of
    VBool b -> VBool (not b)
    _       -> VNull
evalExpr schema row (ExprBinOp op left right) =
  let lv = evalExpr schema row left
      rv = evalExpr schema row right
  in evalBinOp op lv rv

-- | Convert a literal to a runtime Value. Integer literals are always VInt64;
-- cross-type comparisons (e.g. VInt32 vs VInt64) are handled by valCmp.
litToValue :: Literal -> Value
litToValue (LitInt n)    = VInt64 (fromIntegral n)
litToValue (LitFloat d)  = VFloat64 d
litToValue (LitText t)   = VText t
litToValue (LitBool b)   = VBool b
litToValue LitNull       = VNull

evalBinOp :: BinOp -> Value -> Value -> Value
evalBinOp OpAnd (VBool a) (VBool b) = VBool (a && b)
evalBinOp OpOr  (VBool a) (VBool b) = VBool (a || b)
evalBinOp OpEq  a b = VBool (valEq a b)
evalBinOp OpNeq a b = VBool (not (valEq a b))
evalBinOp OpLt  a b = VBool (valCmp a b == Just LT)
evalBinOp OpGt  a b = VBool (valCmp a b == Just GT)
evalBinOp OpLte a b = VBool (valCmp a b `elem` [Just LT, Just EQ])
evalBinOp OpGte a b = VBool (valCmp a b `elem` [Just GT, Just EQ])
evalBinOp OpLike  (VText t) (VText pat) = VBool (sqlLike True t pat)
evalBinOp OpILike (VText t) (VText pat) = VBool (sqlLike False t pat)
evalBinOp _ _ _     = VNull

-- | SQL LIKE/ILIKE pattern matching. caseSensitive=True for LIKE, False for ILIKE.
-- '%' matches any sequence of characters, '_' matches any single character.
sqlLike :: Bool -> Text -> Text -> Bool
sqlLike cs = go
  where
    norm c = if cs then c else toLower c
    go t p = case T.uncons p of
      Nothing -> T.null t
      Just ('%', pRest) -> matchPercent t pRest
      Just ('_', pRest) -> case T.uncons t of
        Nothing     -> False
        Just (_, tRest) -> go tRest pRest
      Just (pc, pRest) -> case T.uncons t of
        Nothing         -> False
        Just (tc, tRest) -> norm tc == norm pc && go tRest pRest
    matchPercent t p = case T.uncons p of
      Nothing -> True  -- trailing % matches everything
      Just ('%', pRest) -> matchPercent t pRest  -- collapse consecutive %
      _ -> go t p || case T.uncons t of
        Nothing     -> False
        Just (_, tRest) -> matchPercent tRest p

valEq :: Value -> Value -> Bool
valEq VNull _ = False
valEq _ VNull = False
valEq a b     = valCmp a b == Just EQ

valCmp :: Value -> Value -> Maybe Ordering
valCmp (VInt32 a)   (VInt32 b)   = Just (compare a b)
valCmp (VInt64 a)   (VInt64 b)   = Just (compare a b)
valCmp (VFloat64 a) (VFloat64 b) = Just (compare a b)
valCmp (VText a)    (VText b)    = Just (compare a b)
valCmp (VBool a)    (VBool b)    = Just (compare a b)
-- Cross-type numeric comparisons
valCmp (VInt32 a)   (VInt64 b)   = Just (compare (fromIntegral a) b)
valCmp (VInt64 a)   (VInt32 b)   = Just (compare a (fromIntegral b))
valCmp (VInt32 a)   (VFloat64 b) = Just (compare (fromIntegral a) b)
valCmp (VFloat64 a) (VInt32 b)   = Just (compare a (fromIntegral b))
valCmp (VInt64 a)   (VFloat64 b) = Just (compare (fromIntegral a) b)
valCmp (VFloat64 a) (VInt64 b)   = Just (compare a (fromIntegral b))
valCmp _ _                       = Nothing

-- ORDER BY helpers

resolveOrderByColumns :: Schema -> [OrderByClause] -> Either Text [(Int, SortOrder)]
resolveOrderByColumns schema clauses = mapM resolve clauses
  where
    indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]
    resolve (OrderByClause col order) =
      case Map.lookup col indexed of
        Nothing -> Left ("ORDER BY column '" <> col <> "' does not exist in table")
        Just i  -> Right (i, order)

sortRows :: [(Int, SortOrder)] -> [(RowId, Row)] -> [(RowId, Row)]
sortRows orderIdxs rows = sortBy comparator rows
  where
    comparator (_, rowA) (_, rowB) = mconcat
      [applyOrder order (valCmpNull (rowA V.! idx) (rowB V.! idx))
      | (idx, order) <- orderIdxs]

    applyOrder Asc  o = o
    applyOrder Desc o = flipOrd o

    flipOrd LT = GT
    flipOrd GT = LT
    flipOrd EQ = EQ

-- | NULL-aware comparison for sorting.
-- NULLs are treated as larger than all non-NULL values, matching PostgreSQL:
-- ASC  -> NULLs last;  DESC -> NULLs first.
valCmpNull :: Value -> Value -> Ordering
valCmpNull VNull VNull = EQ
valCmpNull VNull _     = GT
valCmpNull _     VNull = LT
valCmpNull a     b     = fromMaybe EQ (valCmp a b)

applyAssignments :: Schema -> [(Text, Expr)] -> Row -> Either Text (Vector Value)
applyAssignments schema assigns row = do
  let indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]
  updates <- mapM (\(col, expr) ->
    case Map.lookup col indexed of
      Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      Just i  -> do
        let val = evalExpr schema row expr
            colType = columnType (schema V.! i)
        coerced <- coerceValue colType val
        return (i, coerced)
    ) assigns
  return (V.accum (\_ v -> v) row updates)

-- ALTER TABLE ADD COLUMN

execAlterAddColumn :: Database -> Maybe TxState -> Text -> ColumnDef -> ExceptT Text IO QueryResult
execAlterAddColumn db Nothing name colDef = do
  let col = columnDefToColumn colDef
  if not (columnNullable col)
    then throwE "ALTER TABLE ADD COLUMN requires the new column to be nullable"
    else do
      callback <- atomicallyDbError $ alterAddColumnSTM db name col
      lift $ atomically $ takeTMVar callback
      return (CommandComplete "ALTER TABLE")
execAlterAddColumn db (Just tx) name colDef = do
  let col = columnDefToColumn colDef
  if not (columnNullable col)
    then throwE "ALTER TABLE ADD COLUMN requires the new column to be nullable"
    else do
      -- Verify table exists
      schema <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
      -- Check for duplicate column name
      let colNames = [columnName (schema V.! i) | i <- [0..V.length schema - 1]]
      if columnName col `elem` colNames
        then throwE ("Column '" <> columnName col <> "' already exists in table")
        else do
          lift $ addPendingOp tx (PendingAlterAddColumn name col)
          return (CommandComplete "ALTER TABLE")

-- EXPLAIN

execExplain :: Statement -> ExceptT Text IO QueryResult
execExplain stmt = do
  let desc = describeStatement stmt
      colInfo = [ColumnInfo "QUERY PLAN" TText]
      rows = [[Just line] | line <- desc]
  return (RowResult colInfo rows)

describeStatement :: Statement -> [Text]
describeStatement (Select dist targets name wh orderBy mLimit mOffset) =
  let scan = "Seq Scan on " <> name
      distLine = if dist then ["DISTINCT"] else []
      targetLine = case targets of
        [Star] -> []
        _      -> ["Output: " <> T.intercalate ", " (map descTarget targets)]
      filterLine = case wh of
        Nothing -> []
        Just expr -> ["Filter: " <> descExpr expr]
      orderLine = case orderBy of
        [] -> []
        _  -> ["Sort: " <> T.intercalate ", " (map descOrder orderBy)]
      limitLine = case mLimit of
        Nothing -> []
        Just n  -> ["Limit: " <> T.pack (show n)]
      offsetLine = case mOffset of
        Nothing -> []
        Just n  -> ["Offset: " <> T.pack (show n)]
  in [scan] <> distLine <> targetLine <> filterLine <> orderLine <> limitLine <> offsetLine
describeStatement (Insert name _ _) = ["Insert on " <> name]
describeStatement (Update name _ _) = ["Update on " <> name]
describeStatement (Delete name _)   = ["Delete on " <> name]
describeStatement (CreateTable name _) = ["Create Table " <> name]
describeStatement (DropTable name) = ["Drop Table " <> name]
describeStatement (AlterTableAddColumn name cd) = ["Alter Table " <> name <> " Add Column " <> cdName cd]
describeStatement (Explain inner) = "EXPLAIN:" : describeStatement inner
describeStatement Begin = ["BEGIN"]
describeStatement Commit = ["COMMIT"]
describeStatement Rollback = ["ROLLBACK"]

descTarget :: SelectTarget -> Text
descTarget Star = "*"
descTarget (SQL.Column c) = c
descTarget (ColumnAs c a) = c <> " AS " <> a
descTarget (Agg f _) = descAggFunc f

descAggFunc :: AggFunc -> Text
descAggFunc AggCount = "COUNT(*)"
descAggFunc (AggCountCol c) = "COUNT(" <> c <> ")"
descAggFunc (AggSum c) = "SUM(" <> c <> ")"
descAggFunc (AggAvg c) = "AVG(" <> c <> ")"
descAggFunc (AggMin c) = "MIN(" <> c <> ")"
descAggFunc (AggMax c) = "MAX(" <> c <> ")"

descExpr :: Expr -> Text
descExpr (ExprLit (LitInt n)) = T.pack (show n)
descExpr (ExprLit (LitFloat d)) = T.pack (show d)
descExpr (ExprLit (LitText t)) = "'" <> t <> "'"
descExpr (ExprLit (LitBool b)) = if b then "TRUE" else "FALSE"
descExpr (ExprLit LitNull) = "NULL"
descExpr (ExprColumn c) = c
descExpr (ExprBinOp op l r) = descExpr l <> " " <> descOp op <> " " <> descExpr r
descExpr (ExprIsNull e) = descExpr e <> " IS NULL"
descExpr (ExprIsNotNull e) = descExpr e <> " IS NOT NULL"
descExpr (ExprIn e es) = descExpr e <> " IN (" <> T.intercalate ", " (map descExpr es) <> ")"
descExpr (ExprNot e) = "NOT " <> descExpr e

descOp :: BinOp -> Text
descOp OpEq = "="
descOp OpNeq = "<>"
descOp OpLt = "<"
descOp OpGt = ">"
descOp OpLte = "<="
descOp OpGte = ">="
descOp OpAnd = "AND"
descOp OpOr = "OR"
descOp OpLike = "LIKE"
descOp OpILike = "ILIKE"

descOrder :: OrderByClause -> Text
descOrder (OrderByClause c Asc) = c <> " ASC"
descOrder (OrderByClause c Desc) = c <> " DESC"

-- Aggregate helpers

isAggTarget :: SelectTarget -> Bool
isAggTarget (Agg _ _) = True
isAggTarget _         = False

computeAggregates :: Schema -> [SelectTarget] -> [(RowId, Row)]
                  -> Either Text ([ColumnInfo], [Maybe Text])
computeAggregates schema targets rows = do
  let hasNonAgg = any (not . isAggTarget) targets
  if hasNonAgg
    then Left "Non-aggregate columns in aggregate query require GROUP BY"
    else do
      results <- mapM (computeOneAgg schema rows) targets
      return (map fst results, map snd results)

computeOneAgg :: Schema -> [(RowId, Row)] -> SelectTarget
              -> Either Text (ColumnInfo, Maybe Text)
computeOneAgg _ rows (Agg AggCount mAlias) =
  let name = fromMaybe "count" mAlias
  in Right (ColumnInfo name TInt64, Just (T.pack (show (length rows))))
computeOneAgg schema rows (Agg (AggCountCol col) mAlias) = do
  idx <- findColIdx schema col
  let count = length [() | (_, row) <- rows, row V.! idx /= VNull]
      name = fromMaybe "count" mAlias
  Right (ColumnInfo name TInt64, Just (T.pack (show count)))
computeOneAgg schema rows (Agg (AggSum col) mAlias) = do
  idx <- findColIdx schema col
  let colType = columnType (schema V.! idx)
      vals = [row V.! idx | (_, row) <- rows, row V.! idx /= VNull]
      total = sumValues vals
      name = fromMaybe "sum" mAlias
      resultType = case colType of
        TInt32  -> TInt64
        TInt64  -> TInt64
        _       -> TFloat64
  Right (ColumnInfo name resultType, total >>= formatValue)
computeOneAgg schema rows (Agg (AggAvg col) mAlias) = do
  idx <- findColIdx schema col
  let vals = [row V.! idx | (_, row) <- rows, row V.! idx /= VNull]
      name = fromMaybe "avg" mAlias
  case (sumValues vals, length vals) of
    (Just sv, n) | n > 0 ->
      let s = valueToDouble sv
      in Right (ColumnInfo name TFloat64, Just (T.pack (show (s / fromIntegral n :: Double))))
    _ -> Right (ColumnInfo name TFloat64, Nothing)
computeOneAgg schema rows (Agg (AggMin col) mAlias) = do
  idx <- findColIdx schema col
  let colType = columnType (schema V.! idx)
      vals = [row V.! idx | (_, row) <- rows, row V.! idx /= VNull]
      name = fromMaybe "min" mAlias
  case vals of
    [] -> Right (ColumnInfo name colType, Nothing)
    _  -> let minVal = foldl1 (\a b -> if valCmp a b == Just LT then a else b) vals
          in Right (ColumnInfo name colType, formatValue minVal)
computeOneAgg schema rows (Agg (AggMax col) mAlias) = do
  idx <- findColIdx schema col
  let colType = columnType (schema V.! idx)
      vals = [row V.! idx | (_, row) <- rows, row V.! idx /= VNull]
      name = fromMaybe "max" mAlias
  case vals of
    [] -> Right (ColumnInfo name colType, Nothing)
    _  -> let maxVal = foldl1 (\a b -> if valCmp a b == Just GT then a else b) vals
          in Right (ColumnInfo name colType, formatValue maxVal)
computeOneAgg _ _ _ = Left "Non-aggregate target in aggregate query"

findColIdx :: Schema -> Text -> Either Text Int
findColIdx schema col =
  let indexed = Map.fromList [(columnName (schema V.! i), i) | i <- [0..V.length schema - 1]]
  in case Map.lookup col indexed of
       Nothing -> Left ("Column '" <> col <> "' does not exist in table")
       Just i  -> Right i

-- | Sum values, returning VInt64 when all inputs are integral, VFloat64 otherwise.
sumValues :: [Value] -> Maybe Value
sumValues [] = Nothing
sumValues vs
  | all isIntVal vs = Just (VInt64 (foldl addInt 0 vs))
  | otherwise       = Just (VFloat64 (foldl addFloat 0 vs))
  where
    isIntVal (VInt32 _) = True
    isIntVal (VInt64 _) = True
    isIntVal _          = False
    addInt acc (VInt32 n)  = acc + fromIntegral n
    addInt acc (VInt64 n)  = acc + n
    addInt acc _           = acc
    addFloat acc (VInt32 n)   = acc + fromIntegral n
    addFloat acc (VInt64 n)   = acc + fromIntegral n
    addFloat acc (VFloat64 d) = acc + d
    addFloat acc _            = acc

valueToDouble :: Value -> Double
valueToDouble (VInt64 n)   = fromIntegral n
valueToDouble (VFloat64 d) = d
valueToDouble _            = 0

-- | Coerce a Value to match the expected column type when possible.
-- Returns Left on lossy conversions (e.g. Int64 out of Int32 range).
coerceValue :: ColumnType -> Value -> Either Text Value
coerceValue _ VNull = Right VNull
coerceValue TInt32 (VInt64 n)
  | n < fromIntegral (minBound :: Int32) || n > fromIntegral (maxBound :: Int32) =
      Left ("Value " <> T.pack (show n) <> " out of Int32 range")
  | otherwise = Right (VInt32 (fromIntegral n))
coerceValue TInt64 (VInt32 n)   = Right (VInt64 (fromIntegral n))
coerceValue TFloat64 (VInt32 n) = Right (VFloat64 (fromIntegral n))
coerceValue TFloat64 (VInt64 n) = Right (VFloat64 (fromIntegral n))
coerceValue _ v = Right v
