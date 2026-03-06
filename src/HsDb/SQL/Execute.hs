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
import Data.Int (Int32, Int64)
import Data.List (sortBy)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V

import HsDb.Types
import HsDb.Integration (Database(..), selectAllSTM, createTableSTM,
                          insertRowSTM, updateRowSTM, deleteRowSTM, dropTableSTM,
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
  Select targets name wh orderBy limit offset
                             -> execSelect db mtx name targets wh orderBy limit offset
  Update name assigns wh   -> execUpdate db mtx name assigns wh
  Delete name wh           -> execDelete db mtx name wh
  Begin    -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"
  Commit   -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"
  Rollback -> throwE "BEGIN/COMMIT/ROLLBACK handled by server"

-- CREATE TABLE

execCreateTable :: Database -> Maybe TxState -> Text -> [ColumnDef] -> ExceptT Text IO QueryResult
execCreateTable db Nothing name colDefs = do
  let schema = map columnDefToColumn colDefs
  (_, callback) <- atomicallyDbError $ createTableSTM db name schema
  lift $ atomically $ takeTMVar callback
  return (CommandComplete "CREATE TABLE")
execCreateTable _db (Just tx) name colDefs = do
  let schema = map columnDefToColumn colDefs
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
insertRowsDirect db name schema colOrder (lits:rest) n = do
  row <- except $ buildRow schema colOrder lits
  (_, callback) <- atomicallyDbError $ insertRowSTM db name row
  lift $ atomically $ takeTMVar callback
  insertRowsDirect db name schema colOrder rest (n + 1)

insertRowsTx :: TxState -> Text -> Schema -> [Int] -> [[Literal]] -> Int
             -> ExceptT Text IO Int
insertRowsTx _ _ _ _ [] n = return n
insertRowsTx tx name schema colOrder (lits:rest) n = do
  row <- except $ buildRow schema colOrder lits
  except $ validateRowE schema row
  lift $ addPendingOp tx (PendingInsert name row)
  insertRowsTx tx name schema colOrder rest (n + 1)

-- SELECT

execSelect :: Database -> Maybe TxState -> Text -> [SelectTarget] -> Maybe Expr
           -> [OrderByClause] -> Maybe Int -> Maybe Int
           -> ExceptT Text IO QueryResult
execSelect db Nothing name targets wh orderBy mLimit mOffset = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  (colInfos, colIdxs) <- except $ resolveSelectTargets schema targets
  let filtered = filterRows schema wh rows
  sorted <- case orderBy of
    [] -> return filtered
    _  -> do
      orderIdxs <- except $ resolveOrderByColumns schema orderBy
      return (sortRows orderIdxs filtered)
  let afterOffset = maybe id drop mOffset sorted
  let afterLimit  = maybe id take mLimit afterOffset
  let projected = map (\(_, row) -> projectRow colIdxs row) afterLimit
  let textRows = map (map formatValue) projected
  return (RowResult colInfos textRows)
execSelect db (Just tx) name targets wh orderBy mLimit mOffset = do
  schema <- lift (effectiveSchema (dbCatalog db) tx name) >>= liftEither
  rowsResult <- lift $ effectiveRows (dbCatalog db) tx name
  rows <- liftEither rowsResult
  (colInfos, colIdxs) <- except $ resolveSelectTargets schema targets
  let filtered = filterRows schema wh rows
  sorted <- case orderBy of
    [] -> return filtered
    _  -> do
      orderIdxs <- except $ resolveOrderByColumns schema orderBy
      return (sortRows orderIdxs filtered)
  let afterOffset = maybe id drop mOffset sorted
  let afterLimit  = maybe id take mLimit afterOffset
  let projected = map (\(_, row) -> projectRow colIdxs row) afterLimit
  let textRows = map (map formatValue) projected
  return (RowResult colInfos textRows)

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
updateRowsDirect db name schema assigns ((rowId, row):rest) n = do
  newRow <- except $ applyAssignments schema assigns row
  callback <- atomicallyDbError $ updateRowSTM db name rowId newRow
  lift $ atomically $ takeTMVar callback
  updateRowsDirect db name schema assigns rest (n + 1)

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
deleteRowsDirect db name ((rowId, _):rest) n = do
  callback <- atomicallyDbError $ deleteRowSTM db name rowId
  lift $ atomically $ takeTMVar callback
  deleteRowsDirect db name rest (n + 1)

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
    findCol col = case lookup col indexed of
      Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      Just i  -> Right i
    indexed = zip (map columnName schema) [0..]

-- Build a row vector from literals, mapped to schema column positions.
buildRow :: Schema -> [Int] -> [Literal] -> Either Text (Vector Value)
buildRow schema colOrder lits
  | length lits /= length colOrder =
      Left ("Expected " <> T.pack (show (length colOrder))
            <> " values, got " <> T.pack (show (length lits)))
  | otherwise = do
      let schemaLen = length schema
          base = V.replicate schemaLen VNull
          pairs = zip colOrder lits
      vals <- mapM (\(i, lit) -> do
        let col = schema !! i
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
  let infos = map (\c -> ColumnInfo (columnName c) (columnType c)) schema
      idxs  = [0 .. length schema - 1]
  in Right (infos, idxs)
resolveSelectTargets schema targets =
  let indexed = zip (map columnName schema) [0..]
      resolve (SQL.Column col) =
        case lookup col indexed of
          Just i  -> let c = schema !! i
                     in Right (ColumnInfo (columnName c) (columnType c), i)
          Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      resolve Star = Left "* cannot be mixed with column names"
  in do
      pairs <- mapM resolve targets
      return (map fst pairs, map snd pairs)

projectRow :: [Int] -> Row -> [Value]
projectRow idxs row = map (\i -> row V.! i) idxs

formatValue :: Value -> Maybe Text
formatValue (VInt32 n)   = Just (T.pack (show n))
formatValue (VInt64 n)   = Just (T.pack (show n))
formatValue (VFloat64 d) = Just (T.pack (show d))
formatValue (VText t)    = Just t
formatValue (VBool True) = Just "t"
formatValue (VBool False)= Just "f"
formatValue (VBytea _)   = Just "\\x..."
formatValue VNull        = Nothing

-- WHERE clause evaluation

evalWhere :: Schema -> Expr -> Row -> Bool
evalWhere schema expr row = case evalExpr schema row expr of
  VBool b -> b
  _       -> False

evalExpr :: Schema -> Row -> Expr -> Value
evalExpr _ _ (ExprLit lit) = litToValue lit
evalExpr schema row (ExprColumn col) =
  case lookup col indexed of
    Just i  -> row V.! i
    Nothing -> VNull
  where indexed = zip (map columnName schema) [0..]
evalExpr schema row (ExprIsNull e) =
  VBool (evalExpr schema row e == VNull)
evalExpr schema row (ExprIsNotNull e) =
  VBool (evalExpr schema row e /= VNull)
evalExpr schema row (ExprBinOp op left right) =
  let lv = evalExpr schema row left
      rv = evalExpr schema row right
  in evalBinOp op lv rv

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
evalBinOp _ _ _     = VNull

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
    indexed = zip (map columnName schema) [0..]
    resolve (OrderByClause col order) =
      case lookup col indexed of
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
  let indexed = zip (map columnName schema) [0..]
  updates <- mapM (\(col, expr) ->
    case lookup col indexed of
      Nothing -> Left ("Column '" <> col <> "' does not exist in table")
      Just i  -> do
        let val = evalExpr schema row expr
            colType = columnType (schema !! i)
        return (i, coerceValue colType val)
    ) assigns
  return (V.accum (\_ v -> v) row updates)

-- | Coerce a Value to match the expected column type when possible.
coerceValue :: ColumnType -> Value -> Value
coerceValue _ VNull = VNull
coerceValue TInt32 (VInt64 n)   = VInt32 (fromIntegral n)
coerceValue TInt64 (VInt32 n)   = VInt64 (fromIntegral n)
coerceValue TFloat64 (VInt32 n) = VFloat64 (fromIntegral n)
coerceValue TFloat64 (VInt64 n) = VFloat64 (fromIntegral n)
coerceValue _ v = v
