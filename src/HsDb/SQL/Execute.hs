{-# LANGUAGE OverloadedStrings #-}

module HsDb.SQL.Execute
  ( executeSQL
  , QueryResult(..)
  , ColumnInfo(..)
  ) where

import Control.Concurrent.STM (atomically, readTVar, takeTMVar)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V

import HsDb.Types
import HsDb.Integration (Database(..), selectAllSTM, createTableSTM,
                          insertRowSTM, updateRowSTM, deleteRowSTM, dropTableSTM)
import HsDb.Table (Table(..), TableCatalog)
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

-- | Execute a parsed SQL statement against a Database.
executeSQL :: Database -> Statement -> IO (Either Text QueryResult)
executeSQL db stmt = case stmt of
  CreateTable name colDefs -> execCreateTable db name colDefs
  DropTable name           -> execDropTable db name
  Insert name cols valRows -> execInsert db name cols valRows
  Select targets name wh   -> execSelect db name targets wh
  Update name assigns wh   -> execUpdate db name assigns wh
  Delete name wh           -> execDelete db name wh

-- CREATE TABLE

execCreateTable :: Database -> Text -> [ColumnDef] -> IO (Either Text QueryResult)
execCreateTable db name colDefs = do
  let schema = map columnDefToColumn colDefs
  result <- atomically $ createTableSTM db name schema
  case result of
    Left err -> return (Left (T.pack (show err)))
    Right (_, callback) -> do
      atomically $ takeTMVar callback
      return (Right (CommandComplete "CREATE TABLE"))

-- DROP TABLE

execDropTable :: Database -> Text -> IO (Either Text QueryResult)
execDropTable db name = do
  result <- atomically $ dropTableSTM db name
  case result of
    Left err -> return (Left (T.pack (show err)))
    Right callback -> do
      atomically $ takeTMVar callback
      return (Right (CommandComplete "DROP TABLE"))

-- INSERT

execInsert :: Database -> Text -> [Text] -> [[Literal]] -> IO (Either Text QueryResult)
execInsert db name cols valRows = do
  -- Look up table schema to map column names to positions
  schemaResult <- getTableSchema (dbCatalog db) name
  case schemaResult of
    Left err -> return (Left err)
    Right schema -> do
      -- Resolve column ordering
      case resolveColumns schema cols of
        Left err -> return (Left err)
        Right colOrder -> do
          -- Insert each row
          count <- insertRows db name schema colOrder valRows 0
          case count of
            Left err -> return (Left err)
            Right n  -> return (Right (CommandComplete ("INSERT 0 " <> T.pack (show n))))

insertRows :: Database -> Text -> Schema -> [Int] -> [[Literal]] -> Int
           -> IO (Either Text Int)
insertRows _ _ _ _ [] n = return (Right n)
insertRows db name schema colOrder (lits:rest) n = do
  case buildRow schema colOrder lits of
    Left err -> return (Left err)
    Right row -> do
      result <- atomically $ insertRowSTM db name row
      case result of
        Left err -> return (Left (T.pack (show err)))
        Right (_, callback) -> do
          atomically $ takeTMVar callback
          insertRows db name schema colOrder rest (n + 1)

-- SELECT

execSelect :: Database -> Text -> [SelectTarget] -> Maybe Expr
           -> IO (Either Text QueryResult)
execSelect db name targets wh = do
  schemaResult <- getTableSchema (dbCatalog db) name
  case schemaResult of
    Left err -> return (Left err)
    Right schema -> do
      result <- atomically $ selectAllSTM db name
      case result of
        Left err -> return (Left (T.pack (show err)))
        Right rows -> do
          -- Resolve target columns
          let (colInfos, colIdxs) = resolveSelectTargets schema targets
          -- Filter by WHERE
          let filtered = case wh of
                Nothing -> rows
                Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
          -- Project columns
          let projected = map (\(_, row) -> projectRow colIdxs row) filtered
          -- Format as text
          let textRows = map (map formatValue) projected
          return (Right (RowResult colInfos textRows))

-- UPDATE

execUpdate :: Database -> Text -> [(Text, Expr)] -> Maybe Expr
           -> IO (Either Text QueryResult)
execUpdate db name assigns wh = do
  schemaResult <- getTableSchema (dbCatalog db) name
  case schemaResult of
    Left err -> return (Left err)
    Right schema -> do
      result <- atomically $ selectAllSTM db name
      case result of
        Left err -> return (Left (T.pack (show err)))
        Right rows -> do
          let matching = case wh of
                Nothing -> rows
                Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
          -- Apply updates
          count <- updateRows db name schema assigns matching 0
          case count of
            Left err -> return (Left err)
            Right n  -> return (Right (CommandComplete ("UPDATE " <> T.pack (show n))))

updateRows :: Database -> Text -> Schema -> [(Text, Expr)] -> [(RowId, Row)] -> Int
           -> IO (Either Text Int)
updateRows _ _ _ _ [] n = return (Right n)
updateRows db name schema assigns ((rowId, row):rest) n = do
  case applyAssignments schema assigns row of
    Left err -> return (Left err)
    Right newRow -> do
      result <- atomically $ updateRowSTM db name rowId newRow
      case result of
        Left err -> return (Left (T.pack (show err)))
        Right callback -> do
          atomically $ takeTMVar callback
          updateRows db name schema assigns rest (n + 1)

-- DELETE

execDelete :: Database -> Text -> Maybe Expr -> IO (Either Text QueryResult)
execDelete db name wh = do
  schemaResult <- getTableSchema (dbCatalog db) name
  case schemaResult of
    Left err -> return (Left err)
    Right schema -> do
      result <- atomically $ selectAllSTM db name
      case result of
        Left err -> return (Left (T.pack (show err)))
        Right rows -> do
          let matching = case wh of
                Nothing -> rows
                Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
          count <- deleteRows db name matching 0
          case count of
            Left err -> return (Left err)
            Right n  -> return (Right (CommandComplete ("DELETE " <> T.pack (show n))))

deleteRows :: Database -> Text -> [(RowId, Row)] -> Int -> IO (Either Text Int)
deleteRows _ _ [] n = return (Right n)
deleteRows db name ((rowId, _):rest) n = do
  result <- atomically $ deleteRowSTM db name rowId
  case result of
    Left err -> return (Left (T.pack (show err)))
    Right callback -> do
      atomically $ takeTMVar callback
      deleteRows db name rest (n + 1)

-- Helpers

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

getTableSchema :: TableCatalog -> Text -> IO (Either Text Schema)
getTableSchema catalog name = do
  tables <- atomically $ readTVar catalog
  case Map.lookup name tables of
    Nothing    -> return (Left ("Table '" <> name <> "' does not exist"))
    Just table -> return (Right (tableSchema table))

-- Map column names from INSERT to schema positions.
-- Returns a list of schema indices in the order given by the INSERT column list.
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

resolveSelectTargets :: Schema -> [SelectTarget] -> ([ColumnInfo], [Int])
resolveSelectTargets schema [Star] =
  let infos = map (\c -> ColumnInfo (columnName c) (columnType c)) schema
      idxs  = [0 .. length schema - 1]
  in (infos, idxs)
resolveSelectTargets schema targets =
  let resolve (SQL.Column col) =
        case lookup col indexed of
          Just i  -> let c = schema !! i
                     in (ColumnInfo (columnName c) (columnType c), i)
          Nothing -> (ColumnInfo col TText, -1) -- will produce empty
        where indexed = zip (map columnName schema) [0..]
      resolve Star = error "unexpected Star in non-star target list"
      pairs = map resolve targets
  in (map fst pairs, map snd pairs)

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
valEq a b     = a == b

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
