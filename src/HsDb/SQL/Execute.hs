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
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.Vector as V

import HsDb.Types
import HsDb.Integration (Database(..), selectAllSTM, createTableSTM,
                          insertRowSTM, updateRowSTM, deleteRowSTM, dropTableSTM,
                          atomicallyE)
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

-- | Bridge ExceptT DbError STM to ExceptT Text IO.
atomicallyDbError :: ExceptT DbError STM a -> ExceptT Text IO a
atomicallyDbError = withExceptT (T.pack . show) . atomicallyE

-- | Execute a parsed SQL statement against a Database.
executeSQL :: Database -> Statement -> ExceptT Text IO QueryResult
executeSQL db stmt = case stmt of
  CreateTable name colDefs -> execCreateTable db name colDefs
  DropTable name           -> execDropTable db name
  Insert name cols valRows -> execInsert db name cols valRows
  Select targets name wh   -> execSelect db name targets wh
  Update name assigns wh   -> execUpdate db name assigns wh
  Delete name wh           -> execDelete db name wh

-- CREATE TABLE

execCreateTable :: Database -> Text -> [ColumnDef] -> ExceptT Text IO QueryResult
execCreateTable db name colDefs = do
  let schema = map columnDefToColumn colDefs
  (_, callback) <- atomicallyDbError $ createTableSTM db name schema
  lift $ atomically $ takeTMVar callback
  return (CommandComplete "CREATE TABLE")

-- DROP TABLE

execDropTable :: Database -> Text -> ExceptT Text IO QueryResult
execDropTable db name = do
  callback <- atomicallyDbError $ dropTableSTM db name
  lift $ atomically $ takeTMVar callback
  return (CommandComplete "DROP TABLE")

-- INSERT

execInsert :: Database -> Text -> [Text] -> [[Literal]] -> ExceptT Text IO QueryResult
execInsert db name cols valRows = do
  schema <- getTableSchema (dbCatalog db) name
  colOrder <- except $ resolveColumns schema cols
  n <- insertRows db name schema colOrder valRows 0
  return (CommandComplete ("INSERT 0 " <> T.pack (show n)))

insertRows :: Database -> Text -> Schema -> [Int] -> [[Literal]] -> Int
           -> ExceptT Text IO Int
insertRows _ _ _ _ [] n = return n
insertRows db name schema colOrder (lits:rest) n = do
  row <- except $ buildRow schema colOrder lits
  (_, callback) <- atomicallyDbError $ insertRowSTM db name row
  lift $ atomically $ takeTMVar callback
  insertRows db name schema colOrder rest (n + 1)

-- SELECT

execSelect :: Database -> Text -> [SelectTarget] -> Maybe Expr
           -> ExceptT Text IO QueryResult
execSelect db name targets wh = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  let (colInfos, colIdxs) = resolveSelectTargets schema targets
  let filtered = case wh of
        Nothing -> rows
        Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
  let projected = map (\(_, row) -> projectRow colIdxs row) filtered
  let textRows = map (map formatValue) projected
  return (RowResult colInfos textRows)

-- UPDATE

execUpdate :: Database -> Text -> [(Text, Expr)] -> Maybe Expr
           -> ExceptT Text IO QueryResult
execUpdate db name assigns wh = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  let matching = case wh of
        Nothing -> rows
        Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
  n <- updateRows db name schema assigns matching 0
  return (CommandComplete ("UPDATE " <> T.pack (show n)))

updateRows :: Database -> Text -> Schema -> [(Text, Expr)] -> [(RowId, Row)] -> Int
           -> ExceptT Text IO Int
updateRows _ _ _ _ [] n = return n
updateRows db name schema assigns ((rowId, row):rest) n = do
  newRow <- except $ applyAssignments schema assigns row
  callback <- atomicallyDbError $ updateRowSTM db name rowId newRow
  lift $ atomically $ takeTMVar callback
  updateRows db name schema assigns rest (n + 1)

-- DELETE

execDelete :: Database -> Text -> Maybe Expr -> ExceptT Text IO QueryResult
execDelete db name wh = do
  schema <- getTableSchema (dbCatalog db) name
  rows <- atomicallyDbError $ selectAllSTM db name
  let matching = case wh of
        Nothing -> rows
        Just expr -> filter (\(_, row) -> evalWhere schema expr row) rows
  n <- deleteRows db name matching 0
  return (CommandComplete ("DELETE " <> T.pack (show n)))

deleteRows :: Database -> Text -> [(RowId, Row)] -> Int -> ExceptT Text IO Int
deleteRows _ _ [] n = return n
deleteRows db name ((rowId, _):rest) n = do
  callback <- atomicallyDbError $ deleteRowSTM db name rowId
  lift $ atomically $ takeTMVar callback
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

getTableSchema :: TableCatalog -> Text -> ExceptT Text IO Schema
getTableSchema catalog name = do
  tables <- lift $ atomically $ readTVar catalog
  case Map.lookup name tables of
    Nothing    -> throwE ("Table '" <> name <> "' does not exist")
    Just table -> return (tableSchema table)

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
