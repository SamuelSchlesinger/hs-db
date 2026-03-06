{-# LANGUAGE OverloadedStrings #-}

-- | In-memory table storage engine. Tables are STM-backed maps from 'RowId'
-- to 'Row', with schema validation on every mutation.
module HsDb.Table
  ( Table(..)
  , TableCatalog
  , newTableCatalog
  , createTable
  , dropTable
  , insertRow
  , insertRowWithId
  , selectAll
  , updateRow
  , deleteRow
  , validateRow
  , lookupTable
  ) where

import Control.Concurrent.STM
import Control.Monad (when)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, throwE, except)
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as V

import HsDb.Types

-- | An in-memory table: schema, row storage, and monotonic row ID counter.
data Table = Table
  { tableSchema  :: !Schema
  , tableRows    :: !(TVar (IntMap Row))
  , tableCounter :: !(TVar Int)
  }

-- | A catalog of named tables.
type TableCatalog = TVar (Map TableName Table)

-- | Create a new empty table catalog.
newTableCatalog :: STM TableCatalog
newTableCatalog = newTVar Map.empty

-- | Look up a table by name, throwing 'TableNotFound' if absent.
lookupTable :: TableCatalog -> TableName -> ExceptT DbError STM Table
lookupTable catalog name = do
  tables <- lift $ readTVar catalog
  case Map.lookup name tables of
    Nothing    -> throwE (TableNotFound name)
    Just table -> return table

-- | Create a new table in the catalog.
createTable :: TableCatalog -> TableName -> Schema -> ExceptT DbError STM Table
createTable catalog name schema = do
  tables <- lift $ readTVar catalog
  case Map.lookup name tables of
    Just _  -> throwE (TableAlreadyExists name)
    Nothing -> do
      rows    <- lift $ newTVar IntMap.empty
      counter <- lift $ newTVar 0
      let table = Table schema rows counter
      lift $ writeTVar catalog (Map.insert name table tables)
      return table

-- | Drop a table from the catalog.
dropTable :: TableCatalog -> TableName -> ExceptT DbError STM ()
dropTable catalog name = do
  tables <- lift $ readTVar catalog
  case Map.lookup name tables of
    Nothing -> throwE (TableNotFound name)
    Just _  -> lift $ writeTVar catalog (Map.delete name tables)

-- | Validate a row against a schema. Returns Nothing on success, or a
-- description of the schema violation.
validateRow :: Schema -> Vector Value -> Either DbError ()
validateRow schema row
  | V.length row /= length schema =
      Left (SchemaViolation (T.pack ("Expected " ++ show (length schema)
                                     ++ " columns, got " ++ show (V.length row))))
  | otherwise = checkColumns 0 schema (V.toList row)
  where
    checkColumns :: Int -> [Column] -> [Value] -> Either DbError ()
    checkColumns _ [] [] = Right ()
    checkColumns i (col:cols) (val:vals) =
      case checkValue col val of
        Nothing  -> checkColumns (i + 1) cols vals
        Just err -> Left (SchemaViolation (T.pack ("Column " ++ show i
                                                    ++ " (" ++ T.unpack (columnName col) ++ "): "
                                                    ++ T.unpack err)))
    checkColumns _ _ _ = Right () -- impossible given length check above

    checkValue :: Column -> Value -> Maybe Text
    checkValue col VNull
      | columnNullable col = Nothing
      | otherwise = Just "NULL not allowed"
    checkValue col val =
      if valueMatchesType (columnType col) val
        then Nothing
        else Just (T.pack ("expected " ++ show (columnType col)
                           ++ ", got " ++ valueTypeName val))

    valueMatchesType :: ColumnType -> Value -> Bool
    valueMatchesType TInt32   (VInt32 _)   = True
    valueMatchesType TInt64   (VInt64 _)   = True
    valueMatchesType TFloat64 (VFloat64 _) = True
    valueMatchesType TText    (VText _)    = True
    valueMatchesType TBool    (VBool _)    = True
    valueMatchesType TBytea   (VBytea _)   = True
    valueMatchesType _        _            = False

    valueTypeName :: Value -> String
    valueTypeName (VInt32 _)   = "Int32"
    valueTypeName (VInt64 _)   = "Int64"
    valueTypeName (VFloat64 _) = "Float64"
    valueTypeName (VText _)    = "Text"
    valueTypeName (VBool _)    = "Bool"
    valueTypeName (VBytea _)   = "Bytea"
    valueTypeName VNull        = "Null"

-- | Insert a row, auto-assigning a new row ID. Returns the assigned RowId.
insertRow :: Table -> Vector Value -> ExceptT DbError STM RowId
insertRow table row = do
  except $ validateRow (tableSchema table) row
  rid <- lift $ readTVar (tableCounter table)
  lift $ modifyTVar' (tableCounter table) (+ 1)
  lift $ modifyTVar' (tableRows table) (IntMap.insert rid row)
  return rid

-- | Insert a row with a specific row ID (used during WAL replay).
-- Rejects duplicate row IDs. Advances the counter to max(current, rowId + 1)
-- to avoid ID reuse.
insertRowWithId :: Table -> TableName -> RowId -> Vector Value -> ExceptT DbError STM ()
insertRowWithId table tableName rowId row = do
  except $ validateRow (tableSchema table) row
  rows <- lift $ readTVar (tableRows table)
  if IntMap.member rowId rows
    then throwE (DuplicateRowId tableName rowId)
    else do
      lift $ writeTVar (tableRows table) (IntMap.insert rowId row rows)
      counter <- lift $ readTVar (tableCounter table)
      when (rowId >= counter) $
        lift $ writeTVar (tableCounter table) (rowId + 1)

-- | Select all rows from a table.
selectAll :: Table -> STM [(RowId, Row)]
selectAll table = do
  rows <- readTVar (tableRows table)
  return (IntMap.toAscList rows)

-- | Update an existing row.
updateRow :: Table -> TableName -> RowId -> Vector Value -> ExceptT DbError STM ()
updateRow table tableName rowId row = do
  except $ validateRow (tableSchema table) row
  rows <- lift $ readTVar (tableRows table)
  case IntMap.lookup rowId rows of
    Nothing -> throwE (RowNotFound tableName rowId)
    Just _  -> lift $ writeTVar (tableRows table) (IntMap.insert rowId row rows)

-- | Delete a row by ID.
deleteRow :: Table -> TableName -> RowId -> ExceptT DbError STM ()
deleteRow table tableName rowId = do
  rows <- lift $ readTVar (tableRows table)
  case IntMap.lookup rowId rows of
    Nothing -> throwE (RowNotFound tableName rowId)
    Just _  -> lift $ writeTVar (tableRows table) (IntMap.delete rowId rows)
