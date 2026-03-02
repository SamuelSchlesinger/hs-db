{-# LANGUAGE OverloadedStrings #-}

-- | Glue between the in-memory table engine and the WAL. Each STM function
-- atomically applies a mutation to the catalog and enqueues the corresponding
-- WAL command, returning a durability callback.
module HsDb.Integration
  ( Database(..)
  , createTableSTM
  , insertRowSTM
  , updateRowSTM
  , deleteRowSTM
  , dropTableSTM
  , selectAllSTM
  ) where

import Control.Concurrent.MVar (MVar)
import Control.Concurrent.STM
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)

import HsDb.Types
import HsDb.Table
import HsDb.WAL.Types
import HsDb.WAL.Writer (WALHandle)

-- | The database type: catalog, WAL queue, status, and shutdown handles.
data Database = Database
  { dbCatalog      :: !TableCatalog
  , dbQueue        :: !(TBQueue WALQueueItem)
  , dbStatus       :: !(TVar DatabaseStatus)
  , dbWALHandle    :: !(MVar WALHandle)     -- ^ For closing on shutdown
  , dbShutdownOnce :: !(MVar ())            -- ^ Guards against concurrent closeDatabase
  }

-- | Check that the database is writable before performing a mutation.
checkWritable :: Database -> STM (Either DbError ())
checkWritable db = do
  st <- readTVar (dbStatus db)
  case st of
    DbWritable     -> return (Right ())
    DbReadOnly msg -> return (Left (DatabaseNotWritable msg))
    DbShuttingDown -> return (Left (DatabaseNotWritable "Database is shutting down"))

-- | Create a table and enqueue the WAL command in one STM transaction.
createTableSTM :: Database -> TableName -> Schema
               -> STM (Either DbError (Table, TMVar ()))
createTableSTM db name schema = do
  writable <- checkWritable db
  case writable of
    Left err -> return (Left err)
    Right () -> do
      result <- createTable (dbCatalog db) name schema
      case result of
        Left err -> return (Left err)
        Right table -> do
          callback <- newEmptyTMVar
          writeTBQueue (dbQueue db) (CmdCreateTable name schema, callback)
          return (Right (table, callback))

-- | Insert a row and enqueue the WAL command in one STM transaction.
insertRowSTM :: Database -> TableName -> Vector Value
             -> STM (Either DbError (RowId, TMVar ()))
insertRowSTM db name row = do
  writable <- checkWritable db
  case writable of
    Left err -> return (Left err)
    Right () -> do
      tables <- readTVar (dbCatalog db)
      case Map.lookup name tables of
        Nothing -> return (Left (TableNotFound name))
        Just table -> do
          result <- insertRow table row
          case result of
            Left err -> return (Left err)
            Right rowId -> do
              callback <- newEmptyTMVar
              writeTBQueue (dbQueue db) (CmdInsertRow name rowId row, callback)
              return (Right (rowId, callback))

-- | Update a row and enqueue the WAL command in one STM transaction.
updateRowSTM :: Database -> TableName -> RowId -> Vector Value
             -> STM (Either DbError (TMVar ()))
updateRowSTM db name rowId row = do
  writable <- checkWritable db
  case writable of
    Left err -> return (Left err)
    Right () -> do
      tables <- readTVar (dbCatalog db)
      case Map.lookup name tables of
        Nothing -> return (Left (TableNotFound name))
        Just table -> do
          result <- updateRow table name rowId row
          case result of
            Left err -> return (Left err)
            Right () -> do
              callback <- newEmptyTMVar
              writeTBQueue (dbQueue db) (CmdUpdateRow name rowId row, callback)
              return (Right callback)

-- | Delete a row and enqueue the WAL command in one STM transaction.
deleteRowSTM :: Database -> TableName -> RowId
             -> STM (Either DbError (TMVar ()))
deleteRowSTM db name rowId = do
  writable <- checkWritable db
  case writable of
    Left err -> return (Left err)
    Right () -> do
      tables <- readTVar (dbCatalog db)
      case Map.lookup name tables of
        Nothing -> return (Left (TableNotFound name))
        Just table -> do
          result <- deleteRow table name rowId
          case result of
            Left err -> return (Left err)
            Right () -> do
              callback <- newEmptyTMVar
              writeTBQueue (dbQueue db) (CmdDeleteRow name rowId, callback)
              return (Right callback)

-- | Drop a table and enqueue the WAL command in one STM transaction.
dropTableSTM :: Database -> TableName
             -> STM (Either DbError (TMVar ()))
dropTableSTM db name = do
  writable <- checkWritable db
  case writable of
    Left err -> return (Left err)
    Right () -> do
      result <- dropTable (dbCatalog db) name
      case result of
        Left err -> return (Left err)
        Right () -> do
          callback <- newEmptyTMVar
          writeTBQueue (dbQueue db) (CmdDropTable name, callback)
          return (Right callback)

-- | Select all rows from a table (pure STM read, no WAL involvement).
selectAllSTM :: Database -> TableName -> STM (Either DbError [(RowId, Row)])
selectAllSTM db name = do
  tables <- readTVar (dbCatalog db)
  case Map.lookup name tables of
    Nothing    -> return (Left (TableNotFound name))
    Just table -> Right <$> selectAll table
