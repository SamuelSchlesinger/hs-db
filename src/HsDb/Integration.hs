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
  , atomicallyE
  , commitPendingOps
  ) where

import Control.Concurrent.MVar (MVar)
import Control.Concurrent.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT(..), throwE, runExceptT)
import Data.Vector (Vector)

import HsDb.Types
import HsDb.Table
import HsDb.WAL.Types
import HsDb.WAL.Writer (WALHandle)
import HsDb.Transaction (PendingOp(..))

-- | The database type: catalog, WAL queue, status, and shutdown handles.
data Database = Database
  { dbCatalog      :: !TableCatalog
  , dbQueue        :: !(TBQueue WALQueueItem)
  , dbStatus       :: !(TVar DatabaseStatus)
  , dbWALHandle    :: !(MVar WALHandle)     -- ^ For closing on shutdown
  , dbShutdownOnce :: !(MVar ())            -- ^ Guards against concurrent closeDatabase
  }

-- | Run an 'ExceptT' over STM atomically, bridging to IO.
atomicallyE :: ExceptT e STM a -> ExceptT e IO a
atomicallyE = ExceptT . atomically . runExceptT

-- | Check that the database is writable before performing a mutation.
checkWritable :: Database -> ExceptT DbError STM ()
checkWritable db = do
  st <- lift $ readTVar (dbStatus db)
  case st of
    DbWritable     -> return ()
    DbReadOnly msg -> throwE (DatabaseNotWritable msg)
    DbShuttingDown -> throwE (DatabaseNotWritable "Database is shutting down")

-- | Create a table and enqueue the WAL command in one STM transaction.
createTableSTM :: Database -> TableName -> Schema
               -> ExceptT DbError STM (Table, TMVar ())
createTableSTM db name schema = do
  checkWritable db
  table <- createTable (dbCatalog db) name schema
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdCreateTable name schema, callback)
  return (table, callback)

-- | Insert a row and enqueue the WAL command in one STM transaction.
insertRowSTM :: Database -> TableName -> Vector Value
             -> ExceptT DbError STM (RowId, TMVar ())
insertRowSTM db name row = do
  checkWritable db
  table <- lookupTable (dbCatalog db) name
  rowId <- insertRow table row
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdInsertRow name rowId row, callback)
  return (rowId, callback)

-- | Update a row and enqueue the WAL command in one STM transaction.
updateRowSTM :: Database -> TableName -> RowId -> Vector Value
             -> ExceptT DbError STM (TMVar ())
updateRowSTM db name rowId row = do
  checkWritable db
  table <- lookupTable (dbCatalog db) name
  updateRow table name rowId row
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdUpdateRow name rowId row, callback)
  return callback

-- | Delete a row and enqueue the WAL command in one STM transaction.
deleteRowSTM :: Database -> TableName -> RowId
             -> ExceptT DbError STM (TMVar ())
deleteRowSTM db name rowId = do
  checkWritable db
  table <- lookupTable (dbCatalog db) name
  deleteRow table name rowId
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdDeleteRow name rowId, callback)
  return callback

-- | Drop a table and enqueue the WAL command in one STM transaction.
dropTableSTM :: Database -> TableName
             -> ExceptT DbError STM (TMVar ())
dropTableSTM db name = do
  checkWritable db
  dropTable (dbCatalog db) name
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdDropTable name, callback)
  return callback

-- | Select all rows from a table (pure STM read, no WAL involvement).
selectAllSTM :: Database -> TableName -> ExceptT DbError STM [(RowId, Row)]
selectAllSTM db name = do
  table <- lookupTable (dbCatalog db) name
  lift $ selectAll table

-- | Commit a list of pending ops in a single STM transaction.
-- Returns the last callback (waiting on it guarantees all prior entries are durable).
commitPendingOps :: Database -> [PendingOp] -> ExceptT DbError STM [TMVar ()]
commitPendingOps db ops = do
  checkWritable db
  mapM (commitOp db) ops

commitOp :: Database -> PendingOp -> ExceptT DbError STM (TMVar ())
commitOp db (PendingCreate name schema) = do
  _ <- createTable (dbCatalog db) name schema
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdCreateTable name schema, callback)
  return callback
commitOp db (PendingInsert name row) = do
  table <- lookupTable (dbCatalog db) name
  rowId <- insertRow table row
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdInsertRow name rowId row, callback)
  return callback
commitOp db (PendingUpdate name rowId row) = do
  table <- lookupTable (dbCatalog db) name
  updateRow table name rowId row
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdUpdateRow name rowId row, callback)
  return callback
commitOp db (PendingDelete name rowId) = do
  table <- lookupTable (dbCatalog db) name
  deleteRow table name rowId
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdDeleteRow name rowId, callback)
  return callback
commitOp db (PendingDrop name) = do
  dropTable (dbCatalog db) name
  callback <- lift newEmptyTMVar
  lift $ writeTBQueue (dbQueue db) (CmdDropTable name, callback)
  return callback
