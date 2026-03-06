{-# LANGUAGE OverloadedStrings #-}

-- | Main public API for hs-db. Provides bracket-style database lifecycle
-- management, synchronous durable operations that block until fsync, and
-- async variants that return a durability waiter.
module HsDb
  ( -- * Database lifecycle
    withDatabase
  , openDatabase
  , closeDatabase
    -- * Synchronous durable operations
  , durableCreateTable
  , durableInsert
  , durableUpdate
  , durableDelete
  , durableDrop
    -- * Async variants (return durability waiter)
  , asyncCreateTable
  , asyncInsert
  , asyncUpdate
  , asyncDelete
  , asyncDrop
    -- * Read operations
  , selectAll
    -- * Checkpointing
  , checkpoint
    -- * Types (re-exported)
  , Database
  , DatabaseConfig(..)
  , defaultDatabaseConfig
  , Value(..)
  , ColumnType(..)
  , Column(..)
  , Schema
  , Row
  , RowId
  , TableName
  , DbError(..)
  ) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newMVar, readMVar, takeMVar, putMVar, tryTakeMVar)
import Control.Concurrent.STM
import Control.Exception (SomeException, bracket, finally, throwIO, try)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT)
import Data.IORef (readIORef, writeIORef)
import Data.Text (unpack, pack)
import Data.Vector (Vector)
import System.Directory (doesFileExist)
import System.IO (IOMode(..), openBinaryFile, hClose, hPutStrLn, stderr)

import HsDb.Types
import HsDb.Integration (Database(..), createTableSTM, insertRowSTM,
                          updateRowSTM, deleteRowSTM, dropTableSTM, selectAllSTM,
                          atomicallyE)
import HsDb.WAL.Types
import HsDb.WAL.Writer (WALHandle(..), openWAL, closeWAL, flusherThread)
import HsDb.WAL.Replay (replayWAL, replayWALWithCheckpoint)
import HsDb.Checkpoint (writeCheckpoint)

-- | Open a database using bracket-style resource management.
withDatabase :: DatabaseConfig -> (Database -> IO a) -> IO a
withDatabase config action = bracket (openDatabase config) closeDatabase action

-- | Open a database: replay WAL, create flusher thread.
openDatabase :: DatabaseConfig -> IO Database
openDatabase config = do
  assert64Bit `seq` return ()
  if configQueueCapacity config <= 0
    then throwIO (userError ("configQueueCapacity must be positive, got "
                              ++ show (configQueueCapacity config)))
    else return ()
  let path = configWALPath config
  exists <- doesFileExist path
  if not exists
    then do
      h <- openBinaryFile path WriteMode
      hClose h
    else return ()
  replayResult <- case configCheckpointPath config of
    Nothing   -> replayWAL path
    Just cpPath -> replayWALWithCheckpoint cpPath path
  case replayResult of
    Left err -> throwIO (userError (show err))
    Right (catalog, lastSeq, warnings) -> do
      mapM_ (\w -> hPutStrLn stderr (unpack w)) warnings
      status <- newTVarIO DbWritable
      (walHandle, _) <- openWAL config status
      writeIORef (walSeqCounter walHandle) lastSeq
      walMVar <- newMVar walHandle
      -- Supervised flusher: catches exceptions, transitions to read-only, signals walDone
      _ <- forkIO $ do
        result <- try (flusherThread walHandle)
        case result of
          Left (e :: SomeException) -> do
            atomically $ writeTVar status (DbReadOnly (pack (show e)))
            putMVar (walDone walHandle) (Just (pack (show e)))
          Right () -> return ()  -- flusherThread already signaled walDone
      shutdownOnce <- newMVar ()
      return (Database catalog (walQueue walHandle) status walMVar shutdownOnce)

-- | Close the database: signal shutdown, wait for flusher to drain via walDone, close WAL.
-- Safe to call concurrently: the first call performs shutdown, subsequent calls return immediately.
closeDatabase :: Database -> IO ()
closeDatabase db = do
  acquired <- tryTakeMVar (dbShutdownOnce db)
  case acquired of
    Nothing -> return ()  -- Another thread is already shutting down
    Just () -> do
      walHandle <- readMVar (dbWALHandle db)
      -- Signal shutdown — flusher will see this, drain remaining items, then signal walDone
      atomically $ writeTVar (dbStatus db) DbShuttingDown
      -- Block until flusher has finished (deterministic, no threadDelay or killThread)
      -- Use finally to ensure WAL handles are closed even if we throw on error
      shutdownResult <- takeMVar (walDone walHandle)
      (case shutdownResult of
        Nothing  -> return ()
        Just err -> throwIO (userError ("WAL shutdown error: " ++ unpack err))
       ) `finally` closeWAL walHandle

-- | Select all rows from a table (pure STM read, no durability needed).
selectAll :: Database -> TableName -> ExceptT DbError IO [(RowId, Row)]
selectAll db name = atomicallyE $ selectAllSTM db name

-- Synchronous durable operations --

-- | Create a table, blocking until the WAL entry is fsynced to disk.
durableCreateTable :: Database -> TableName -> Schema -> ExceptT DbError IO ()
durableCreateTable db name schema = do
  (_, callback) <- atomicallyE $ createTableSTM db name schema
  lift $ atomically $ takeTMVar callback

-- | Insert a row, blocking until the WAL entry is fsynced. Returns the
-- assigned 'RowId' on success.
durableInsert :: Database -> TableName -> Vector Value -> ExceptT DbError IO RowId
durableInsert db name row = do
  (rowId, callback) <- atomicallyE $ insertRowSTM db name row
  lift $ atomically $ takeTMVar callback
  return rowId

-- | Update an existing row in place, blocking until the WAL entry is fsynced.
durableUpdate :: Database -> TableName -> RowId -> Vector Value -> ExceptT DbError IO ()
durableUpdate db name rowId row = do
  callback <- atomicallyE $ updateRowSTM db name rowId row
  lift $ atomically $ takeTMVar callback

-- | Delete a row by ID, blocking until the WAL entry is fsynced.
durableDelete :: Database -> TableName -> RowId -> ExceptT DbError IO ()
durableDelete db name rowId = do
  callback <- atomicallyE $ deleteRowSTM db name rowId
  lift $ atomically $ takeTMVar callback

-- | Drop a table, blocking until the WAL entry is fsynced.
durableDrop :: Database -> TableName -> ExceptT DbError IO ()
durableDrop db name = do
  callback <- atomicallyE $ dropTableSTM db name
  lift $ atomically $ takeTMVar callback

-- Async variants --

-- | Create a table, returning an @IO ()@ action that blocks until durable.
-- The table is immediately visible to STM readers.
asyncCreateTable :: Database -> TableName -> Schema
                 -> ExceptT DbError IO ((), IO ())
asyncCreateTable db name schema = do
  (_, callback) <- atomicallyE $ createTableSTM db name schema
  return ((), atomically (takeTMVar callback))

-- | Insert a row, returning the 'RowId' and a durability waiter.
asyncInsert :: Database -> TableName -> Vector Value
            -> ExceptT DbError IO (RowId, IO ())
asyncInsert db name row = do
  (rowId, callback) <- atomicallyE $ insertRowSTM db name row
  return (rowId, atomically (takeTMVar callback))

-- | Update a row, returning a durability waiter.
asyncUpdate :: Database -> TableName -> RowId -> Vector Value
            -> ExceptT DbError IO ((), IO ())
asyncUpdate db name rowId row = do
  callback <- atomicallyE $ updateRowSTM db name rowId row
  return ((), atomically (takeTMVar callback))

-- | Delete a row, returning a durability waiter.
asyncDelete :: Database -> TableName -> RowId
            -> ExceptT DbError IO ((), IO ())
asyncDelete db name rowId = do
  callback <- atomicallyE $ deleteRowSTM db name rowId
  return ((), atomically (takeTMVar callback))

-- | Drop a table, returning a durability waiter.
asyncDrop :: Database -> TableName
          -> ExceptT DbError IO ((), IO ())
asyncDrop db name = do
  callback <- atomicallyE $ dropTableSTM db name
  return ((), atomically (takeTMVar callback))

-- | Write a checkpoint of the current database state.
-- The checkpoint captures the committed state atomically via STM.
-- Pass the WAL sequence number from the WALHandle to record the checkpoint point.
checkpoint :: Database -> FilePath -> IO ()
checkpoint db cpPath = do
  walHandle <- readMVar (dbWALHandle db)
  walSeq <- readIORef (walSeqCounter walHandle)
  writeCheckpoint cpPath (dbCatalog db) walSeq
