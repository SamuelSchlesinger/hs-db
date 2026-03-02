{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import Data.IORef (writeIORef)
import Data.Text (unpack, pack)
import Data.Vector (Vector)
import System.Directory (doesFileExist)
import System.IO (IOMode(..), openBinaryFile, hClose)

import HsDb.Types
import HsDb.Integration (Database(..), createTableSTM, insertRowSTM,
                          updateRowSTM, deleteRowSTM, dropTableSTM, selectAllSTM)
import HsDb.WAL.Types
import HsDb.WAL.Writer (WALHandle(..), openWAL, closeWAL, flusherThread)
import HsDb.WAL.Replay (replayWAL)

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
  replayResult <- replayWAL path
  case replayResult of
    Left err -> throwIO (userError (show err))
    Right (catalog, lastSeq, _warnings) -> do
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
selectAll :: Database -> TableName -> IO (Either DbError [(RowId, Row)])
selectAll db name = atomically $ selectAllSTM db name

-- Synchronous durable operations --

durableCreateTable :: Database -> TableName -> Schema -> IO (Either DbError ())
durableCreateTable db name schema = do
  result <- atomically $ createTableSTM db name schema
  case result of
    Left err -> return (Left err)
    Right (_, callback) -> do
      atomically $ takeTMVar callback
      return (Right ())

durableInsert :: Database -> TableName -> Vector Value -> IO (Either DbError RowId)
durableInsert db name row = do
  result <- atomically $ insertRowSTM db name row
  case result of
    Left err -> return (Left err)
    Right (rowId, callback) -> do
      atomically $ takeTMVar callback
      return (Right rowId)

durableUpdate :: Database -> TableName -> RowId -> Vector Value -> IO (Either DbError ())
durableUpdate db name rowId row = do
  result <- atomically $ updateRowSTM db name rowId row
  case result of
    Left err -> return (Left err)
    Right callback -> do
      atomically $ takeTMVar callback
      return (Right ())

durableDelete :: Database -> TableName -> RowId -> IO (Either DbError ())
durableDelete db name rowId = do
  result <- atomically $ deleteRowSTM db name rowId
  case result of
    Left err -> return (Left err)
    Right callback -> do
      atomically $ takeTMVar callback
      return (Right ())

durableDrop :: Database -> TableName -> IO (Either DbError ())
durableDrop db name = do
  result <- atomically $ dropTableSTM db name
  case result of
    Left err -> return (Left err)
    Right callback -> do
      atomically $ takeTMVar callback
      return (Right ())

-- Async variants --

asyncCreateTable :: Database -> TableName -> Schema
                 -> IO (Either DbError ((), IO ()))
asyncCreateTable db name schema = do
  result <- atomically $ createTableSTM db name schema
  case result of
    Left err -> return (Left err)
    Right (_, callback) ->
      return (Right ((), atomically (takeTMVar callback)))

asyncInsert :: Database -> TableName -> Vector Value
            -> IO (Either DbError (RowId, IO ()))
asyncInsert db name row = do
  result <- atomically $ insertRowSTM db name row
  case result of
    Left err -> return (Left err)
    Right (rowId, callback) ->
      return (Right (rowId, atomically (takeTMVar callback)))

asyncUpdate :: Database -> TableName -> RowId -> Vector Value
            -> IO (Either DbError ((), IO ()))
asyncUpdate db name rowId row = do
  result <- atomically $ updateRowSTM db name rowId row
  case result of
    Left err -> return (Left err)
    Right callback ->
      return (Right ((), atomically (takeTMVar callback)))

asyncDelete :: Database -> TableName -> RowId
            -> IO (Either DbError ((), IO ()))
asyncDelete db name rowId = do
  result <- atomically $ deleteRowSTM db name rowId
  case result of
    Left err -> return (Left err)
    Right callback ->
      return (Right ((), atomically (takeTMVar callback)))

asyncDrop :: Database -> TableName
          -> IO (Either DbError ((), IO ()))
asyncDrop db name = do
  result <- atomically $ dropTableSTM db name
  case result of
    Left err -> return (Left err)
    Right callback ->
      return (Right ((), atomically (takeTMVar callback)))
