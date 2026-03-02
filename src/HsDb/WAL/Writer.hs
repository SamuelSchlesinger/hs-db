{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | WAL writer and flusher thread. Opens the WAL file, assigns sequence
-- numbers, batches writes, fsyncs to disk, and signals durability callbacks.
module HsDb.WAL.Writer
  ( WALHandle(..)
  , openWAL
  , closeWAL
  , flusherThread
  ) where

import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar)
import Control.Concurrent.STM
import Control.Exception (SomeException, catch, finally, throwIO)
import Control.Monad (when)
import qualified Data.ByteString as BS
import Data.IORef (IORef, newIORef, readIORef, writeIORef)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Clock (getCurrentTime)
import Data.Word (Word64)
import Foreign.C.Types (CInt(..))
import System.IO.Error (mkIOError, illegalOperationErrorType)
import System.IO (Handle, hFlush, IOMode(..), openBinaryFile, hClose, hFileSize, SeekMode(..), hSeek)
import System.Posix.IO (openFd, closeFd, defaultFileFlags, OpenMode(..))
import System.Posix.Types (Fd(..))

import HsDb.WAL.Types
import HsDb.WAL.Serialize

-- POSIX fsync via FFI
foreign import ccall "fsync" c_fsync :: CInt -> IO CInt

fsyncFd :: Fd -> IO ()
fsyncFd (Fd fd) = do
  result <- c_fsync fd
  when (result /= 0) $
    throwIO (mkIOError illegalOperationErrorType "fsync failed" Nothing Nothing)

-- | Handle to an open WAL file, with the queue and state.
data WALHandle = WALHandle
  { walFileHandle :: !Handle
  , walSyncFd     :: !Fd       -- ^ Separate fd for fsync only
  , walQueue      :: !(TBQueue WALQueueItem)
  , walSeqCounter :: !(IORef Word64)
  , walStatus     :: !(TVar DatabaseStatus)
  , walDone       :: !(MVar (Maybe Text))  -- ^ Flusher signals on exit: Nothing=clean, Just err=error
  }

-- | Open (or create) a WAL file. Verifies the header if the file exists,
-- or writes a fresh header if the file is new.
-- Returns the WAL handle and the last sequence number found (0 if new).
openWAL :: DatabaseConfig -> TVar DatabaseStatus -> IO (WALHandle, Word64)
openWAL config status = do
  let path = configWALPath config
  h <- openBinaryFile path ReadWriteMode
  size <- hFileSize h
  lastSeq <- if size == 0
    then do
      BS.hPut h (writeWALHeader (WALHeader walMagic walVersion))
      hFlush h
      return 0
    else do
      hSeek h AbsoluteSeek 0
      headerBs <- BS.hGet h 6
      case readWALHeader headerBs of
        Left err -> throwIO (userError ("WAL header error: " ++ err))
        Right (WALHeader _ ver, _)
          | ver /= walVersion -> throwIO (userError ("WAL version mismatch: expected "
                                                      ++ show walVersion ++ ", got " ++ show ver))
          | otherwise -> return 0
  hSeek h SeekFromEnd 0
  -- Open a separate fd for fsync (the Handle remains for buffered IO)
  syncFd <- openFd path WriteOnly defaultFileFlags
  queue <- newTBQueueIO (fromIntegral (configQueueCapacity config))
  seqRef <- newIORef lastSeq
  done <- newEmptyMVar
  return (WALHandle h syncFd queue seqRef status done, lastSeq)

-- | The flusher thread: drains the queue, assigns sequence numbers and
-- timestamps, serializes entries, writes them to disk, fsyncs, and signals
-- durability callbacks. Catches IO exceptions and transitions to read-only.
flusherThread :: WALHandle -> IO ()
flusherThread wal = do
    result <- loop
    putMVar (walDone wal) result
  where
    loop = do
      -- Block until at least one item is available, checking status
      itemsOrDone <- atomically $ do
        st <- readTVar (walStatus wal)
        case st of
          DbShuttingDown -> return Nothing
          DbReadOnly _   -> return Nothing
          DbWritable     -> Just <$> drainQueue (walQueue wal)
      case itemsOrDone of
        Nothing -> drainRemaining
        Just items -> do
          flushResult <- flushBatch wal items
          case flushResult of
            Left err -> do
              atomically $ writeTVar (walStatus wal) (DbReadOnly err)
              -- Signal any callbacks orphaned between batch drain and status change
              orphaned <- atomically $ tryDrainQueue (walQueue wal)
              signalCallbacks orphaned
              return (Just err)
            Right () -> loop

    drainRemaining = do
      items <- atomically $ tryDrainQueue (walQueue wal)
      case items of
        [] -> return Nothing
        _  -> do
          result <- flushBatch wal items
          case result of
            Left err -> return (Just err)
            Right () -> return Nothing

-- | Block until at least one item, then drain all remaining.
drainQueue :: TBQueue a -> STM [a]
drainQueue q = do
  first <- readTBQueue q
  rest <- drainRest q
  return (first : rest)
  where
    drainRest queue = do
      mItem <- tryReadTBQueue queue
      case mItem of
        Nothing   -> return []
        Just item -> (item :) <$> drainRest queue

-- | Non-blocking drain: get whatever is available.
tryDrainQueue :: TBQueue a -> STM [a]
tryDrainQueue q = do
  mItem <- tryReadTBQueue q
  case mItem of
    Nothing   -> return []
    Just item -> (item :) <$> tryDrainQueue q

-- | Flush a batch of WAL items: assign sequence numbers, encode, write, fsync,
-- and signal all callbacks.
flushBatch :: WALHandle -> [WALQueueItem] -> IO (Either Text ())
flushBatch wal items =
  (do
    now <- getCurrentTime
    entries <- mapM (assignSeqNum now) items
    let encoded = BS.concat (map (encodeFramed . fst) entries)
    BS.hPut (walFileHandle wal) encoded
    hFlush (walFileHandle wal)
    fsyncFd (walSyncFd wal)
    signalCallbacks entries
    return (Right ())
  ) `catch` \(e :: SomeException) -> do
    -- Signal all callbacks so callers don't block forever
    signalCallbacks items
    return (Left (T.pack ("WAL flush error: " ++ show e)))
  where
    assignSeqNum now (cmd, callback) = do
      seqN <- readIORef (walSeqCounter wal)
      let nextSeq = seqN + 1
      writeIORef (walSeqCounter wal) nextSeq
      return (WALEntry nextSeq now cmd, callback)

-- | Signal all callbacks in a batch, exception-safe per callback.
-- Each callback is attempted independently so an async exception
-- during one signal does not prevent the rest from being signaled.
signalCallbacks :: [(a, TMVar ())] -> IO ()
signalCallbacks = mapM_ $ \(_, cb) ->
  atomically (putTMVar cb ()) `catch` \(_ :: SomeException) -> return ()

-- | Close the WAL file handle and sync fd.
-- Exception-safe: closeFd runs even if hClose throws.
closeWAL :: WALHandle -> IO ()
closeWAL wal =
  hClose (walFileHandle wal) `finally` closeFd (walSyncFd wal)
