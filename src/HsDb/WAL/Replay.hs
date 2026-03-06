{-# LANGUAGE OverloadedStrings #-}

-- | WAL replay: reads a WAL file from disk and reconstructs the in-memory
-- table catalog by re-applying each logged command in sequence order.
module HsDb.WAL.Replay
  ( replayWAL
  , replayWALWithCheckpoint
  ) where

import Control.Concurrent.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import qualified Data.ByteString as BS
import qualified Data.IntMap.Strict as IntMap
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import Data.Word (Word64)
import System.Directory (doesFileExist)

import HsDb.Types
import HsDb.Table
import HsDb.WAL.Types
import HsDb.WAL.Serialize
import HsDb.Checkpoint

-- | Replay a WAL file to reconstruct the database state.
-- Returns the reconstructed table catalog, the last sequence number,
-- and a list of warning messages (e.g. for corrupted trailing entries).
-- Missing file: returns empty catalog with seq 0.
-- Empty file (header only): returns empty catalog with seq 0.
replayWAL :: FilePath -> IO (Either DbError (TableCatalog, Word64, [Text]))
replayWAL path = do
  exists <- doesFileExist path
  if not exists
    then do
      catalog <- atomically newTableCatalog
      return (Right (catalog, 0, []))
    else do
      contents <- BS.readFile path
      if BS.null contents
        then do
          catalog <- atomically newTableCatalog
          return (Right (catalog, 0, []))
        else case readWALHeader contents of
          Left err -> return (Left (WALCorrupted (T.pack ("Header: " ++ err))))
          Right (WALHeader _ ver, rest)
            | ver /= walVersion ->
                return (Left (WALVersionMismatch walVersion ver))
            | otherwise -> do
                catalog <- atomically newTableCatalog
                replayEntries catalog rest 0 []

-- | Replay entries sequentially from the remaining bytes after the header.
-- Accumulates warning messages for non-fatal issues (e.g. corrupted trailing bytes).
replayEntries :: TableCatalog -> BS.ByteString -> Word64 -> [Text]
              -> IO (Either DbError (TableCatalog, Word64, [Text]))
replayEntries catalog bs lastSeq warnings
  | BS.null bs = return (Right (catalog, lastSeq, reverse warnings))
  | otherwise = case decodeFramed bs of
      Left err ->
        -- Corrupted trailing entry: append warning, return what we have
        let warning = T.pack ("Corrupted trailing WAL entry after seq " ++ show lastSeq
                              ++ ": " ++ show err)
        in return (Right (catalog, lastSeq, reverse (warning : warnings)))
      Right (entry, rest) -> do
        let seq' = walSeqNum entry
        if seq' <= lastSeq
          then return (Left (WALSequenceError (T.pack ("Expected seq > "
                              ++ show lastSeq ++ ", got " ++ show seq'))))
          else do
            result <- atomically $ runExceptT $ applyCommand catalog (walCommand entry)
            case result of
              Left err -> return (Left err)
              Right () -> replayEntries catalog rest seq' warnings

-- | Apply a single WAL command to the table catalog in STM.
applyCommand :: TableCatalog -> WALCommand -> ExceptT DbError STM ()
applyCommand catalog cmd = case cmd of
  CmdCreateTable name schema -> do
    _ <- createTable catalog name schema
    return ()

  CmdInsertRow name rowId row -> do
    table <- lookupTable catalog name
    insertRowWithId table name rowId row

  CmdUpdateRow name rowId row -> do
    table <- lookupTable catalog name
    updateRow table name rowId row

  CmdDeleteRow name rowId -> do
    table <- lookupTable catalog name
    deleteRow table name rowId

  CmdDropTable name -> dropTable catalog name

-- | Recovery with checkpoint support: load checkpoint if available,
-- then replay only WAL entries after the checkpoint sequence number.
-- Falls back to full WAL replay if checkpoint is corrupted.
replayWALWithCheckpoint :: FilePath -> FilePath
                        -> IO (Either DbError (TableCatalog, Word64, [Text]))
replayWALWithCheckpoint cpPath walPath = do
  cpExists <- doesFileExist cpPath
  if not cpExists
    then replayWAL walPath
    else do
      cpResult <- readCheckpoint cpPath
      case cpResult of
        Left cpErr -> do
          -- Checkpoint corrupted — fall back to full replay
          let warning = "Checkpoint load failed (" <> cpErr <> "), falling back to full WAL replay"
          fullResult <- replayWAL walPath
          case fullResult of
            Left err -> return (Left err)
            Right (catalog, seq', warnings) ->
              return (Right (catalog, seq', warning : warnings))
        Right (tableData, cpSeq) -> do
          -- Rebuild catalog from checkpoint
          catalog <- atomically newTableCatalog
          rebuildResult <- rebuildFromCheckpoint catalog tableData
          case rebuildResult of
            Left err -> return (Left err)
            Right () -> do
              -- Replay WAL entries after the checkpoint
              walExists <- doesFileExist walPath
              if not walExists
                then return (Right (catalog, cpSeq, []))
                else do
                  contents <- BS.readFile walPath
                  if BS.null contents
                    then return (Right (catalog, cpSeq, []))
                    else case readWALHeader contents of
                      Left err -> return (Left (WALCorrupted (T.pack ("Header: " ++ err))))
                      Right (WALHeader _ ver, rest)
                        | ver /= walVersion ->
                            return (Left (WALVersionMismatch walVersion ver))
                        | otherwise ->
                            replayEntriesSkipping catalog rest cpSeq cpSeq []

-- | Replay entries, skipping those with sequence <= skipSeq.
replayEntriesSkipping :: TableCatalog -> BS.ByteString -> Word64 -> Word64 -> [Text]
                      -> IO (Either DbError (TableCatalog, Word64, [Text]))
replayEntriesSkipping catalog bs skipSeq lastSeq warnings
  | BS.null bs = return (Right (catalog, lastSeq, reverse warnings))
  | otherwise = case decodeFramed bs of
      Left err ->
        let warning = T.pack ("Corrupted trailing WAL entry after seq " ++ show lastSeq
                              ++ ": " ++ show err)
        in return (Right (catalog, lastSeq, reverse (warning : warnings)))
      Right (entry, rest) -> do
        let seq' = walSeqNum entry
        if seq' <= skipSeq
          then replayEntriesSkipping catalog rest skipSeq lastSeq warnings
          else if seq' <= lastSeq
            then return (Left (WALSequenceError (T.pack ("Expected seq > "
                                ++ show lastSeq ++ ", got " ++ show seq'))))
            else do
              result <- atomically $ runExceptT $ applyCommand catalog (walCommand entry)
              case result of
                Left err -> return (Left err)
                Right () -> replayEntriesSkipping catalog rest skipSeq seq' warnings

-- | Rebuild the in-memory catalog from checkpoint data.
rebuildFromCheckpoint :: TableCatalog -> [(TableName, Schema, Int, [(RowId, Vector Value)])]
                      -> IO (Either DbError ())
rebuildFromCheckpoint catalog tables = atomically $ runExceptT $
  mapM_ restoreTable tables
  where
    restoreTable (name, schema, counter, rows) = do
      table <- createTable catalog name schema
      lift $ writeTVar (tableCounter table) counter
      lift $ writeTVar (tableRows table) (IntMap.fromList rows)
