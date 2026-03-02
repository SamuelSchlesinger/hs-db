{-# LANGUAGE OverloadedStrings #-}

module HsDb.WAL.Replay
  ( replayWAL
  ) where

import Control.Concurrent.STM
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word (Word64)
import System.Directory (doesFileExist)

import HsDb.Types
import HsDb.Table
import HsDb.WAL.Types
import HsDb.WAL.Serialize

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
            result <- atomically $ applyCommand catalog (walCommand entry)
            case result of
              Left err -> return (Left err)
              Right () -> replayEntries catalog rest seq' warnings

-- | Apply a single WAL command to the table catalog in STM.
applyCommand :: TableCatalog -> WALCommand -> STM (Either DbError ())
applyCommand catalog cmd = case cmd of
  CmdCreateTable name schema -> do
    result <- createTable catalog name schema
    case result of
      Left err -> return (Left err)
      Right _  -> return (Right ())

  CmdInsertRow name rowId row -> do
    tables <- readTVar catalog
    case Map.lookup name tables of
      Nothing    -> return (Left (TableNotFound name))
      Just table -> insertRowWithId table name rowId row

  CmdUpdateRow name rowId row -> do
    tables <- readTVar catalog
    case Map.lookup name tables of
      Nothing    -> return (Left (TableNotFound name))
      Just table -> updateRow table name rowId row

  CmdDeleteRow name rowId -> do
    tables <- readTVar catalog
    case Map.lookup name tables of
      Nothing    -> return (Left (TableNotFound name))
      Just table -> deleteRow table name rowId

  CmdDropTable name -> dropTable catalog name
