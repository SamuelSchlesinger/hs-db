{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.WAL.Replay (replayTests) where

import Hedgehog
import Control.Concurrent.STM
import qualified Data.ByteString as BS
import qualified Data.Map.Strict as Map
import Data.Time (UTCTime(..))
import Data.Time.Calendar (fromGregorian)
import Data.Time.Clock (secondsToDiffTime)
import qualified Data.Vector as V
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import HsDb.Types
import HsDb.Table (selectAll, tableSchema)
import HsDb.WAL.Types
import HsDb.WAL.Serialize
import HsDb.WAL.Replay

dummyTime :: UTCTime
dummyTime = UTCTime (fromGregorian 2026 1 1) (secondsToDiffTime 0)

replayTests :: Group
replayTests = Group "WAL.Replay"
  [ ("prop_missing_file_empty_db", prop_missing_file_empty_db)
  , ("prop_header_only_empty_db", prop_header_only_empty_db)
  , ("prop_replay_create_insert", prop_replay_create_insert)
  , ("prop_replay_drop_recreate", prop_replay_drop_recreate)
  , ("prop_replay_update_delete", prop_replay_update_delete)
  , ("prop_replay_corrupted_trailing", prop_replay_corrupted_trailing)
  , ("prop_replay_preserves_row_ids", prop_replay_preserves_row_ids)
  , ("prop_replay_duplicate_rowid", prop_replay_duplicate_rowid)
  , ("prop_replay_rejects_seq_zero", prop_replay_rejects_seq_zero)
  , ("prop_replay_rejects_non_monotonic", prop_replay_rejects_non_monotonic)
  ]

-- | Write a WAL file with the given entries.
writeTestWAL :: FilePath -> [WALEntry] -> IO ()
writeTestWAL path entries = do
  let header = writeWALHeader (WALHeader walMagic walVersion)
  let body = BS.concat (map encodeFramed entries)
  BS.writeFile path (header <> body)

prop_missing_file_empty_db :: Property
prop_missing_file_empty_db = property $ do
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    replayWAL (dir </> "nonexistent.wal")
  case result of
    Right (catalog, seqN, _warnings) -> do
      tables <- evalIO $ atomically $ readTVar catalog
      Map.size tables === 0
      seqN === 0
    Left err -> do annotateShow err; failure

prop_header_only_empty_db :: Property
prop_header_only_empty_db = property $ do
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "empty.wal"
    BS.writeFile path (writeWALHeader (WALHeader walMagic walVersion))
    replayWAL path
  case result of
    Right (catalog, seqN, _warnings) -> do
      tables <- evalIO $ atomically $ readTVar catalog
      Map.size tables === 0
      seqN === 0
    Left err -> do annotateShow err; failure

prop_replay_create_insert :: Property
prop_replay_create_insert = property $ do
  let schema = V.fromList [Column "id" TInt64 False, Column "name" TText True]
  let row = V.fromList [VInt64 42, VText "hello"]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "users" schema)
      , WALEntry 2 dummyTime (CmdInsertRow "users" 0 row)
      ]
    replayWAL path
  case result of
    Right (catalog, seqN, _warnings) -> do
      seqN === 2
      rows <- evalIO $ atomically $ do
        tables <- readTVar catalog
        case Map.lookup "users" tables of
          Nothing -> error "Table 'users' not found"
          Just table -> selectAll table
      rows === [(0, row)]
    Left err -> do annotateShow err; failure

prop_replay_drop_recreate :: Property
prop_replay_drop_recreate = property $ do
  let schema1 = V.fromList [Column "a" TInt32 False]
  let schema2 = V.fromList [Column "b" TText False]
  let row = V.fromList [VText "world"]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "t" schema1)
      , WALEntry 2 dummyTime (CmdDropTable "t")
      , WALEntry 3 dummyTime (CmdCreateTable "t" schema2)
      , WALEntry 4 dummyTime (CmdInsertRow "t" 0 row)
      ]
    replayWAL path
  case result of
    Right (catalog, seqN, _warnings) -> do
      seqN === 4
      (s, rows) <- evalIO $ atomically $ do
        tables <- readTVar catalog
        case Map.lookup "t" tables of
          Nothing -> error "Table 't' not found"
          Just table -> do
            rs <- selectAll table
            return (tableSchema table, rs)
      s === schema2
      rows === [(0, row)]
    Left err -> do annotateShow err; failure

prop_replay_update_delete :: Property
prop_replay_update_delete = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let row1 = V.fromList [VInt32 1]
  let row2 = V.fromList [VInt32 2]
  let row3 = V.fromList [VInt32 3]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "t" schema)
      , WALEntry 2 dummyTime (CmdInsertRow "t" 0 row1)
      , WALEntry 3 dummyTime (CmdInsertRow "t" 1 row2)
      , WALEntry 4 dummyTime (CmdUpdateRow "t" 0 row3)
      , WALEntry 5 dummyTime (CmdDeleteRow "t" 1)
      ]
    replayWAL path
  case result of
    Right (catalog, seqN, _warnings) -> do
      seqN === 5
      rows <- evalIO $ atomically $ do
        tables <- readTVar catalog
        case Map.lookup "t" tables of
          Nothing -> error "Table 't' not found"
          Just table -> selectAll table
      rows === [(0, row3)]
    Left err -> do annotateShow err; failure

prop_replay_corrupted_trailing :: Property
prop_replay_corrupted_trailing = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let row = V.fromList [VInt32 1]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    let header = writeWALHeader (WALHeader walMagic walVersion)
    let goodEntry = encodeFramed (WALEntry 1 dummyTime (CmdCreateTable "t" schema))
    let goodEntry2 = encodeFramed (WALEntry 2 dummyTime (CmdInsertRow "t" 0 row))
    -- Append some garbage bytes at the end
    BS.writeFile path (header <> goodEntry <> goodEntry2 <> BS.pack [0xFF, 0xFF, 0xFF])
    replayWAL path
  case result of
    Right (catalog, seqN, warnings) -> do
      seqN === 2  -- Should have replayed 2 good entries
      -- Should have a warning about corrupted trailing entry
      assert (not (null warnings))
      rows <- evalIO $ atomically $ do
        tables <- readTVar catalog
        case Map.lookup "t" tables of
          Nothing -> error "Table 't' not found"
          Just table -> selectAll table
      rows === [(0, row)]
    Left err -> do annotateShow err; failure

prop_replay_preserves_row_ids :: Property
prop_replay_preserves_row_ids = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "t" schema)
      , WALEntry 2 dummyTime (CmdInsertRow "t" 0 (V.fromList [VInt32 10]))
      , WALEntry 3 dummyTime (CmdInsertRow "t" 5 (V.fromList [VInt32 50]))
      , WALEntry 4 dummyTime (CmdInsertRow "t" 10 (V.fromList [VInt32 100]))
      ]
    replayWAL path
  case result of
    Right (catalog, _, _warnings) -> do
      rows <- evalIO $ atomically $ do
        tables <- readTVar catalog
        case Map.lookup "t" tables of
          Nothing -> error "Table 't' not found"
          Just table -> selectAll table
      -- Row IDs should be preserved exactly
      map fst rows === [0, 5, 10]
      map snd rows === [V.fromList [VInt32 10], V.fromList [VInt32 50], V.fromList [VInt32 100]]
    Left err -> do annotateShow err; failure

prop_replay_duplicate_rowid :: Property
prop_replay_duplicate_rowid = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let row1 = V.fromList [VInt32 1]
  let row2 = V.fromList [VInt32 2]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "t" schema)
      , WALEntry 2 dummyTime (CmdInsertRow "t" 0 row1)
      , WALEntry 3 dummyTime (CmdInsertRow "t" 0 row2)  -- duplicate rowId 0
      ]
    replayWAL path
  case result of
    Left (DuplicateRowId tbl rid) -> do
      tbl === "t"
      rid === 0
    Left err -> do annotateShow err; failure
    Right _ -> do annotate "Expected DuplicateRowId error"; failure

prop_replay_rejects_seq_zero :: Property
prop_replay_rejects_seq_zero = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 0 dummyTime (CmdCreateTable "t" schema)  -- seq 0 should be rejected
      ]
    replayWAL path
  case result of
    Left (WALSequenceError _) -> success
    Left err -> do annotateShow err; failure
    Right _ -> do annotate "Expected WALSequenceError for seq=0"; failure

prop_replay_rejects_non_monotonic :: Property
prop_replay_rejects_non_monotonic = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let row = V.fromList [VInt32 1]
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let path = dir </> "test.wal"
    writeTestWAL path
      [ WALEntry 1 dummyTime (CmdCreateTable "t" schema)
      , WALEntry 2 dummyTime (CmdInsertRow "t" 0 row)
      , WALEntry 2 dummyTime (CmdInsertRow "t" 1 row)  -- non-monotonic: 2 again
      ]
    replayWAL path
  case result of
    Left (WALSequenceError _) -> success
    Left err -> do annotateShow err; failure
    Right _ -> do annotate "Expected WALSequenceError for non-monotonic seq"; failure
