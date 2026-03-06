{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Checkpoint (checkpointTests) where

import Hedgehog
import Control.Monad.Trans.Except (runExceptT)
import qualified Data.Vector as V
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import HsDb
import HsDb.Integration (Database(..))

checkpointTests :: Group
checkpointTests = Group "Checkpoint"
  [ ("prop_checkpoint_roundtrip", prop_checkpoint_roundtrip)
  , ("prop_checkpoint_recovery", prop_checkpoint_recovery)
  , ("prop_checkpoint_with_new_entries", prop_checkpoint_with_new_entries)
  , ("prop_checkpoint_multiple_tables", prop_checkpoint_multiple_tables)
  , ("prop_checkpoint_after_drop", prop_checkpoint_after_drop)
  , ("prop_no_checkpoint_fallback", prop_no_checkpoint_fallback)
  ]

expectRight :: Show e => Either e a -> IO a
expectRight (Right a) = return a
expectRight (Left e)  = error ("Expected Right, got Left: " ++ show e)

mkConfig :: FilePath -> DatabaseConfig
mkConfig dir = (defaultDatabaseConfig (dir </> "test.wal"))
  { configCheckpointPath = Just (dir </> "checkpoint") }

prop_checkpoint_roundtrip :: Property
prop_checkpoint_roundtrip = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False, Column "y" TText True]
  let row1 = V.fromList [VInt32 1, VText "hello"]
  let row2 = V.fromList [VInt32 2, VNull]
  rows <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    -- Session 1: write data, checkpoint, close
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" row1)
      _ <- expectRight =<< runExceptT (durableInsert db "t" row2)
      checkpoint db (dir </> "checkpoint")
    -- Session 2: recover from checkpoint
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, row1), (1, row2)]

prop_checkpoint_recovery :: Property
prop_checkpoint_recovery = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    -- Write, checkpoint
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      checkpoint db (dir </> "checkpoint")
    -- Reopen
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, V.fromList [VInt32 1])]

prop_checkpoint_with_new_entries :: Property
prop_checkpoint_with_new_entries = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      checkpoint db (dir </> "checkpoint")
      -- Write more data after checkpoint
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 2]))
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 3]))
      return ()
    -- Reopen: should recover checkpoint + replay new entries
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  rows === [ (0, V.fromList [VInt32 1])
           , (1, V.fromList [VInt32 2])
           , (2, V.fromList [VInt32 3])
           ]

prop_checkpoint_multiple_tables :: Property
prop_checkpoint_multiple_tables = withTests 5 $ property $ do
  let schema1 = V.fromList [Column "a" TInt32 False]
  let schema2 = V.fromList [Column "b" TText False]
  (rows1, rows2) <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t1" schema1)
      expectRight =<< runExceptT (durableCreateTable db "t2" schema2)
      _ <- expectRight =<< runExceptT (durableInsert db "t1" (V.fromList [VInt32 42]))
      _ <- expectRight =<< runExceptT (durableInsert db "t2" (V.fromList [VText "hi"]))
      checkpoint db (dir </> "checkpoint")
    withDatabase config $ \db -> do
      r1 <- expectRight =<< runExceptT (selectAll db "t1")
      r2 <- expectRight =<< runExceptT (selectAll db "t2")
      return (r1, r2)
  rows1 === [(0, V.fromList [VInt32 42])]
  rows2 === [(0, V.fromList [VText "hi"])]

prop_checkpoint_after_drop :: Property
prop_checkpoint_after_drop = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  result <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      expectRight =<< runExceptT (durableDrop db "t")
      checkpoint db (dir </> "checkpoint")
    withDatabase config $ \db ->
      runExceptT (selectAll db "t")
  case result of
    Left _ -> success  -- Table should not exist
    Right _ -> do annotate "Expected error: table should be dropped"; failure

prop_no_checkpoint_fallback :: Property
prop_no_checkpoint_fallback = withTests 5 $ property $ do
  -- Without a checkpoint file, should fall back to full WAL replay
  let schema = V.fromList [Column "x" TInt32 False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-cp-test" $ \dir -> do
    let config = mkConfig dir
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      return ()
    -- No checkpoint was written — should recover from WAL alone
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, V.fromList [VInt32 1])]
