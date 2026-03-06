{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Integration (integrationTests) where

import Hedgehog
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (readMVar, newEmptyMVar, putMVar, takeMVar)
import Control.Concurrent.STM
import Control.Monad.Trans.Except (runExceptT)
import qualified Data.Vector as V
import System.FilePath ((</>))
import System.IO (hClose)
import System.IO.Temp (withSystemTempDirectory)
import System.Timeout (timeout)

import HsDb
import HsDb.Integration (Database(..))
import HsDb.WAL.Types (DatabaseStatus(..))
import HsDb.WAL.Writer (WALHandle(..))

integrationTests :: Group
integrationTests = Group "Integration"
  [ ("prop_create_insert_select", prop_create_insert_select)
  , ("prop_durability_roundtrip", prop_durability_roundtrip)
  , ("prop_concurrent_inserts", prop_concurrent_inserts)
  , ("prop_drop_recreate", prop_drop_recreate)
  , ("prop_update_delete_lifecycle", prop_update_delete_lifecycle)
  , ("prop_withDatabase_cleanup", prop_withDatabase_cleanup)
  , ("prop_async_variants", prop_async_variants)
  , ("prop_flusher_crash_readonly", prop_flusher_crash_readonly)
  , ("prop_deterministic_shutdown", prop_deterministic_shutdown)
  , ("prop_concurrent_close_no_deadlock", prop_concurrent_close_no_deadlock)
  ]

-- Helper to unwrap Right in IO
expectRight :: Show e => Either e a -> IO a
expectRight (Right a) = return a
expectRight (Left e)  = error ("Expected Right, got Left: " ++ show e)

prop_create_insert_select :: Property
prop_create_insert_select = withTests 10 $ property $ do
  let schema = V.fromList [Column "id" TInt64 False, Column "name" TText True]
  let row = V.fromList [VInt64 42, VText "hello"]
  (rowId, rows) <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "users" schema)
      rid <- expectRight =<< runExceptT (durableInsert db "users" row)
      rs <- expectRight =<< runExceptT (selectAll db "users")
      return (rid, rs)
  rowId === 0
  rows === [(0, row)]

prop_durability_roundtrip :: Property
prop_durability_roundtrip = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let row1 = V.fromList [VInt32 1]
  let row2 = V.fromList [VInt32 2]
  rows <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    -- Session 1: write data
    _ <- withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      _ <- expectRight =<< runExceptT (durableInsert db "t" row1)
      _ <- expectRight =<< runExceptT (durableInsert db "t" row2)
      return ()
    -- Session 2: reopen and verify data survived
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, row1), (1, row2)]

prop_concurrent_inserts :: Property
prop_concurrent_inserts = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let n = 20
  ids <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      results <- newTVarIO ([] :: [Either DbError RowId])
      dones <- mapM (\i -> do
        done <- newEmptyTMVarIO
        _ <- forkIO $ do
          r <- runExceptT $ durableInsert db "t" (V.fromList [VInt32 (fromIntegral i)])
          atomically $ modifyTVar' results (r :)
          atomically $ putTMVar done ()
        return done
        ) [1..n :: Int]
      mapM_ (\d -> atomically (takeTMVar d)) dones
      atomically $ readTVar results
  let rights = [rid | Right rid <- ids]
  length rights === n
  -- All IDs unique: check by comparing length of deduplicated list
  let unique = removeDups rights
  length unique === n
  where
    removeDups [] = []
    removeDups (x:xs) = x : removeDups (filter (/= x) xs)

prop_drop_recreate :: Property
prop_drop_recreate = withTests 5 $ property $ do
  let schema1 = V.fromList [Column "a" TInt32 False]
  let schema2 = V.fromList [Column "b" TText False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema1)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      expectRight =<< runExceptT (durableDrop db "t")
      expectRight =<< runExceptT (durableCreateTable db "t" schema2)
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VText "hello"]))
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, V.fromList [VText "hello"])]

prop_update_delete_lifecycle :: Property
prop_update_delete_lifecycle = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" schema)
      r0 <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      r1 <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 2]))
      expectRight =<< runExceptT (durableUpdate db "t" r0 (V.fromList [VInt32 10]))
      expectRight =<< runExceptT (durableDelete db "t" r1)
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, V.fromList [VInt32 10])]

prop_withDatabase_cleanup :: Property
prop_withDatabase_cleanup = withTests 5 $ property $ do
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    _ <- withDatabase config $ \db -> do
      expectRight =<< runExceptT (durableCreateTable db "t" (V.fromList [Column "x" TInt32 False]))
      _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
      return ()
    -- Reopen: data should persist
    withDatabase config $ \db ->
      expectRight =<< runExceptT (selectAll db "t")
  result === [(0, V.fromList [VInt32 1])]

prop_async_variants :: Property
prop_async_variants = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  rows <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    withDatabase config $ \db -> do
      r1 <- expectRight =<< runExceptT (asyncCreateTable db "t" schema)
      snd r1  -- wait for durability
      r2 <- expectRight =<< runExceptT (asyncInsert db "t" (V.fromList [VInt32 42]))
      snd r2  -- wait for durability
      expectRight =<< runExceptT (selectAll db "t")
  rows === [(0, V.fromList [VInt32 42])]

prop_flusher_crash_readonly :: Property
prop_flusher_crash_readonly = withTests 5 $ property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  writeResult <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    db <- openDatabase config
    expectRight =<< runExceptT (durableCreateTable db "t" schema)
    _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
    -- Sabotage: close the WAL file handle to crash the flusher on next write
    walHandle <- readMVar (dbWALHandle db)
    hClose (walFileHandle walHandle)
    -- Trigger the crash: this insert will enqueue an item, the flusher will
    -- try to write to the closed handle, catch the error, signal the callback,
    -- and transition to read-only. durableInsert itself may return Right since
    -- the callback gets signaled even on error.
    _ <- runExceptT $ durableInsert db "t" (V.fromList [VInt32 2])
    -- Wait for flusher to transition to read-only (deterministic, no threadDelay)
    transitioned <- timeout 5000000 $ atomically $ do
      st <- readTVar (dbStatus db)
      case st of
        DbWritable -> retry  -- keep waiting
        _          -> return ()
    case transitioned of
      Nothing -> error "Timed out waiting for flusher to transition to read-only"
      Just () -> return ()
    -- Now the DB should be read-only; subsequent writes should fail
    runExceptT $ durableInsert db "t" (V.fromList [VInt32 3])
  case writeResult of
    Left (DatabaseNotWritable _) -> success
    other -> do annotateShow other; failure

prop_deterministic_shutdown :: Property
prop_deterministic_shutdown = withTests 5 $ property $ do
  -- Verify closeDatabase returns promptly without relying on timing
  evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    db <- openDatabase config
    expectRight =<< runExceptT (durableCreateTable db "t" (V.fromList [Column "x" TInt32 False]))
    _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
    _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 2]))
    -- closeDatabase should block on walDone and complete deterministically
    closeDatabase db
    -- If we get here, shutdown was deterministic (no hangs)
    return ()
  -- Verify data persisted by reopening
  rows <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    -- Write + close
    db <- openDatabase config
    expectRight =<< runExceptT (durableCreateTable db "t" (V.fromList [Column "x" TInt32 False]))
    _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
    closeDatabase db
    -- Reopen and read
    db2 <- openDatabase config
    result <- expectRight =<< runExceptT (selectAll db2 "t")
    closeDatabase db2
    return result
  rows === [(0, V.fromList [VInt32 1])]

prop_concurrent_close_no_deadlock :: Property
prop_concurrent_close_no_deadlock = withTests 5 $ property $ do
  result <- evalIO $ withSystemTempDirectory "hs-db-test" $ \dir -> do
    let config = defaultDatabaseConfig (dir </> "test.wal")
    db <- openDatabase config
    expectRight =<< runExceptT (durableCreateTable db "t" (V.fromList [Column "x" TInt32 False]))
    _ <- expectRight =<< runExceptT (durableInsert db "t" (V.fromList [VInt32 1]))
    -- Launch two concurrent close calls with a timeout to detect deadlocks
    done1 <- newEmptyMVar
    done2 <- newEmptyMVar
    _ <- forkIO $ closeDatabase db >> putMVar done1 ()
    _ <- forkIO $ closeDatabase db >> putMVar done2 ()
    -- Both should complete within 5 seconds (generous timeout)
    r1 <- timeout 5000000 (takeMVar done1)
    r2 <- timeout 5000000 (takeMVar done2)
    return (r1, r2)
  case result of
    (Just (), Just ()) -> success
    _ -> do annotate "Deadlock detected: concurrent closeDatabase calls did not complete"; failure
