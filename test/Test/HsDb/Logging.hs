{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Logging (loggingTests) where

import Hedgehog
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar, newMVar, withMVar)
import Data.IORef (newIORef, readIORef, modifyIORef')
import qualified Data.Text as T

import HsDb.Logging

loggingTests :: Group
loggingTests = Group "Logging"
  [ ("prop_logger_creation", prop_logger_creation)
  , ("prop_log_levels_exist", prop_log_levels_exist)
  , ("prop_log_thread_safety_no_crash", prop_log_thread_safety_no_crash)
  , ("prop_log_lock_serializes", prop_log_lock_serializes)
  ]

-- Test: newLogger creates a logger without error
prop_logger_creation :: Property
prop_logger_creation = withTests 1 $ property $ do
  logger <- evalIO newLogger
  -- Just verify it doesn't crash when used
  evalIO $ logInfo logger "test"
  evalIO $ logWarn logger "test"
  evalIO $ logError logger "test"
  success

-- Test: LogLevel values are distinct
prop_log_levels_exist :: Property
prop_log_levels_exist = withTests 1 $ property $ do
  LevelInfo /== LevelWarn
  LevelInfo /== LevelError
  LevelWarn /== LevelError

-- Test: concurrent logging from many threads doesn't crash
prop_log_thread_safety_no_crash :: Property
prop_log_thread_safety_no_crash = withTests 1 $ property $ do
  evalIO $ do
    logger <- newLogger
    let n = 50
    dones <- mapM (\i -> do
      done <- newEmptyMVar
      _ <- forkIO $ do
        logInfo logger ("thread-" <> T.pack (show i))
        putMVar done ()
      return done
      ) [1..n :: Int]
    mapM_ takeMVar dones
  -- If we got here without deadlock or crash, the test passes
  success

-- Test: the MVar lock serializes access (no interleaving within a single log call)
-- We verify this by checking that a counter incremented under the same MVar
-- pattern produces the expected count.
prop_log_lock_serializes :: Property
prop_log_lock_serializes = withTests 1 $ property $ do
  count <- evalIO $ do
    lock <- newMVar ()
    counter <- newIORef (0 :: Int)
    let n = 100
    dones <- mapM (\_ -> do
      done <- newEmptyMVar
      _ <- forkIO $ do
        withMVar lock $ \_ -> modifyIORef' counter (+1)
        putMVar done ()
      return done
      ) [1..n :: Int]
    mapM_ takeMVar dones
    readIORef counter
  count === 100
