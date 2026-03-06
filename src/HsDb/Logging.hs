{-# LANGUAGE OverloadedStrings #-}

-- | Structured logging to stderr with timestamps. Thread-safe via MVar.
module HsDb.Logging
  ( Logger
  , LogLevel(..)
  , newLogger
  , logInfo
  , logWarn
  , logError
  ) where

import Control.Concurrent.MVar (MVar, newMVar, withMVar)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time (getCurrentTime, formatTime, defaultTimeLocale)
import System.IO (hPutStrLn, stderr)

-- | Thread-safe logger.
data Logger = Logger { logLock :: !(MVar ()) }

data LogLevel = LevelInfo | LevelWarn | LevelError
  deriving (Show, Eq)

newLogger :: IO Logger
newLogger = Logger <$> newMVar ()

logInfo, logWarn, logError :: Logger -> Text -> IO ()
logInfo  = logAt LevelInfo
logWarn  = logAt LevelWarn
logError = logAt LevelError

logAt :: LogLevel -> Logger -> Text -> IO ()
logAt level logger msg = withMVar (logLock logger) $ \_ -> do
  now <- getCurrentTime
  let ts = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%SZ" now
      tag = case level of
              LevelInfo  -> "INFO"
              LevelWarn  -> "WARN"
              LevelError -> "ERROR"
  hPutStrLn stderr (ts ++ " [" ++ tag ++ "] " ++ T.unpack msg)
