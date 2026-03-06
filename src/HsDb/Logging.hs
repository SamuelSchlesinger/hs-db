{-# LANGUAGE OverloadedStrings #-}

-- | Structured logging to stderr with timestamps. Thread-safe via MVar.
module HsDb.Logging
  ( Logger
  , LogLevel(..)
  , newLogger
  , newLoggerWithLevel
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
data Logger = Logger
  { logLock     :: !(MVar ())
  , logMinLevel :: !LogLevel
  }

data LogLevel = LevelInfo | LevelWarn | LevelError
  deriving (Show, Eq, Ord)

-- | Create a logger that logs all levels.
newLogger :: IO Logger
newLogger = newLoggerWithLevel LevelInfo

-- | Create a logger with a minimum log level.
newLoggerWithLevel :: LogLevel -> IO Logger
newLoggerWithLevel minLevel = do
  lock <- newMVar ()
  return (Logger lock minLevel)

logInfo, logWarn, logError :: Logger -> Text -> IO ()
logInfo  = logAt LevelInfo
logWarn  = logAt LevelWarn
logError = logAt LevelError

logAt :: LogLevel -> Logger -> Text -> IO ()
logAt level logger msg
  | level < logMinLevel logger = return ()
  | otherwise = withMVar (logLock logger) $ \_ -> do
      now <- getCurrentTime
      let ts = formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%SZ" now
          tag = case level of
                  LevelInfo  -> "INFO"
                  LevelWarn  -> "WARN"
                  LevelError -> "ERROR"
      hPutStrLn stderr (ts ++ " [" ++ tag ++ "] " ++ T.unpack msg)
