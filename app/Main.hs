-- | Entry point for the @hs-db-server@ executable. Parses CLI flags, opens
-- the database, and starts the PostgreSQL-protocol TCP server.
module Main (main) where

import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Data.Char (isDigit)
import System.Directory (createDirectoryIfMissing)
import System.Environment (getArgs)
import System.FilePath ((</>))
import System.Posix.Signals (installHandler, Handler(..), sigINT, sigTERM)

import HsDb (withDatabase, defaultDatabaseConfig)
import HsDb.Logging (LogLevel(..), newLoggerWithLevel)
import HsDb.Server (runServer, ServerConfig(..), defaultServerConfig)

main :: IO ()
main = do
  args <- getArgs
  let config = parseArgs args defaultServerConfig "./hs-db-data" LevelInfo
  case config of
    Left err -> putStrLn ("Error: " ++ err)
    Right (srvCfg, dataDir, logLevel) -> do
      createDirectoryIfMissing True dataDir
      let walPath = dataDir </> "wal"
          dbCfg = defaultDatabaseConfig walPath
      putStrLn ("Data directory: " ++ dataDir)
      -- Install signal handlers for graceful shutdown
      shutdownSignal <- newEmptyMVar
      let handler = Catch $ putMVar shutdownSignal ()
      _ <- installHandler sigINT handler Nothing
      _ <- installHandler sigTERM handler Nothing
      withDatabase dbCfg $ \db -> do
        logger <- newLoggerWithLevel logLevel
        -- Run server in background; wait for shutdown signal
        _ <- forkIO $ do
          runServer srvCfg logger db
        takeMVar shutdownSignal
        putStrLn "Shutting down..."
        -- withDatabase's bracket handles cleanup

parseArgs :: [String] -> ServerConfig -> String -> LogLevel
          -> Either String (ServerConfig, String, LogLevel)
parseArgs [] cfg dir ll = Right (cfg, dir, ll)
parseArgs ("--host":h:rest) cfg dir ll = parseArgs rest (cfg { serverHost = h }) dir ll
parseArgs ("--port":p:rest) cfg dir ll
  | all isDigit p, not (null p)
  , let n = read p :: Int, n > 0 && n <= 65535
  = parseArgs rest (cfg { serverPort = p }) dir ll
  | otherwise = Left ("Invalid port: " ++ p ++ " (must be 1-65535)")
parseArgs ("--data-dir":d:rest) cfg _ ll = parseArgs rest cfg d ll
parseArgs ("--max-connections":n:rest) cfg dir ll
  | all isDigit n, not (null n)
  = parseArgs rest (cfg { serverMaxConns = read n }) dir ll
  | otherwise = Left ("Invalid max-connections: " ++ n)
parseArgs ("--timeout":n:rest) cfg dir ll
  | all isDigit n, not (null n)
  = parseArgs rest (cfg { serverIdleTimeout = read n }) dir ll
  | otherwise = Left ("Invalid timeout: " ++ n)
parseArgs ("--log-level":l:rest) cfg dir _
  | l == "info"  = parseArgs rest cfg dir LevelInfo
  | l == "warn"  = parseArgs rest cfg dir LevelWarn
  | l == "error" = parseArgs rest cfg dir LevelError
  | otherwise = Left ("Invalid log-level: " ++ l ++ " (must be info|warn|error)")
parseArgs ("--help":_) _ _ _ = Left helpText
parseArgs [flag] _ _ _
  | flag `elem` ["--host", "--port", "--data-dir", "--max-connections", "--timeout", "--log-level"]
  = Left ("Missing value for " ++ flag)
parseArgs (unknown:_) _ _ _ = Left ("Unknown option: " ++ unknown)

helpText :: String
helpText = unlines
  [ "Usage: hs-db-server [OPTIONS]"
  , ""
  , "Options:"
  , "  --host HOST              Listen address (default: 127.0.0.1)"
  , "  --port PORT              Listen port (default: 5433)"
  , "  --data-dir DIR           Data directory (default: ./hs-db-data)"
  , "  --max-connections N      Max concurrent connections (default: 0 = unlimited)"
  , "  --timeout N              Idle timeout in seconds (default: 0 = none)"
  , "  --log-level LEVEL        Log level: info|warn|error (default: info)"
  , "  --help                   Show this help"
  ]
