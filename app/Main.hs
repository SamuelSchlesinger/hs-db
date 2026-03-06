-- | Entry point for the @hs-db-server@ executable. Parses CLI flags, opens
-- the database, and starts the PostgreSQL-protocol TCP server.
module Main (main) where

import Data.Char (isDigit)
import System.Directory (createDirectoryIfMissing)
import System.Environment (getArgs)
import System.FilePath ((</>))

import HsDb (withDatabase, defaultDatabaseConfig)
import HsDb.Server (runServer, ServerConfig(..), defaultServerConfig)

main :: IO ()
main = do
  args <- getArgs
  let config = parseArgs args defaultServerConfig "./hs-db-data"
  case config of
    Left err -> putStrLn ("Error: " ++ err)
    Right (srvCfg, dataDir) -> do
      createDirectoryIfMissing True dataDir
      let walPath = dataDir </> "wal"
          dbCfg = defaultDatabaseConfig walPath
      putStrLn ("Data directory: " ++ dataDir)
      withDatabase dbCfg $ \db ->
        runServer srvCfg db

parseArgs :: [String] -> ServerConfig -> String -> Either String (ServerConfig, String)
parseArgs [] cfg dir = Right (cfg, dir)
parseArgs ("--host":h:rest) cfg dir = parseArgs rest (cfg { serverHost = h }) dir
parseArgs ("--port":p:rest) cfg dir
  | all isDigit p, not (null p)
  , let n = read p :: Int, n > 0 && n <= 65535
  = parseArgs rest (cfg { serverPort = p }) dir
  | otherwise = Left ("Invalid port: " ++ p ++ " (must be 1-65535)")
parseArgs ("--data-dir":d:rest) cfg _ = parseArgs rest cfg d
parseArgs ("--max-connections":n:rest) cfg dir
  | all isDigit n, not (null n)
  = parseArgs rest (cfg { serverMaxConns = read n }) dir
  | otherwise = Left ("Invalid max-connections: " ++ n)
parseArgs ("--timeout":n:rest) cfg dir
  | all isDigit n, not (null n)
  = parseArgs rest (cfg { serverIdleTimeout = read n }) dir
  | otherwise = Left ("Invalid timeout: " ++ n)
parseArgs ("--help":_) _ _ = Left helpText
parseArgs [flag] _ _
  | flag `elem` ["--host", "--port", "--data-dir", "--max-connections", "--timeout"]
  = Left ("Missing value for " ++ flag)
parseArgs (unknown:_) _ _ = Left ("Unknown option: " ++ unknown)

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
  , "  --help                   Show this help"
  ]
