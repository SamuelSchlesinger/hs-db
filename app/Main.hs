module Main (main) where

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
parseArgs ("--port":p:rest) cfg dir = parseArgs rest (cfg { serverPort = p }) dir
parseArgs ("--data-dir":d:rest) cfg _ = parseArgs rest cfg d
parseArgs ("--help":_) _ _ = Left helpText
parseArgs (unknown:_) _ _ = Left ("Unknown option: " ++ unknown)

helpText :: String
helpText = unlines
  [ "Usage: hs-db-server [OPTIONS]"
  , ""
  , "Options:"
  , "  --host HOST       Listen address (default: 127.0.0.1)"
  , "  --port PORT       Listen port (default: 5433)"
  , "  --data-dir DIR    Data directory (default: ./hs-db-data)"
  , "  --help            Show this help"
  ]
