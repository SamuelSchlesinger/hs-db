{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

module HsDb.Server
  ( runServer
  , ServerConfig(..)
  , defaultServerConfig
  ) where

import Control.Concurrent (forkFinally)
import Control.Exception (SomeException, catch, bracket)
import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text as T
import Network.Socket
import System.IO (Handle, IOMode(..), hClose, hSetBuffering, hFlush, BufferMode(..))

import HsDb.Integration (Database)
import HsDb.SQL.Parser (parseSQL, ParseError(..))
import HsDb.SQL.Execute (executeSQL, QueryResult(..))
import HsDb.Server.Protocol

-- | Server configuration.
data ServerConfig = ServerConfig
  { serverHost :: !String
  , serverPort :: !String
  } deriving (Show)

-- | Default server configuration.
defaultServerConfig :: ServerConfig
defaultServerConfig = ServerConfig
  { serverHost = "127.0.0.1"
  , serverPort = "5433"
  }

-- | Run the server, accepting connections until the action is interrupted.
runServer :: ServerConfig -> Database -> IO ()
runServer config db = do
  let hints = defaultHints
        { addrFlags = [AI_PASSIVE]
        , addrSocketType = Stream
        }
  addr:_ <- getAddrInfo (Just hints) (Just (serverHost config)) (Just (serverPort config))
  bracket (openListenSocket addr) close $ \sock -> do
    setSocketOption sock ReuseAddr 1
    bind sock (addrAddress addr)
    listen sock 128
    putStrLn ("hs-db server listening on " ++ serverHost config ++ ":" ++ serverPort config)
    acceptLoop sock db

openListenSocket :: AddrInfo -> IO Socket
openListenSocket addr = socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)

acceptLoop :: Socket -> Database -> IO ()
acceptLoop sock db = do
  (conn, peer) <- accept sock
  putStrLn ("Connection from " ++ show peer)
  _ <- forkFinally (handleConnection conn db) (\_ -> close conn)
  acceptLoop sock db

handleConnection :: Socket -> Database -> IO ()
handleConnection conn db = do
  h <- socketToHandle conn ReadWriteMode
  hSetBuffering h NoBuffering
  handleStartup h db `catch` \(e :: SomeException) ->
    putStrLn ("Connection error: " ++ show e)

handleStartup :: Handle -> Database -> IO ()
handleStartup h db = do
  (msg, rs) <- readStartupMessage h
  case msg of
    SSLRequest -> do
      BS.hPut h (BS.singleton 0x4E) -- 'N' = no SSL
      hFlush h
      (msg', rs') <- readStartupMessage h
      case msg' of
        StartupMsg _ -> doStartup h rs' db
        _            -> hClose h
    StartupMsg _ -> doStartup h rs db
    _            -> hClose h

doStartup :: Handle -> ReadState -> Database -> IO ()
doStartup h rs db = do
  sendAuthOk h
  sendParameterStatus h "server_version" "0.1.0"
  sendParameterStatus h "server_encoding" "UTF8"
  sendParameterStatus h "client_encoding" "UTF8"
  sendParameterStatus h "DateStyle" "ISO, MDY"
  sendBackendKeyData h 0 0
  sendReadyForQuery h
  queryLoop h rs db

queryLoop :: Handle -> ReadState -> Database -> IO ()
queryLoop h rs db = do
  msg <- readFrontendMsg h rs
  case msg of
    TerminateMsg -> hClose h
    QueryMsg sql -> do
      handleQuery h db sql
      queryLoop h rs db
    _ -> do
      sendErrorResponse h "Unsupported message type"
      sendReadyForQuery h
      queryLoop h rs db

handleQuery :: Handle -> Database -> Text -> IO ()
handleQuery h db sql
  | T.null (T.strip sql) = do
      sendEmptyQueryResponse h
      sendReadyForQuery h
  | otherwise =
      case parseSQL (stripSemicolon sql) of
        Left err -> do
          sendErrorResponse h ("Parse error: " <> peMessage err)
          sendReadyForQuery h
        Right stmt -> do
          result <- executeSQL db stmt
          case result of
            Left err -> do
              sendErrorResponse h err
              sendReadyForQuery h
            Right (CommandComplete tag) -> do
              sendCommandComplete h tag
              sendReadyForQuery h
            Right (RowResult cols rows) -> do
              sendRowDescription h cols
              mapM_ (sendDataRow h) rows
              sendCommandComplete h ("SELECT " <> T.pack (show (length rows)))
              sendReadyForQuery h

stripSemicolon :: Text -> Text
stripSemicolon = T.dropWhileEnd (\c -> c == ';' || c == ' ' || c == '\n' || c == '\r')
