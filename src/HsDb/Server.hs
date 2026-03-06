{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | TCP server that accepts PostgreSQL-protocol connections and executes
-- SQL queries against an hs-db 'Database'.
module HsDb.Server
  ( runServer
  , ServerConfig(..)
  , defaultServerConfig
  ) where

import Control.Concurrent (forkFinally)
import Control.Concurrent.STM
import Control.Exception (SomeException, catch, bracket)
import Control.Monad.Trans.Except (runExceptT, withExceptT)
import Data.IORef (readIORef, writeIORef)
import qualified Data.ByteString as BS
import Data.Text (Text)
import qualified Data.Text as T
import Network.Socket
import System.IO (Handle, IOMode(..), hClose, hSetBuffering, hFlush, BufferMode(..))
import System.Timeout (timeout)

import HsDb.Integration (Database(..), atomicallyE, commitPendingOps)
import HsDb.Logging
import HsDb.SQL.Parser (parseSQL, ParseError(..))
import HsDb.SQL.Execute (executeSQL, QueryResult(..))
import HsDb.SQL.Types (Statement(..))
import HsDb.Transaction
import HsDb.Server.Protocol

-- | Server configuration.
data ServerConfig = ServerConfig
  { serverHost        :: !String
  , serverPort        :: !String
  , serverMaxConns    :: !Int    -- ^ Maximum concurrent connections (0 = unlimited)
  , serverIdleTimeout :: !Int    -- ^ Idle timeout in seconds (0 = no timeout)
  } deriving (Show)

-- | Default server configuration.
defaultServerConfig :: ServerConfig
defaultServerConfig = ServerConfig
  { serverHost        = "127.0.0.1"
  , serverPort        = "5433"
  , serverMaxConns    = 0
  , serverIdleTimeout = 0
  }

-- | Run the server, accepting connections until the action is interrupted.
runServer :: ServerConfig -> Database -> IO ()
runServer config db = do
  logger <- newLogger
  connCount <- newTVarIO (0 :: Int)
  let hints = defaultHints
        { addrFlags = [AI_PASSIVE]
        , addrSocketType = Stream
        }
  addr:_ <- getAddrInfo (Just hints) (Just (serverHost config)) (Just (serverPort config))
  bracket (openListenSocket addr) close $ \sock -> do
    setSocketOption sock ReuseAddr 1
    bind sock (addrAddress addr)
    listen sock 128
    logInfo logger (T.pack ("Listening on " ++ serverHost config ++ ":" ++ serverPort config))
    acceptLoop config sock db logger connCount

openListenSocket :: AddrInfo -> IO Socket
openListenSocket addr = socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)

acceptLoop :: ServerConfig -> Socket -> Database -> Logger -> TVar Int -> IO ()
acceptLoop config sock db logger connCount = do
  (conn, peer) <- accept sock
  let peerStr = T.pack (show peer)
  accepted <- atomically $ do
    n <- readTVar connCount
    let maxC = serverMaxConns config
    if maxC > 0 && n >= maxC
      then return False
      else do
        writeTVar connCount (n + 1)
        return True
  if accepted
    then do
      logInfo logger ("Connection from " <> peerStr)
      _ <- forkFinally (handleConnection config conn db logger) $ \result -> do
        case result of
          Left e  -> logError logger ("Connection " <> peerStr <> " error: " <> T.pack (show e))
          Right _ -> return ()
        atomically $ modifyTVar' connCount (subtract 1)
        close conn
        logInfo logger ("Connection closed: " <> peerStr)
      return ()
    else do
      logWarn logger ("Connection rejected (limit reached): " <> peerStr)
      (do h <- socketToHandle conn ReadWriteMode
          hSetBuffering h NoBuffering
          _ <- readStartupMessage h
          sendErrorResponse h "too many connections"
          hFlush h
          hClose h
       ) `catch` \(_ :: SomeException) -> close conn
  acceptLoop config sock db logger connCount

handleConnection :: ServerConfig -> Socket -> Database -> Logger -> IO ()
handleConnection config conn db logger = do
  h <- socketToHandle conn ReadWriteMode
  hSetBuffering h NoBuffering
  handleStartup config h db logger `catch` \(e :: SomeException) ->
    logError logger ("Startup error: " <> T.pack (show e))

handleStartup :: ServerConfig -> Handle -> Database -> Logger -> IO ()
handleStartup config h db logger = do
  (msg, rs) <- readStartupMessage h
  case msg of
    SSLRequest -> do
      BS.hPut h (BS.singleton 0x4E) -- 'N' = no SSL
      hFlush h
      (msg', rs') <- readStartupMessage h
      case msg' of
        StartupMsg _ -> doStartup config h rs' db logger
        _            -> hClose h
    StartupMsg _ -> doStartup config h rs db logger
    _            -> hClose h

doStartup :: ServerConfig -> Handle -> ReadState -> Database -> Logger -> IO ()
doStartup config h rs db logger = do
  sendAuthOk h
  sendParameterStatus h "server_version" "0.1.0"
  sendParameterStatus h "server_encoding" "UTF8"
  sendParameterStatus h "client_encoding" "UTF8"
  sendParameterStatus h "DateStyle" "ISO, MDY"
  sendBackendKeyData h 0 0
  sendReadyForQuery h
  queryLoop config h rs db logger Nothing

queryLoop :: ServerConfig -> Handle -> ReadState -> Database -> Logger
          -> Maybe TxState -> IO ()
queryLoop config h rs db logger mtx = do
  let idleUs = serverIdleTimeout config * 1000000
  mMsg <- if idleUs > 0
          then timeout idleUs (readFrontendMsg h rs)
          else Just <$> readFrontendMsg h rs
  case mMsg of
    Nothing -> do
      logWarn logger "Idle timeout, closing connection"
      hClose h
    Just TerminateMsg -> hClose h
    Just (QueryMsg sql) -> do
      mtx' <- handleQuery h db sql logger mtx
      queryLoop config h rs db logger mtx'
    Just _ -> do
      sendErrorResponse h "Unsupported message type"
      sendReady h mtx
      queryLoop config h rs db logger mtx

handleQuery :: Handle -> Database -> Text -> Logger -> Maybe TxState
            -> IO (Maybe TxState)
handleQuery h db sql logger mtx
  | T.null (T.strip sql) = do
      sendEmptyQueryResponse h
      sendReady h mtx
      return mtx
  | otherwise =
      case parseSQL (stripSemicolon sql) of
        Left err -> do
          logWarn logger ("Parse error: " <> peMessage err)
          sendErrorResponse h ("Parse error: " <> peMessage err)
          mtx' <- markAbortedIfInTx mtx ("Parse error: " <> peMessage err)
          sendReady h mtx'
          return mtx'
        Right Begin -> handleBegin h logger mtx
        Right Commit -> handleCommit h db logger mtx
        Right Rollback -> handleRollback h logger mtx
        Right stmt -> handleStatement h db stmt logger mtx

handleBegin :: Handle -> Logger -> Maybe TxState -> IO (Maybe TxState)
handleBegin h _logger Nothing = do
  tx <- newTxState
  sendCommandComplete h "BEGIN"
  sendReady h (Just tx)
  return (Just tx)
handleBegin h logger (Just tx) = do
  -- Already in a transaction — warn but keep going (PostgreSQL behavior)
  logWarn logger "BEGIN inside a transaction block"
  sendErrorResponse h "WARNING: there is already a transaction in progress"
  sendReady h (Just tx)
  return (Just tx)

handleCommit :: Handle -> Database -> Logger -> Maybe TxState -> IO (Maybe TxState)
handleCommit h _ logger Nothing = do
  logWarn logger "COMMIT without active transaction"
  sendCommandComplete h "COMMIT"
  sendReady h Nothing
  return Nothing
handleCommit h db logger (Just tx) = do
  status <- readIORef (txStatus tx)
  case status of
    TxAborted reason -> do
      logWarn logger ("COMMIT on aborted transaction: " <> reason)
      sendErrorResponse h "current transaction is aborted, COMMIT ignored; use ROLLBACK"
      sendReadyForQueryTx TxFailed h
      return (Just tx)
    TxActive -> do
      ops <- getPendingOps tx
      if null ops
        then do
          sendCommandComplete h "COMMIT"
          sendReady h Nothing
          return Nothing
        else do
          result <- runExceptT $ withExceptT (T.pack . show) $ atomicallyE $
                      commitPendingOps db ops
          case result of
            Left err -> do
              logError logger ("Commit failed: " <> err)
              sendErrorResponse h ("COMMIT failed: " <> err)
              writeIORef (txStatus tx) (TxAborted err)
              sendReadyForQueryTx TxFailed h
              return (Just tx)
            Right callbacks -> do
              -- Wait for the last callback (WAL ordering guarantees all prior are durable)
              case callbacks of
                [] -> return ()
                _  -> atomically $ takeTMVar (last callbacks)
              sendCommandComplete h "COMMIT"
              sendReady h Nothing
              return Nothing

handleRollback :: Handle -> Logger -> Maybe TxState -> IO (Maybe TxState)
handleRollback h _ Nothing = do
  sendCommandComplete h "ROLLBACK"
  sendReady h Nothing
  return Nothing
handleRollback h _ (Just _) = do
  -- Discard all pending ops — since we used deferred apply, nothing to undo
  sendCommandComplete h "ROLLBACK"
  sendReady h Nothing
  return Nothing

handleStatement :: Handle -> Database -> Statement -> Logger -> Maybe TxState
                -> IO (Maybe TxState)
handleStatement h db stmt logger mtx = do
  -- Check if transaction is aborted
  case mtx of
    Just tx -> do
      status <- readIORef (txStatus tx)
      case status of
        TxAborted _ -> do
          sendErrorResponse h "current transaction is aborted, commands ignored until end of transaction block"
          sendReadyForQueryTx TxFailed h
          return mtx
        TxActive -> execAndRespond h db stmt logger mtx
    Nothing -> execAndRespond h db stmt logger mtx

execAndRespond :: Handle -> Database -> Statement -> Logger -> Maybe TxState
               -> IO (Maybe TxState)
execAndRespond h db stmt logger mtx = do
  result <- runExceptT $ executeSQL db mtx stmt
  case result of
    Left err -> do
      logWarn logger ("Query error: " <> err)
      sendErrorResponse h err
      mtx' <- markAbortedIfInTx mtx err
      sendReady h mtx'
      return mtx'
    Right (CommandComplete tag) -> do
      sendCommandComplete h tag
      sendReady h mtx
      return mtx
    Right (RowResult cols rows) -> do
      sendRowDescription h cols
      mapM_ (sendDataRow h) rows
      sendCommandComplete h ("SELECT " <> T.pack (show (length rows)))
      sendReady h mtx
      return mtx

markAbortedIfInTx :: Maybe TxState -> Text -> IO (Maybe TxState)
markAbortedIfInTx Nothing _ = return Nothing
markAbortedIfInTx (Just tx) reason = do
  writeIORef (txStatus tx) (TxAborted reason)
  return (Just tx)

sendReady :: Handle -> Maybe TxState -> IO ()
sendReady h Nothing    = sendReadyForQuery h
sendReady h (Just tx)  = do
  status <- readIORef (txStatus tx)
  case status of
    TxActive    -> sendReadyForQueryTx TxInBlock h
    TxAborted _ -> sendReadyForQueryTx TxFailed h

stripSemicolon :: Text -> Text
stripSemicolon = T.dropWhileEnd (\c -> c == ';' || c == ' ' || c == '\n' || c == '\r')
