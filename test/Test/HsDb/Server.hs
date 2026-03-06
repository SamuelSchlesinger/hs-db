{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Server (serverTests) where

import Hedgehog
import Control.Concurrent (forkIO, threadDelay, killThread)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (bracket, SomeException, try)
import qualified Data.ByteString as BS
import Data.Bits (shiftR, shiftL, (.|.))
import Data.Word (Word8)
import Network.Socket
import Network.Socket.ByteString (recv, sendAll)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)
import System.Timeout (timeout)

import HsDb
import HsDb.Logging (newLogger)
import HsDb.Server

serverTests :: Group
serverTests = Group "Server"
  [ ("prop_server_startup_and_query", prop_server_startup_and_query)
  , ("prop_server_connection_limit", prop_server_connection_limit)
  , ("prop_server_idle_timeout", prop_server_idle_timeout)
  , ("prop_server_tx_status_indicators", prop_server_tx_status_indicators)
  , ("prop_server_multiple_queries", prop_server_multiple_queries)
  ]

-- Wire protocol helpers

putInt32BE :: Int -> BS.ByteString
putInt32BE n = BS.pack
  [ fromIntegral (n `shiftR` 24)
  , fromIntegral (n `shiftR` 16)
  , fromIntegral (n `shiftR` 8)
  , fromIntegral n
  ]

getInt32BE :: BS.ByteString -> Int
getInt32BE bs
  | BS.length bs < 4 = 0
  | otherwise =
      let b0 = fromIntegral (BS.index bs 0) :: Int
          b1 = fromIntegral (BS.index bs 1) :: Int
          b2 = fromIntegral (BS.index bs 2) :: Int
          b3 = fromIntegral (BS.index bs 3) :: Int
      in (b0 `shiftL` 24) .|. (b1 `shiftL` 16) .|. (b2 `shiftL` 8) .|. b3

-- Build a PostgreSQL v3 startup message
buildStartupMessage :: BS.ByteString
buildStartupMessage =
  let version = putInt32BE 196608  -- 3.0
      params = "user" <> BS.singleton 0 <> "test" <> BS.singleton 0 <> BS.singleton 0
      payload = version <> params
      len = putInt32BE (4 + BS.length payload)
  in len <> payload

-- Build a simple query message ('Q')
buildQueryMessage :: BS.ByteString -> BS.ByteString
buildQueryMessage sql =
  let payload = sql <> BS.singleton 0
      len = putInt32BE (4 + BS.length payload)
  in BS.singleton 0x51 <> len <> payload  -- 'Q'

-- Build a terminate message ('X')
buildTerminate :: BS.ByteString
buildTerminate = BS.singleton 0x58 <> putInt32BE 4  -- 'X' + length 4

-- Read all available bytes from the socket (with timeout)
recvAll :: Socket -> IO BS.ByteString
recvAll sock = go BS.empty
  where
    go acc = do
      mChunk <- timeout 500000 (recv sock 4096)
      case mChunk of
        Nothing    -> return acc
        Just chunk
          | BS.null chunk -> return acc
          | otherwise     -> go (acc <> chunk)

-- Connect and do startup handshake, return the socket ready for queries
connectAndStartup :: String -> IO Socket
connectAndStartup port = do
  let hints = defaultHints { addrSocketType = Stream }
  addr:_ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just port)
  sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
  connect sock (addrAddress addr)
  sendAll sock buildStartupMessage
  -- Read startup response (AuthOk + params + BackendKeyData + ReadyForQuery)
  _ <- recvAll sock
  return sock

-- Find the ReadyForQuery status byte in a response.
-- ReadyForQuery = 'Z' (0x5A) followed by int32 length (5) and status byte
findReadyForQueryStatus :: BS.ByteString -> Maybe Word8
findReadyForQueryStatus bs = go 0
  where
    go i
      | i >= BS.length bs = Nothing
      | BS.index bs i == 0x5A =  -- 'Z'
          if i + 5 < BS.length bs
            then let len = getInt32BE (BS.drop (i+1) bs)
                 in if len == 5 then Just (BS.index bs (i + 5)) else go (i+1)
            else go (i+1)
      | otherwise = go (i+1)

-- Start server in background, run action, then kill server
withTestServer :: ServerConfig -> (String -> IO a) -> IO a
withTestServer config action =
  withSystemTempDirectory "hs-db-server-test" $ \dir -> do
    let dbConfig = defaultDatabaseConfig (dir </> "test.wal")
    bracket (openDatabase dbConfig) closeDatabase $ \db -> do
      ready <- newEmptyMVar
      let cfg = config { serverPort = "0" }  -- We need a free port
      -- Find a free port
      let hints = defaultHints { addrFlags = [AI_PASSIVE], addrSocketType = Stream }
      addr:_ <- getAddrInfo (Just hints) (Just (serverHost cfg)) Nothing
      freePort <- bracket
        (socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr))
        close
        (\s -> do
          setSocketOption s ReuseAddr 1
          bind s (addrAddress addr)
          p <- socketPort s
          return (show p))
      let cfg' = config { serverPort = freePort }
      logger <- newLogger
      tid <- forkIO $ do
        putMVar ready ()
        runServer cfg' logger db
      takeMVar ready
      threadDelay 100000  -- Give server time to start listening
      result <- action freePort
      killThread tid
      return result

-- Test: basic connection and query
prop_server_startup_and_query :: Property
prop_server_startup_and_query = withTests 1 $ property $ do
  result <- evalIO $ withTestServer defaultServerConfig $ \port -> do
    sock <- connectAndStartup port
    -- Send a CREATE TABLE + query
    sendAll sock (buildQueryMessage "CREATE TABLE t (x INT NOT NULL)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "INSERT INTO t (x) VALUES (42)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "SELECT * FROM t")
    resp <- recvAll sock
    sendAll sock buildTerminate
    close sock
    return resp
  -- Response should contain data ('D' = 0x44) and ReadyForQuery ('Z' = 0x5A)
  let hasDataRow = BS.elem 0x44 result
  let hasReady   = BS.elem 0x5A result
  assert hasDataRow
  assert hasReady

-- Test: connection limit enforcement
prop_server_connection_limit :: Property
prop_server_connection_limit = withTests 1 $ property $ do
  result <- evalIO $ withTestServer (defaultServerConfig { serverMaxConns = 2 }) $ \port -> do
    -- Open 2 connections (at the limit)
    sock1 <- connectAndStartup port
    sock2 <- connectAndStartup port
    -- Try a 3rd connection — server should reject it
    let hints = defaultHints { addrSocketType = Stream }
    addr:_ <- getAddrInfo (Just hints) (Just "127.0.0.1") (Just port)
    sock3 <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock3 (addrAddress addr)
    sendAll sock3 buildStartupMessage
    resp <- recvAll sock3
    -- Clean up
    sendAll sock1 buildTerminate
    sendAll sock2 buildTerminate
    close sock1
    close sock2
    close sock3
    return resp
  -- The rejected connection should get an ErrorResponse ('E' = 0x45)
  assert (BS.elem 0x45 result)

-- Test: idle timeout closes connection
prop_server_idle_timeout :: Property
prop_server_idle_timeout = withTests 1 $ property $ do
  result <- evalIO $ withTestServer (defaultServerConfig { serverIdleTimeout = 1 }) $ \port -> do
    sock <- connectAndStartup port
    -- Wait longer than the 1-second timeout
    threadDelay 2000000
    -- Try to send a query — should fail because server closed the connection
    r <- try $ do
      sendAll sock (buildQueryMessage "SELECT 1")
      recv sock 4096
    close sock
    case r of
      Left (_ :: SomeException) -> return True    -- Connection was closed
      Right bs | BS.null bs     -> return True    -- EOF = closed
      Right _                   -> return False   -- Still alive (shouldn't happen)
  assert result

-- Test: transaction status indicators (I/T/E)
prop_server_tx_status_indicators :: Property
prop_server_tx_status_indicators = withTests 1 $ property $ do
  (idle, inTx, failed) <- evalIO $ withTestServer defaultServerConfig $ \port -> do
    sock <- connectAndStartup port
    -- Send a simple query outside transaction — should get 'I' (idle)
    sendAll sock (buildQueryMessage "CREATE TABLE t (x INT NOT NULL)")
    respIdle <- recvAll sock
    let idleStatus = findReadyForQueryStatus respIdle

    -- BEGIN — should get 'T' (in transaction)
    sendAll sock (buildQueryMessage "BEGIN")
    respBegin <- recvAll sock
    let txStatus = findReadyForQueryStatus respBegin

    -- Cause an error inside the transaction — should get 'E' (failed)
    sendAll sock (buildQueryMessage "INSERT INTO nonexistent (x) VALUES (1)")
    respErr <- recvAll sock
    let errStatus = findReadyForQueryStatus respErr

    sendAll sock (buildQueryMessage "ROLLBACK")
    _ <- recvAll sock
    sendAll sock buildTerminate
    close sock
    return (idleStatus, txStatus, errStatus)
  idle   === Just 0x49  -- 'I'
  inTx   === Just 0x54  -- 'T'
  failed === Just 0x45  -- 'E'

-- Test: multiple queries on the same connection
prop_server_multiple_queries :: Property
prop_server_multiple_queries = withTests 1 $ property $ do
  rowCount <- evalIO $ withTestServer defaultServerConfig $ \port -> do
    sock <- connectAndStartup port
    sendAll sock (buildQueryMessage "CREATE TABLE t (x INT NOT NULL)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "INSERT INTO t (x) VALUES (1)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "INSERT INTO t (x) VALUES (2)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "INSERT INTO t (x) VALUES (3)")
    _ <- recvAll sock
    sendAll sock (buildQueryMessage "SELECT * FROM t")
    resp <- recvAll sock
    sendAll sock buildTerminate
    close sock
    -- Count DataRow messages ('D' = 0x44)
    return (BS.count 0x44 resp)
  -- Should have at least 3 DataRow messages (one per row)
  assert (rowCount >= 3)
