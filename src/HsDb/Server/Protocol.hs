{-# LANGUAGE OverloadedStrings #-}

-- | PostgreSQL v3 wire protocol implementation. Handles startup negotiation,
-- frontend message parsing, and backend message encoding.
module HsDb.Server.Protocol
  ( -- * Frontend (client → server) messages
    FrontendMsg(..)
  , readStartupMessage
  , readFrontendMsg
    -- * Backend (server → client) messages
  , sendAuthOk
  , sendParameterStatus
  , sendBackendKeyData
  , sendReadyForQuery
  , sendRowDescription
  , sendDataRow
  , sendCommandComplete
  , sendEmptyQueryResponse
  , sendErrorResponse
    -- * Transaction indicator
  , TxIndicator(..)
  , sendReadyForQueryTx
    -- * Read state
  , ReadState
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int32)
import qualified Data.ByteString as BS
import Data.ByteString.Builder (Builder, word8, int32BE, int16BE,
                                 byteString, toLazyByteString)
import qualified Data.ByteString.Lazy as LBS
import Data.IORef (IORef, readIORef, writeIORef, newIORef)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Word (Word8, Word32)
import Data.Bits (shiftR, shiftL, (.|.))
import System.IO (Handle, hFlush)

import HsDb.SQL.Execute (ColumnInfo(..))
import HsDb.Types (ColumnType(..))

-- | Messages from the client.
data FrontendMsg
  = StartupMsg [(ByteString, ByteString)]
    -- ^ Startup parameters (user, database, etc.)
  | QueryMsg !Text
    -- ^ Simple query
  | TerminateMsg
    -- ^ Client wants to disconnect
  | SSLRequest
    -- ^ Client is requesting SSL
  deriving (Show)

-- Reading bytes from a handle with buffering

-- | Buffered read state for a client connection, carrying unconsumed bytes
-- between message reads.
type ReadState = IORef ByteString

newReadState :: IO ReadState
newReadState = newIORef BS.empty

readExact :: Handle -> ReadState -> Int -> IO ByteString
readExact h ref n = do
  buf <- readIORef ref
  go buf n
  where
    go acc remaining
      | BS.length acc >= remaining = do
          let (result, leftover) = BS.splitAt remaining acc
          writeIORef ref leftover
          return result
      | otherwise = do
          chunk <- BS.hGetSome h (max 4096 remaining)
          if BS.null chunk
            then fail "Connection closed"
            else go (acc <> chunk) remaining

-- Parse a 32-bit big-endian integer from bytes.
-- Caller must ensure ByteString has at least 4 bytes (readExact guarantees this).
getInt32 :: ByteString -> Int
getInt32 bs
  | BS.length bs < 4 = error "getInt32: ByteString too short (need 4 bytes)"
  | otherwise =
      let b0 = fromIntegral (BS.index bs 0) :: Int
          b1 = fromIntegral (BS.index bs 1) :: Int
          b2 = fromIntegral (BS.index bs 2) :: Int
          b3 = fromIntegral (BS.index bs 3) :: Int
      in (b0 `shiftL` 24) .|. (b1 `shiftL` 16) .|. (b2 `shiftL` 8) .|. b3

-- | Read the initial startup message (no type byte, just length + payload).
-- Returns the parsed message and a ReadState for subsequent reads.
readStartupMessage :: Handle -> IO (FrontendMsg, ReadState)
readStartupMessage h = do
  rs <- newReadState
  lenBytes <- readExact h rs 4
  let len = getInt32 lenBytes
  payload <- readExact h rs (len - 4)
  let msg = parseStartup payload
  return (msg, rs)

parseStartup :: ByteString -> FrontendMsg
parseStartup payload
  -- SSL request: version = 80877103 (0x04d2162f)
  | BS.length payload >= 4
  , getInt32 (BS.take 4 payload) == 80877103 = SSLRequest
  -- Normal startup: version 3.0 = 196608 (0x00030000)
  | BS.length payload >= 4
  , getInt32 (BS.take 4 payload) == 196608 =
      let params = parseParams (BS.drop 4 payload)
      in StartupMsg params
  | otherwise = StartupMsg [] -- Unknown version, treat as empty

parseParams :: ByteString -> [(ByteString, ByteString)]
parseParams bs
  | BS.null bs = []
  | BS.head bs == 0 = [] -- Null terminator
  | otherwise =
      let (key, rest1) = BS.break (== 0) bs
          rest2 = if BS.null rest1 then rest1 else BS.drop 1 rest1
          (val, rest3) = BS.break (== 0) rest2
          rest4 = if BS.null rest3 then rest3 else BS.drop 1 rest3
      in (key, val) : parseParams rest4

-- | Read a frontend message (has 1-byte type prefix + 4-byte length).
readFrontendMsg :: Handle -> ReadState -> IO FrontendMsg
readFrontendMsg h rs = do
  typeBytes <- readExact h rs 1
  let msgType = BS.head typeBytes
  lenBytes <- readExact h rs 4
  let len = getInt32 lenBytes
  payload <- if len > 4 then readExact h rs (len - 4) else return BS.empty
  return (parseFrontend msgType payload)

parseFrontend :: Word8 -> ByteString -> FrontendMsg
parseFrontend 0x51 payload = -- 'Q'
  -- Query: null-terminated string
  let query = BS.takeWhile (/= 0) payload
  in QueryMsg (TE.decodeUtf8 query)
parseFrontend 0x58 _ = TerminateMsg -- 'X'
parseFrontend _ _    = TerminateMsg -- Unknown, treat as terminate

-- Sending backend messages

sendMsg :: Handle -> Word8 -> Builder -> IO ()
sendMsg h msgType payload = do
  let payloadBytes = LBS.toStrict (toLazyByteString payload)
      len = BS.length payloadBytes + 4 -- length includes itself
  BS.hPut h (BS.singleton msgType)
  BS.hPut h (putInt32 (fromIntegral len))
  BS.hPut h payloadBytes

putInt32 :: Word32 -> ByteString
putInt32 n = BS.pack
  [ fromIntegral (n `shiftR` 24)
  , fromIntegral (n `shiftR` 16)
  , fromIntegral (n `shiftR` 8)
  , fromIntegral n
  ]

-- | AuthenticationOk (R, int32 0)
sendAuthOk :: Handle -> IO ()
sendAuthOk h = sendMsg h 0x52 (int32BE 0) -- 'R', auth type 0 = ok

-- | ParameterStatus (S, name\0, value\0)
sendParameterStatus :: Handle -> ByteString -> ByteString -> IO ()
sendParameterStatus h name val =
  sendMsg h 0x53 (byteString name <> word8 0 <> byteString val <> word8 0)

-- | BackendKeyData (K, pid, secret)
sendBackendKeyData :: Handle -> Int -> Int -> IO ()
sendBackendKeyData h pid secret =
  sendMsg h 0x4B (int32BE (fromIntegral pid) <> int32BE (fromIntegral secret))

-- | Transaction status byte for ReadyForQuery.
data TxIndicator = TxIdle | TxInBlock | TxFailed
  deriving (Show, Eq)

-- | ReadyForQuery (Z, status: @I@=idle, @T@=in transaction, @E@=failed transaction)
sendReadyForQuery :: Handle -> IO ()
sendReadyForQuery = sendReadyForQueryTx TxIdle

sendReadyForQueryTx :: TxIndicator -> Handle -> IO ()
sendReadyForQueryTx indicator h = do
  let byte = case indicator of
        TxIdle    -> 0x49 :: Word8  -- 'I'
        TxInBlock -> 0x54           -- 'T'
        TxFailed  -> 0x45           -- 'E'
  sendMsg h 0x5A (word8 byte)
  hFlush h

-- | RowDescription (T, field count, field descriptions)
sendRowDescription :: Handle -> [ColumnInfo] -> IO ()
sendRowDescription h cols = do
  let fieldCount = length cols
      fields = mconcat (map describeField cols)
  sendMsg h 0x54 (int16BE (fromIntegral fieldCount) <> fields)

describeField :: ColumnInfo -> Builder
describeField ci =
  byteString (TE.encodeUtf8 (ciName ci)) <> word8 0  -- name
  <> int32BE 0        -- table OID
  <> int16BE 0        -- column attr number
  <> int32BE (typeOid (ciType ci))  -- type OID
  <> int16BE (fromIntegral (typeLen (ciType ci)))  -- type length
  <> int32BE (-1)     -- type modifier
  <> int16BE 0        -- format code (0 = text)

typeOid :: ColumnType -> Int32
typeOid TInt32   = 23    -- int4
typeOid TInt64   = 20    -- int8
typeOid TFloat64 = 701   -- float8
typeOid TText    = 25    -- text
typeOid TBool    = 16    -- bool
typeOid TBytea   = 17    -- bytea

typeLen :: ColumnType -> Int
typeLen TInt32   = 4
typeLen TInt64   = 8
typeLen TFloat64 = 8
typeLen TText    = -1
typeLen TBool    = 1
typeLen TBytea   = -1

-- | DataRow (D, field count, field values as length-prefixed text)
sendDataRow :: Handle -> [Maybe Text] -> IO ()
sendDataRow h vals = do
  let fieldCount = length vals
      fields = mconcat (map encodeField vals)
  sendMsg h 0x44 (int16BE (fromIntegral fieldCount) <> fields)

encodeField :: Maybe Text -> Builder
encodeField Nothing = int32BE (-1)  -- NULL
encodeField (Just t) =
  let bs = TE.encodeUtf8 t
  in int32BE (fromIntegral (BS.length bs)) <> byteString bs

-- | CommandComplete (C, tag string)
sendCommandComplete :: Handle -> Text -> IO ()
sendCommandComplete h tag =
  sendMsg h 0x43 (byteString (TE.encodeUtf8 tag) <> word8 0)

-- | EmptyQueryResponse (I)
sendEmptyQueryResponse :: Handle -> IO ()
sendEmptyQueryResponse h = sendMsg h 0x49 mempty

-- | ErrorResponse (E, field type + value pairs, null terminator)
sendErrorResponse :: Handle -> Text -> IO ()
sendErrorResponse h msg =
  let payload = word8 0x53 <> byteString "ERROR" <> word8 0     -- Severity
             <> word8 0x56 <> byteString "ERROR" <> word8 0     -- Severity (V)
             <> word8 0x43 <> byteString "42000" <> word8 0     -- SQLSTATE
             <> word8 0x4D <> byteString (TE.encodeUtf8 msg) <> word8 0  -- Message
             <> word8 0  -- Terminator
  in sendMsg h 0x45 payload
