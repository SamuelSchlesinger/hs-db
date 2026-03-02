{-# OPTIONS_GHC -Wno-orphans #-}

module HsDb.WAL.Serialize
  ( encodeFramed
  , decodeFramed
  , writeWALHeader
  , readWALHeader
  ) where

import Data.Binary (Binary(..), Get, Put)
import qualified Data.Binary as Binary
import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Digest.CRC32 (crc32)
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text.Encoding as TE
import Data.Time (UTCTime(..))
import Data.Time.Calendar (toModifiedJulianDay, Day(..))
import Data.Time.Clock (diffTimeToPicoseconds, picosecondsToDiffTime)
import Data.Vector (Vector)
import qualified Data.Vector as V
import Data.Word (Word32)

import HsDb.Types (Value(..), ColumnType(..), Column(..))
import HsDb.WAL.Types

-- Binary instances --

instance Binary Value where
  put (VInt32 x)   = Put.putWord8 0 >> Binary.put x
  put (VInt64 x)   = Put.putWord8 1 >> Binary.put x
  put (VFloat64 x) = Put.putWord8 2 >> Binary.put x
  put (VText t)    = Put.putWord8 3 >> putText t
  put (VBool b)    = Put.putWord8 4 >> Put.putWord8 (if b then 1 else 0)
  put (VBytea bs)  = Put.putWord8 5 >> putByteString bs
  put VNull        = Put.putWord8 6

  get = do
    tag <- Get.getWord8
    case tag of
      0 -> VInt32 <$> Binary.get
      1 -> VInt64 <$> Binary.get
      2 -> VFloat64 <$> Binary.get
      3 -> VText <$> getText
      4 -> do b <- Get.getWord8; return (VBool (b /= 0))
      5 -> VBytea <$> getByteString
      6 -> return VNull
      _ -> fail ("Unknown Value tag: " ++ show tag)

instance Binary ColumnType where
  put TInt32   = Put.putWord8 0
  put TInt64   = Put.putWord8 1
  put TFloat64 = Put.putWord8 2
  put TText    = Put.putWord8 3
  put TBool    = Put.putWord8 4
  put TBytea   = Put.putWord8 5

  get = do
    tag <- Get.getWord8
    case tag of
      0 -> return TInt32
      1 -> return TInt64
      2 -> return TFloat64
      3 -> return TText
      4 -> return TBool
      5 -> return TBytea
      _ -> fail ("Unknown ColumnType tag: " ++ show tag)

instance Binary Column where
  put (Column name typ nullable) = do
    putText name
    Binary.put typ
    Put.putWord8 (if nullable then 1 else 0)

  get = do
    name <- getText
    typ <- Binary.get
    nullable <- Get.getWord8
    return (Column name typ (nullable /= 0))

instance Binary WALCommand where
  put (CmdCreateTable name schema) = do
    Put.putWord8 0
    putText name
    putListOf schema
  put (CmdInsertRow name rid row) = do
    Put.putWord8 1
    putText name
    Put.putWord64be (fromIntegral rid)
    putVector row
  put (CmdUpdateRow name rid row) = do
    Put.putWord8 2
    putText name
    Put.putWord64be (fromIntegral rid)
    putVector row
  put (CmdDeleteRow name rid) = do
    Put.putWord8 3
    putText name
    Put.putWord64be (fromIntegral rid)
  put (CmdDropTable name) = do
    Put.putWord8 4
    putText name

  get = do
    tag <- Get.getWord8
    case tag of
      0 -> CmdCreateTable <$> getText <*> getListOf
      1 -> CmdInsertRow <$> getText <*> (fromIntegral <$> Get.getWord64be) <*> getVector
      2 -> CmdUpdateRow <$> getText <*> (fromIntegral <$> Get.getWord64be) <*> getVector
      3 -> CmdDeleteRow <$> getText <*> (fromIntegral <$> Get.getWord64be)
      4 -> CmdDropTable <$> getText
      _ -> fail ("Unknown WALCommand tag: " ++ show tag)

instance Binary WALEntry where
  put (WALEntry seqn ts cmd) = do
    Put.putWord64be seqn
    putUTCTime ts
    Binary.put cmd

  get = WALEntry
    <$> Get.getWord64be
    <*> getUTCTime
    <*> Binary.get

-- Helpers for Text, ByteString, Vector, lists --

putText :: Text -> Put
putText t = let bs = TE.encodeUtf8 t
            in Put.putWord32be (fromIntegral (BS.length bs)) >> Put.putByteString bs

-- | Maximum text field size in bytes (16 MiB).
maxTextLen :: Word32
maxTextLen = 16 * 1024 * 1024

getText :: Get Text
getText = do
  len <- Get.getWord32be
  if len > maxTextLen
    then fail ("Text length " ++ show len ++ " exceeds maximum " ++ show maxTextLen)
    else do
      bs <- Get.getByteString (fromIntegral len)
      case TE.decodeUtf8' bs of
        Left err -> fail ("Invalid UTF-8 in Text: " ++ show err)
        Right t  -> return t

putByteString :: ByteString -> Put
putByteString bs = Put.putWord32be (fromIntegral (BS.length bs)) >> Put.putByteString bs

-- | Maximum byte string field size in bytes (64 MiB).
maxByteStringLen :: Word32
maxByteStringLen = 64 * 1024 * 1024

getByteString :: Get ByteString
getByteString = do
  len <- Get.getWord32be
  if len > maxByteStringLen
    then fail ("ByteString length " ++ show len ++ " exceeds maximum " ++ show maxByteStringLen)
    else Get.getByteString (fromIntegral len)

putVector :: Binary a => Vector a -> Put
putVector v = do
  Put.putWord32be (fromIntegral (V.length v))
  V.mapM_ Binary.put v

-- | Maximum number of elements allowed in a deserialized vector.
-- Prevents OOM from malicious or corrupted WAL files.
maxVectorLen :: Word32
maxVectorLen = 1000000

getVector :: Binary a => Get (Vector a)
getVector = do
  len <- Get.getWord32be
  if len > maxVectorLen
    then fail ("Vector length " ++ show len ++ " exceeds maximum " ++ show maxVectorLen)
    else V.replicateM (fromIntegral len) Binary.get

putListOf :: Binary a => [a] -> Put
putListOf xs = do
  Put.putWord32be (fromIntegral (length xs))
  mapM_ Binary.put xs

-- | Maximum number of elements allowed in a deserialized list.
-- Used for schema column lists — no table should have millions of columns.
maxListLen :: Word32
maxListLen = 1000

getListOf :: Binary a => Get [a]
getListOf = do
  len <- Get.getWord32be
  if len > maxListLen
    then fail ("List length " ++ show len ++ " exceeds maximum " ++ show maxListLen)
    else sequence (replicate (fromIntegral len) Binary.get)

-- UTCTime encoding: modified Julian day (Int64) + picoseconds (Int64) --

putUTCTime :: UTCTime -> Put
putUTCTime (UTCTime day dt) = do
  Binary.put (fromIntegral (toModifiedJulianDay day) :: Int64)
  Binary.put (fromIntegral (diffTimeToPicoseconds dt) :: Int64)

getUTCTime :: Get UTCTime
getUTCTime = do
  day <- Binary.get :: Get Int64
  pico <- Binary.get :: Get Int64
  return (UTCTime (ModifiedJulianDay (fromIntegral day))
                  (picosecondsToDiffTime (fromIntegral pico)))

-- CRC32 framed encoding/decoding --

-- | Encode a WALEntry with length prefix and CRC32 checksum.
-- Format: [8-byte length][payload bytes][4-byte CRC32]
-- CRC32 covers the length prefix + payload.
encodeFramed :: WALEntry -> ByteString
encodeFramed entry =
  let payload = LBS.toStrict (Binary.encode entry)
      len = BS.length payload
      lenBs = LBS.toStrict (Put.runPut (Put.putWord64be (fromIntegral len)))
      toCheck = lenBs <> payload
      checksum = crc32 toCheck
      crcBs = LBS.toStrict (Put.runPut (Put.putWord32be (fromIntegral checksum)))
  in toCheck <> crcBs

-- | Decode a framed WAL entry. Returns the entry and remaining bytes,
-- or an error message.
decodeFramed :: ByteString -> Either String (WALEntry, ByteString)
decodeFramed bs
  | BS.length bs < 8 = Left "Not enough data for length prefix"
  | otherwise = do
      let (lenBs, rest1) = BS.splitAt 8 bs
      len <- case Get.runGetOrFail Get.getWord64be (LBS.fromStrict lenBs) of
               Left (_, _, err) -> Left ("Invalid length prefix: " ++ err)
               Right (_, _, n)  -> Right (fromIntegral n :: Int)
      if BS.length rest1 < len + 4
        then Left "Not enough data for payload + CRC32"
        else do
          let (payload, rest2) = BS.splitAt len rest1
              (crcBs, rest3) = BS.splitAt 4 rest2
              toCheck = lenBs <> payload
              expectedCrc = crc32 toCheck
          actualCrc <- case Get.runGetOrFail Get.getWord32be (LBS.fromStrict crcBs) of
                         Left (_, _, err) -> Left ("Invalid CRC bytes: " ++ err)
                         Right (_, _, n)  -> Right (fromIntegral n :: Word32)
          if fromIntegral expectedCrc /= actualCrc
            then Left "CRC32 mismatch"
            else case Get.runGetOrFail Binary.get (LBS.fromStrict payload) of
                   Left (_, _, err) -> Left ("Decode error: " ++ err)
                   Right (_, _, entry) -> Right (entry, rest3)

-- WAL header read/write --

-- | Write the WAL header (6 bytes: 4 magic + 2 version).
writeWALHeader :: WALHeader -> ByteString
writeWALHeader (WALHeader magic ver) =
  magic <> LBS.toStrict (Put.runPut (Put.putWord16be (fromIntegral ver)))

-- | Read the WAL header from a ByteString. Returns the header and remaining bytes.
readWALHeader :: ByteString -> Either String (WALHeader, ByteString)
readWALHeader bs
  | BS.length bs < 6 = Left "Not enough data for WAL header"
  | otherwise = do
      let (magic, rest1) = BS.splitAt 4 bs
          (verBs, rest2) = BS.splitAt 2 rest1
      ver <- case Get.runGetOrFail Get.getWord16be (LBS.fromStrict verBs) of
               Left (_, _, err) -> Left ("Invalid version bytes: " ++ err)
               Right (_, _, n)  -> Right (fromIntegral n :: Int)
      if magic /= walMagic
        then Left ("Invalid magic bytes: expected HSDB, got " ++ show magic)
        else Right (WALHeader magic ver, rest2)
