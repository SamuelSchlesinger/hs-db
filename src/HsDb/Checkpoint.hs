{-# LANGUAGE OverloadedStrings #-}

-- | Checkpoint support: serializes the in-memory catalog to a file so that
-- WAL replay can skip entries up to the checkpoint sequence number.
-- On recovery, load the checkpoint and replay only newer WAL entries.
module HsDb.Checkpoint
  ( writeCheckpoint
  , readCheckpoint
  , CheckpointHeader(..)
  , checkpointMagic
  , checkpointVersion
  ) where

import Control.Concurrent.STM (atomically, readTVar)
import Control.Monad (when)
import qualified Data.Binary as Binary
import qualified Data.Binary.Get as Get
import qualified Data.Binary.Put as Put
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import Data.Digest.CRC32 (crc32)
import qualified Data.IntMap.Strict as IntMap
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Vector as V
import qualified Data.Text as T
import Data.Word (Word32, Word64)
import System.Directory (renameFile, doesFileExist)
import System.Posix.IO (openFd, closeFd, defaultFileFlags, OpenMode(..))
import System.Posix.Types (Fd(..))
import Foreign.C.Types (CInt(..))

import HsDb.Types
import HsDb.Table (Table(..), TableCatalog)
import HsDb.WAL.Serialize ()  -- Binary instances

-- POSIX fsync via FFI (same as WAL.Writer)
foreign import ccall "fsync" c_fsync :: CInt -> IO CInt

fsyncFd :: Fd -> IO ()
fsyncFd (Fd fd) = do
  result <- c_fsync fd
  when (result /= 0) $ fail "fsync failed during checkpoint"

-- | Checkpoint file header.
data CheckpointHeader = CheckpointHeader
  { cpMagic   :: !BS.ByteString  -- ^ 4 bytes: "HSCP"
  , cpVersion :: !Int            -- ^ 2 bytes
  , cpWALSeq  :: !Word64         -- ^ 8 bytes: WAL sequence at checkpoint time
  } deriving (Show, Eq)

checkpointMagic :: BS.ByteString
checkpointMagic = "HSCP"

checkpointVersion :: Int
checkpointVersion = 1

-- | Write a checkpoint file atomically (write to .tmp, fsync, rename).
-- The catalog is read in a single STM transaction for consistency.
writeCheckpoint :: FilePath -> TableCatalog -> Word64 -> IO ()
writeCheckpoint path catalog walSeq = do
  -- Read entire catalog in one atomic STM transaction
  tableData <- atomically $ do
    tables <- readTVar catalog
    mapM (\(name, table) -> do
      rows <- readTVar (tableRows table)
      counter <- readTVar (tableCounter table)
      return (name, tableSchema table, counter, IntMap.toAscList rows)
      ) (Map.toAscList tables)
  -- Serialize
  let headerBs = encodeHeader (CheckpointHeader checkpointMagic checkpointVersion walSeq)
      bodyBs = encodeBody tableData
      payload = headerBs <> bodyBs
      checksum = crc32 payload
      crcBs = LBS.toStrict (Put.runPut (Put.putWord32be (fromIntegral checksum)))
      fullData = payload <> crcBs
  -- Write to temp file, fsync, rename (atomic)
  let tmpPath = path ++ ".tmp"
  BS.writeFile tmpPath fullData
  -- Fsync the temp file
  fd <- openFd tmpPath WriteOnly defaultFileFlags
  fsyncFd fd
  closeFd fd
  -- Atomic rename
  renameFile tmpPath path

-- | Read a checkpoint file, returning the catalog data and WAL sequence number.
readCheckpoint :: FilePath -> IO (Either Text ([(TableName, Schema, Int, [(RowId, Row)])], Word64))
readCheckpoint path = do
  exists <- doesFileExist path
  if not exists
    then return (Left "Checkpoint file not found")
    else do
      contents <- BS.readFile path
      if BS.length contents < 18  -- 4 magic + 2 version + 8 seq + 4 crc minimum
        then return (Left "Checkpoint file too small")
        else do
          -- Verify CRC
          let (payload, crcBs) = BS.splitAt (BS.length contents - 4) contents
              expectedCrc = crc32 payload
          case Get.runGetOrFail Get.getWord32be (LBS.fromStrict crcBs) of
            Left (_, _, err) -> return (Left (T.pack ("Bad CRC bytes: " ++ err)))
            Right (_, _, actualCrc)
              | fromIntegral expectedCrc /= (actualCrc :: Word32) ->
                  return (Left "Checkpoint CRC mismatch")
              | otherwise -> do
                  -- Decode header
                  case decodeHeader payload of
                    Left err -> return (Left err)
                    Right (header, rest)
                      | cpVersion header /= checkpointVersion ->
                          return (Left (T.pack ("Checkpoint version mismatch: expected "
                                    ++ show checkpointVersion ++ ", got "
                                    ++ show (cpVersion header))))
                      | otherwise ->
                          case decodeBody rest of
                            Left err -> return (Left err)
                            Right tables -> return (Right (tables, cpWALSeq header))

-- Encoding helpers

encodeHeader :: CheckpointHeader -> BS.ByteString
encodeHeader (CheckpointHeader magic ver walSeq) =
  magic
  <> LBS.toStrict (Put.runPut (Put.putWord16be (fromIntegral ver)))
  <> LBS.toStrict (Put.runPut (Put.putWord64be walSeq))

encodeBody :: [(TableName, Schema, Int, [(RowId, Row)])] -> BS.ByteString
encodeBody tables = LBS.toStrict $ Put.runPut $ do
  Put.putWord32be (fromIntegral (length tables))
  mapM_ encodeTable tables
  where
    encodeTable (name, schema, counter, rows) = do
      putText name
      Put.putWord32be (fromIntegral (length schema))
      mapM_ Binary.put schema
      Put.putWord64be (fromIntegral counter)
      Put.putWord64be (fromIntegral (length rows))
      mapM_ (\(rid, row) -> do
        Put.putWord64be (fromIntegral rid)
        Put.putWord32be (fromIntegral (V.length row))
        V.mapM_ Binary.put row
        ) rows

    putText t = do
      let bs = LBS.toStrict (Binary.encode t)
      -- Binary Text encoding includes length prefix already
      Put.putByteString bs

-- Decoding helpers

decodeHeader :: BS.ByteString -> Either Text (CheckpointHeader, BS.ByteString)
decodeHeader bs
  | BS.length bs < 14 = Left "Not enough data for checkpoint header"
  | otherwise = do
      let (magic, rest1) = BS.splitAt 4 bs
      if magic /= checkpointMagic
        then Left ("Invalid checkpoint magic: expected HSCP, got " <> T.pack (show magic))
        else do
          let (verBs, rest2) = BS.splitAt 2 rest1
              (seqBs, rest3) = BS.splitAt 8 rest2
          ver <- decodeWord16 verBs
          seqN <- decodeWord64 seqBs
          Right (CheckpointHeader magic ver seqN, rest3)
  where
    decodeWord16 b = case Get.runGetOrFail Get.getWord16be (LBS.fromStrict b) of
      Left (_, _, err) -> Left (T.pack err)
      Right (_, _, n) -> Right (fromIntegral n)
    decodeWord64 b = case Get.runGetOrFail Get.getWord64be (LBS.fromStrict b) of
      Left (_, _, err) -> Left (T.pack err)
      Right (_, _, n) -> Right n

decodeBody :: BS.ByteString -> Either Text [(TableName, Schema, Int, [(RowId, Row)])]
decodeBody bs =
  case Get.runGetOrFail getBody (LBS.fromStrict bs) of
    Left (_, _, err) -> Left (T.pack ("Checkpoint body decode error: " ++ err))
    Right (_, _, tables) -> Right tables
  where
    getBody = do
      numTables <- Get.getWord32be
      sequence (replicate (fromIntegral numTables) getTable)

    getTable = do
      name <- Binary.get :: Get.Get Text
      numCols <- Get.getWord32be
      schema <- sequence (replicate (fromIntegral numCols) Binary.get)
      counter <- fromIntegral <$> Get.getWord64be
      numRows <- Get.getWord64be
      rows <- sequence (replicate (fromIntegral numRows) getRow)
      return (name, schema, counter, rows)

    getRow = do
      rid <- fromIntegral <$> Get.getWord64be
      numVals <- Get.getWord32be
      vals <- V.replicateM (fromIntegral numVals) Binary.get
      return (rid, vals)
