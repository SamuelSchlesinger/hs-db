{-# LANGUAGE OverloadedStrings #-}

-- | Transaction support for multi-statement BEGIN/COMMIT/ROLLBACK.
-- Uses deferred apply: mutations are buffered as pending ops and only
-- applied to the in-memory catalog + WAL on commit. Rollback discards
-- the buffer with no side effects.
module HsDb.Transaction
  ( TxState(..)
  , TxStatus(..)
  , PendingOp(..)
  , newTxState
  , addPendingOp
  , getPendingOps
  , effectiveRows
  , effectiveSchema
  ) where

import Control.Concurrent.STM (atomically, readTVar)
import Data.IORef
import Data.Text (Text)
import qualified Data.Map.Strict as Map
import Data.Vector (Vector)
import qualified Data.IntMap.Strict as IntMap

import HsDb.Types
import HsDb.Table (Table(..), TableCatalog)

-- | Transaction status.
data TxStatus = TxActive | TxAborted !Text
  deriving (Show, Eq)

-- | A pending operation buffered within a transaction.
data PendingOp
  = PendingCreate !TableName !Schema
  | PendingInsert !TableName !(Vector Value)
  | PendingUpdate !TableName !RowId !(Vector Value)
  | PendingDelete !TableName !RowId
  | PendingDrop   !TableName
  deriving (Show, Eq)

-- | Per-connection transaction state.
data TxState = TxState
  { txOps    :: !(IORef [PendingOp])   -- ^ Accumulated ops (in reverse order)
  , txStatus :: !(IORef TxStatus)
  }

newTxState :: IO TxState
newTxState = TxState <$> newIORef [] <*> newIORef TxActive

addPendingOp :: TxState -> PendingOp -> IO ()
addPendingOp tx op = modifyIORef' (txOps tx) (op :)

getPendingOps :: TxState -> IO [PendingOp]
getPendingOps tx = reverse <$> readIORef (txOps tx)

-- | Get the effective schema for a table, considering pending creates/drops.
effectiveSchema :: TableCatalog -> TxState -> TableName -> IO (Either Text Schema)
effectiveSchema catalog tx name = do
  committed <- atomically $ readTVar catalog
  ops <- getPendingOps tx
  let base = case Map.lookup name committed of
               Just t  -> Right (tableSchema t)
               Nothing -> Left ("Table '" <> name <> "' does not exist")
  return (foldl go base ops)
  where
    go _acc (PendingCreate n schema) | n == name = Right schema
    go _   (PendingDrop n)          | n == name = Left ("Table '" <> name <> "' does not exist")
    go acc _                                    = acc

-- | Get effective rows for a table, overlaying pending ops on committed state.
-- Pending inserts get temporary row IDs starting from the table's next counter.
effectiveRows :: TableCatalog -> TxState -> TableName
              -> IO (Either Text [(RowId, Row)])
effectiveRows catalog tx name = do
  committed <- atomically $ readTVar catalog
  ops <- getPendingOps tx
  -- Determine base state: committed rows or empty (if table created in tx)
  baseResult <- case resolveTable committed ops name of
    Left err          -> return (Left err)
    Right Nothing     -> return (Right (IntMap.empty, 0))
    Right (Just table) -> do
      rows <- atomically $ readTVar (tableRows table)
      counter <- atomically $ readTVar (tableCounter table)
      return (Right (rows, counter))
  return $ case baseResult of
    Left err -> Left err
    Right (rows, counter) ->
      Right (IntMap.toAscList (applyOps name ops rows counter))

-- | Determine if a table exists after applying pending creates/drops.
-- Returns: Left error, Right Nothing (created in tx), Right (Just table) (committed).
resolveTable :: Map.Map TableName Table -> [PendingOp] -> TableName
             -> Either Text (Maybe Table)
resolveTable committed ops tname = foldl go base ops
  where
    base = case Map.lookup tname committed of
             Just t  -> Right (Just t)
             Nothing -> Left ("Table '" <> tname <> "' does not exist")
    go _ (PendingCreate n _) | n == tname = Right Nothing
    go _ (PendingDrop n)     | n == tname = Left ("Table '" <> tname <> "' does not exist")
    go acc _                              = acc

-- | Apply pending ops to a row map.
applyOps :: TableName -> [PendingOp] -> IntMap.IntMap Row -> Int -> IntMap.IntMap Row
applyOps tname ops rows0 counter0 = fst (foldl go (rows0, counter0) ops)
  where
    go (_rows, _nxt) (PendingCreate n _) | n == tname = (IntMap.empty, 0)
    go _ (PendingDrop n)               | n == tname = (IntMap.empty, 0)
    go (rows, nxt) (PendingInsert n row) | n == tname =
      (IntMap.insert nxt row rows, nxt + 1)
    go (rows, nxt) (PendingUpdate n rid row) | n == tname =
      (IntMap.insert rid row rows, nxt)
    go (rows, nxt) (PendingDelete n rid) | n == tname =
      (IntMap.delete rid rows, nxt)
    go acc _ = acc
