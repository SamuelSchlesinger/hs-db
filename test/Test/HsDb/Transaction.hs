{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Transaction (transactionTests) where

import Hedgehog
import Control.Concurrent (forkIO)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad.Trans.Except (runExceptT)
import Data.Text (Text)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import HsDb
import HsDb.Integration (atomicallyE, commitPendingOps)
import HsDb.SQL.Parser (parseSQL)
import HsDb.SQL.Execute (executeSQL, QueryResult(..))
import HsDb.Transaction

transactionTests :: Group
transactionTests = Group "Transaction"
  [ ("prop_concurrent_tx_isolation", prop_concurrent_tx_isolation)
  , ("prop_concurrent_tx_both_commit", prop_concurrent_tx_both_commit)
  , ("prop_tx_visibility_before_commit", prop_tx_visibility_before_commit)
  , ("prop_concurrent_inserts_different_tables", prop_concurrent_inserts_different_tables)
  ]

withFreshDb :: (Database -> IO a) -> IO a
withFreshDb action = withSystemTempDirectory "hs-db-tx-test" $ \dir -> do
  let config = defaultDatabaseConfig (dir </> "test.wal")
  withDatabase config action

execSQL :: Database -> Maybe TxState -> Text -> IO (Either String QueryResult)
execSQL db mtx sql = case parseSQL sql of
  Left err -> return (Left ("Parse error: " ++ show err))
  Right stmt -> do
    result <- runExceptT (executeSQL db mtx stmt)
    case result of
      Left e  -> return (Left (show e))
      Right r -> return (Right r)

commitTx :: Database -> TxState -> IO (Either String ())
commitTx db tx = do
  ops <- getPendingOps tx
  result <- runExceptT $ atomicallyE $ commitPendingOps db ops
  case result of
    Left err -> return (Left (show err))
    Right _  -> return (Right ())

expectRight :: Show e => Either e a -> IO a
expectRight (Right a) = return a
expectRight (Left e)  = error ("Expected Right, got Left: " ++ show e)

-- Test: uncommitted transaction data is invisible to other connections
prop_concurrent_tx_isolation :: Property
prop_concurrent_tx_isolation = withTests 5 $ property $ do
  (txRows, otherRows) <- evalIO $ withFreshDb $ \db -> do
    -- Setup: create table with one committed row
    _ <- execSQL db Nothing "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db Nothing "INSERT INTO t (x) VALUES (1)"

    -- Transaction 1: insert a row but don't commit
    tx <- newTxState
    _ <- execSQL db (Just tx) "INSERT INTO t (x) VALUES (2)"

    -- The transaction should see both rows
    txResult <- expectRight =<< execSQL db (Just tx) "SELECT x FROM t ORDER BY x"

    -- A separate read (no transaction) should only see the committed row
    otherResult <- expectRight =<< execSQL db Nothing "SELECT x FROM t ORDER BY x"

    let getRows (RowResult _ rs) = rs
        getRows _ = []

    return (getRows txResult, getRows otherResult)

  -- Transaction sees both rows
  txRows === [[Just "1"], [Just "2"]]
  -- Other connection only sees committed row
  otherRows === [[Just "1"]]

-- Test: two concurrent transactions both commit successfully
prop_concurrent_tx_both_commit :: Property
prop_concurrent_tx_both_commit = withTests 5 $ property $ do
  rows <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db Nothing "CREATE TABLE t (x INT NOT NULL)"

    done1 <- newEmptyMVar
    done2 <- newEmptyMVar

    -- Transaction 1: insert value 10
    _ <- forkIO $ do
      tx <- newTxState
      _ <- execSQL db (Just tx) "INSERT INTO t (x) VALUES (10)"
      _ <- commitTx db tx
      putMVar done1 ()

    -- Transaction 2: insert value 20
    _ <- forkIO $ do
      tx <- newTxState
      _ <- execSQL db (Just tx) "INSERT INTO t (x) VALUES (20)"
      _ <- commitTx db tx
      putMVar done2 ()

    takeMVar done1
    takeMVar done2

    -- Both should be visible
    result <- expectRight =<< execSQL db Nothing "SELECT x FROM t ORDER BY x"
    case result of
      RowResult _ rs -> return rs
      _ -> return []

  -- Both transactions' inserts should be visible, sorted
  rows === [[Just "10"], [Just "20"]]

-- Test: pending writes are invisible to reads outside the transaction
prop_tx_visibility_before_commit :: Property
prop_tx_visibility_before_commit = withTests 5 $ property $ do
  (beforeCommit, afterCommit) <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db Nothing "CREATE TABLE t (x INT NOT NULL)"

    tx <- newTxState
    _ <- execSQL db (Just tx) "INSERT INTO t (x) VALUES (42)"

    -- Read without transaction — should see nothing
    r1 <- expectRight =<< execSQL db Nothing "SELECT x FROM t"

    -- Now commit
    _ <- commitTx db tx

    -- Read without transaction — should now see the row
    r2 <- expectRight =<< execSQL db Nothing "SELECT x FROM t"

    let getRows (RowResult _ rs) = rs
        getRows _ = []

    return (getRows r1, getRows r2)

  beforeCommit === []
  afterCommit === [[Just "42"]]

-- Test: concurrent transactions on different tables don't interfere
prop_concurrent_inserts_different_tables :: Property
prop_concurrent_inserts_different_tables = withTests 5 $ property $ do
  (rows1, rows2) <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db Nothing "CREATE TABLE t1 (a INT NOT NULL)"
    _ <- execSQL db Nothing "CREATE TABLE t2 (b INT NOT NULL)"

    done1 <- newEmptyMVar
    done2 <- newEmptyMVar

    -- Transaction on t1
    _ <- forkIO $ do
      tx <- newTxState
      _ <- execSQL db (Just tx) "INSERT INTO t1 (a) VALUES (1)"
      _ <- execSQL db (Just tx) "INSERT INTO t1 (a) VALUES (2)"
      _ <- commitTx db tx
      putMVar done1 ()

    -- Transaction on t2
    _ <- forkIO $ do
      tx <- newTxState
      _ <- execSQL db (Just tx) "INSERT INTO t2 (b) VALUES (10)"
      _ <- execSQL db (Just tx) "INSERT INTO t2 (b) VALUES (20)"
      _ <- commitTx db tx
      putMVar done2 ()

    takeMVar done1
    takeMVar done2

    r1 <- expectRight =<< execSQL db Nothing "SELECT a FROM t1 ORDER BY a"
    r2 <- expectRight =<< execSQL db Nothing "SELECT b FROM t2 ORDER BY b"

    let getRows (RowResult _ rs) = rs
        getRows _ = []

    return (getRows r1, getRows r2)

  rows1 === [[Just "1"], [Just "2"]]
  rows2 === [[Just "10"], [Just "20"]]
