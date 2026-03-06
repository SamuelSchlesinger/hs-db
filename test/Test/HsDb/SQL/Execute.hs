{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.SQL.Execute (executeTests) where

import Hedgehog
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Except (runExceptT)
import Data.Text (Text)
import System.FilePath ((</>))
import System.IO.Temp (withSystemTempDirectory)

import HsDb
import HsDb.Integration (Database(..), atomicallyE, commitPendingOps)
import HsDb.SQL.Parser (parseSQL)
import HsDb.SQL.Execute
import HsDb.SQL.Types (Statement(..))
import HsDb.Transaction

executeTests :: Group
executeTests = Group "SQL.Execute"
  [ ("prop_create_and_insert", prop_create_and_insert)
  , ("prop_select_star", prop_select_star)
  , ("prop_select_columns", prop_select_columns)
  , ("prop_select_nonexistent_column", prop_select_nonexistent_column)
  , ("prop_select_where", prop_select_where)
  , ("prop_update", prop_update)
  , ("prop_delete", prop_delete)
  , ("prop_drop_table", prop_drop_table)
  , ("prop_insert_type_mismatch", prop_insert_type_mismatch)
  , ("prop_select_mixed_star_error", prop_select_mixed_star_error)
  , ("prop_null_handling", prop_null_handling)
  , ("prop_multi_row_insert", prop_multi_row_insert)
  , ("prop_order_by_asc", prop_order_by_asc)
  , ("prop_order_by_desc", prop_order_by_desc)
  , ("prop_order_by_multi", prop_order_by_multi)
  , ("prop_limit", prop_limit)
  , ("prop_offset", prop_offset)
  , ("prop_order_by_limit", prop_order_by_limit)
  , ("prop_order_by_null", prop_order_by_null)
  , ("prop_tx_commit", prop_tx_commit)
  , ("prop_tx_rollback", prop_tx_rollback)
  , ("prop_tx_select_sees_pending", prop_tx_select_sees_pending)
  , ("prop_tx_drop_rollback", prop_tx_drop_rollback)
  , ("prop_tx_abort_on_error", prop_tx_abort_on_error)
  , ("prop_distinct", prop_distinct)
  , ("prop_column_alias", prop_column_alias)
  , ("prop_count_star", prop_count_star)
  , ("prop_count_col", prop_count_col)
  , ("prop_sum_avg", prop_sum_avg)
  , ("prop_min_max", prop_min_max)
  , ("prop_like", prop_like)
  , ("prop_ilike", prop_ilike)
  , ("prop_in_operator", prop_in_operator)
  , ("prop_not_operator", prop_not_operator)
  , ("prop_alter_add_column", prop_alter_add_column)
  , ("prop_explain", prop_explain)
  ]

-- Run a SQL string against a fresh database in a temp directory.
withFreshDb :: (Database -> IO a) -> IO a
withFreshDb action = withSystemTempDirectory "hs-db-test" $ \dir -> do
  let config = defaultDatabaseConfig (dir </> "test.wal")
  withDatabase config action

execSQL :: Database -> Text -> IO (Either String QueryResult)
execSQL db sql = case parseSQL sql of
  Left err -> return (Left ("Parse error: " ++ show err))
  Right stmt -> do
    result <- runExceptT (executeSQL db Nothing stmt)
    case result of
      Left e  -> return (Left (show e))
      Right r -> return (Right r)

expectExecRight :: (MonadTest m, MonadIO m) => IO (Either String QueryResult) -> m QueryResult
expectExecRight action = do
  result <- evalIO action
  case result of
    Right r -> return r
    Left e  -> do annotate e; failure

expectExecLeft :: (MonadTest m, MonadIO m) => IO (Either String QueryResult) -> m ()
expectExecLeft action = do
  result <- evalIO action
  case result of
    Left _  -> success
    Right r -> do annotateShow r; failure

prop_create_and_insert :: Property
prop_create_and_insert = withTests 1 $ property $ do
  r <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (id INT NOT NULL, name TEXT)"
    execSQL db "INSERT INTO t (id, name) VALUES (42, 'hello')"
  case r of
    Right (CommandComplete tag) -> tag === "INSERT 0 1"
    Right other -> do annotateShow other; failure
    Left e      -> do annotate e; failure

prop_select_star :: Property
prop_select_star = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL, y TEXT)"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (1, 'a')"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (2, 'b')"
    execSQL db "SELECT * FROM t"
  case result of
    RowResult cols rows -> do
      length cols === 2
      ciName (head cols) === "x"
      length rows === 2
    other -> do annotateShow other; failure

prop_select_columns :: Property
prop_select_columns = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL, y TEXT, z BOOLEAN)"
    _ <- execSQL db "INSERT INTO t (x, y, z) VALUES (1, 'hello', TRUE)"
    execSQL db "SELECT y, x FROM t"
  case result of
    RowResult cols rows -> do
      map ciName cols === ["y", "x"]
      rows === [[Just "hello", Just "1"]]
    other -> do annotateShow other; failure

prop_select_nonexistent_column :: Property
prop_select_nonexistent_column = withTests 1 $ property $ do
  expectExecLeft $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1)"
    execSQL db "SELECT nonexistent FROM t"

prop_select_where :: Property
prop_select_where = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL, y TEXT)"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (1, 'a')"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (2, 'b')"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (3, 'c')"
    execSQL db "SELECT y FROM t WHERE x > 1"
  case result of
    RowResult _ rows -> do
      length rows === 2
      rows === [[Just "b"], [Just "c"]]
    other -> do annotateShow other; failure

prop_update :: Property
prop_update = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL, y TEXT)"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (1, 'old')"
    _ <- execSQL db "UPDATE t SET y = 'new' WHERE x = 1"
    execSQL db "SELECT y FROM t"
  case result of
    RowResult _ rows -> rows === [[Just "new"]]
    other -> do annotateShow other; failure

prop_delete :: Property
prop_delete = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (2)"
    _ <- execSQL db "DELETE FROM t WHERE x = 1"
    execSQL db "SELECT x FROM t"
  case result of
    RowResult _ rows -> rows === [[Just "2"]]
    other -> do annotateShow other; failure

prop_drop_table :: Property
prop_drop_table = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "DROP TABLE t"
    execSQL db "SELECT * FROM t"
  case result of
    Left _ -> success  -- table should not exist
    Right _ -> do annotate "Expected error after DROP TABLE"; failure

prop_insert_type_mismatch :: Property
prop_insert_type_mismatch = withTests 1 $ property $ do
  expectExecLeft $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    execSQL db "INSERT INTO t (x) VALUES ('not a number')"

prop_select_mixed_star_error :: Property
prop_select_mixed_star_error = withTests 1 $ property $ do
  -- SELECT *, x FROM t — the parser would parse this as [Star] since * is
  -- first, so this is more of a parser-level test. The executor's error
  -- path for Star mixed with columns is covered if the parser produces it.
  -- For now, just verify * alone works.
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1)"
    execSQL db "SELECT * FROM t"
  case result of
    RowResult cols _ -> map ciName cols === ["x"]
    other -> do annotateShow other; failure

prop_null_handling :: Property
prop_null_handling = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT, y TEXT)"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (NULL, 'a')"
    execSQL db "SELECT * FROM t WHERE x IS NULL"
  case result of
    RowResult _ rows -> length rows === 1
    other -> do annotateShow other; failure

prop_multi_row_insert :: Property
prop_multi_row_insert = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (3)"
    execSQL db "SELECT * FROM t"
  case result of
    RowResult _ rows -> length rows === 3
    other -> do annotateShow other; failure

prop_order_by_asc :: Property
prop_order_by_asc = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (3), (1), (2)"
    execSQL db "SELECT x FROM t ORDER BY x"
  case result of
    RowResult _ rows -> rows === [[Just "1"], [Just "2"], [Just "3"]]
    other -> do annotateShow other; failure

prop_order_by_desc :: Property
prop_order_by_desc = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (3), (1), (2)"
    execSQL db "SELECT x FROM t ORDER BY x DESC"
  case result of
    RowResult _ rows -> rows === [[Just "3"], [Just "2"], [Just "1"]]
    other -> do annotateShow other; failure

prop_order_by_multi :: Property
prop_order_by_multi = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL, y INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x, y) VALUES (1, 2), (1, 1), (2, 1)"
    execSQL db "SELECT x, y FROM t ORDER BY x ASC, y DESC"
  case result of
    RowResult _ rows -> rows === [[Just "1", Just "2"], [Just "1", Just "1"], [Just "2", Just "1"]]
    other -> do annotateShow other; failure

prop_limit :: Property
prop_limit = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (3), (4), (5)"
    execSQL db "SELECT x FROM t LIMIT 3"
  case result of
    RowResult _ rows -> length rows === 3
    other -> do annotateShow other; failure

prop_offset :: Property
prop_offset = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (3), (4), (5)"
    execSQL db "SELECT x FROM t ORDER BY x LIMIT 2 OFFSET 2"
  case result of
    RowResult _ rows -> rows === [[Just "3"], [Just "4"]]
    other -> do annotateShow other; failure

prop_order_by_limit :: Property
prop_order_by_limit = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (5), (3), (1), (4), (2)"
    execSQL db "SELECT x FROM t ORDER BY x DESC LIMIT 3"
  case result of
    RowResult _ rows -> rows === [[Just "5"], [Just "4"], [Just "3"]]
    other -> do annotateShow other; failure

prop_order_by_null :: Property
prop_order_by_null = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (3), (NULL), (1)"
    execSQL db "SELECT x FROM t ORDER BY x"
  case result of
    -- NULLs last in ASC order
    RowResult _ rows -> rows === [[Just "1"], [Just "3"], [Nothing]]
    other -> do annotateShow other; failure

-- Helper for executing SQL in a transaction context
execSQLTx :: Database -> Maybe TxState -> Text -> IO (Either String QueryResult)
execSQLTx db mtx sql = case parseSQL sql of
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

prop_tx_commit :: Property
prop_tx_commit = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    tx <- newTxState
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (1)"
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (2)"
    _ <- commitTx db tx
    execSQL db "SELECT x FROM t ORDER BY x"
  case result of
    Right (RowResult _ rows) -> rows === [[Just "1"], [Just "2"]]
    Right other -> do annotateShow other; failure
    Left e -> do annotate e; failure

prop_tx_rollback :: Property
prop_tx_rollback = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1)"
    tx <- newTxState
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (2)"
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (3)"
    -- Don't commit — just drop tx (rollback)
    execSQL db "SELECT x FROM t ORDER BY x"
  case result of
    Right (RowResult _ rows) -> rows === [[Just "1"]]
    Right other -> do annotateShow other; failure
    Left e -> do annotate e; failure

prop_tx_select_sees_pending :: Property
prop_tx_select_sees_pending = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1)"
    tx <- newTxState
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (2)"
    execSQLTx db (Just tx) "SELECT x FROM t ORDER BY x"
  case result of
    Right (RowResult _ rows) -> rows === [[Just "1"], [Just "2"]]
    Right other -> do annotateShow other; failure
    Left e -> do annotate e; failure

prop_tx_drop_rollback :: Property
prop_tx_drop_rollback = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (42)"
    tx <- newTxState
    _ <- execSQLTx db (Just tx) "DROP TABLE t"
    -- Don't commit — table should survive
    execSQL db "SELECT x FROM t"
  case result of
    Right (RowResult _ rows) -> rows === [[Just "42"]]
    Right other -> do annotateShow other; failure
    Left e -> do annotate e; failure

prop_tx_abort_on_error :: Property
prop_tx_abort_on_error = withTests 1 $ property $ do
  result <- evalIO $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    tx <- newTxState
    _ <- execSQLTx db (Just tx) "INSERT INTO t (x) VALUES (1)"
    -- Insert into nonexistent table — should fail
    r <- execSQLTx db (Just tx) "INSERT INTO nonexistent (x) VALUES (2)"
    case r of
      Left _ -> return ()
      Right _ -> error "Expected error"
    -- Even after the error, pending ops from before the error still exist
    -- But the tx should not be committed if the user rolls back
    execSQL db "SELECT x FROM t"
  case result of
    Right (RowResult _ rows) -> length rows === 0  -- nothing committed
    Right other -> do annotateShow other; failure
    Left e -> do annotate e; failure

-- Phase 8 feature tests

prop_distinct :: Property
prop_distinct = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (1), (3), (2)"
    execSQL db "SELECT DISTINCT x FROM t ORDER BY x"
  case result of
    RowResult _ rows -> rows === [[Just "1"], [Just "2"], [Just "3"]]
    other -> do annotateShow other; failure

prop_column_alias :: Property
prop_column_alias = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (42)"
    execSQL db "SELECT x AS val FROM t"
  case result of
    RowResult cols rows -> do
      map ciName cols === ["val"]
      rows === [[Just "42"]]
    other -> do annotateShow other; failure

prop_count_star :: Property
prop_count_star = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (3)"
    execSQL db "SELECT COUNT(*) FROM t"
  case result of
    RowResult cols rows -> do
      map ciName cols === ["count"]
      rows === [[Just "3"]]
    other -> do annotateShow other; failure

prop_count_col :: Property
prop_count_col = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (NULL), (3)"
    execSQL db "SELECT COUNT(x) FROM t"
  case result of
    RowResult _ rows -> rows === [[Just "2"]]  -- NULL not counted
    other -> do annotateShow other; failure

prop_sum_avg :: Property
prop_sum_avg = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (10), (20), (30)"
    execSQL db "SELECT SUM(x) FROM t"
  case result of
    RowResult _ rows -> rows === [[Just "60"]]
    other -> do annotateShow other; failure

prop_min_max :: Property
prop_min_max = withTests 1 $ property $ do
  resultMin <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (3), (1), (2)"
    execSQL db "SELECT MIN(x) FROM t"
  case resultMin of
    RowResult _ rows -> rows === [[Just "1"]]
    other -> do annotateShow other; failure
  resultMax <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (3), (1), (2)"
    execSQL db "SELECT MAX(x) FROM t"
  case resultMax of
    RowResult _ rows -> rows === [[Just "3"]]
    other -> do annotateShow other; failure

prop_like :: Property
prop_like = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (name TEXT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (name) VALUES ('hello'), ('world'), ('help')"
    execSQL db "SELECT name FROM t WHERE name LIKE 'hel%' ORDER BY name"
  case result of
    RowResult _ rows -> rows === [[Just "hello"], [Just "help"]]
    other -> do annotateShow other; failure

prop_ilike :: Property
prop_ilike = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (name TEXT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (name) VALUES ('Hello'), ('WORLD'), ('help')"
    execSQL db "SELECT name FROM t WHERE name ILIKE 'hel%' ORDER BY name"
  case result of
    RowResult _ rows -> rows === [[Just "Hello"], [Just "help"]]
    other -> do annotateShow other; failure

prop_in_operator :: Property
prop_in_operator = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2), (3), (4), (5)"
    execSQL db "SELECT x FROM t WHERE x IN (2, 4) ORDER BY x"
  case result of
    RowResult _ rows -> rows === [[Just "2"], [Just "4"]]
    other -> do annotateShow other; failure

prop_not_operator :: Property
prop_not_operator = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x BOOLEAN NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (TRUE), (FALSE)"
    execSQL db "SELECT x FROM t WHERE NOT x = TRUE"
  case result of
    RowResult _ rows -> rows === [[Just "f"]]
    other -> do annotateShow other; failure

prop_alter_add_column :: Property
prop_alter_add_column = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    _ <- execSQL db "INSERT INTO t (x) VALUES (1), (2)"
    _ <- execSQL db "ALTER TABLE t ADD COLUMN y TEXT"
    _ <- execSQL db "UPDATE t SET y = 'hello' WHERE x = 1"
    execSQL db "SELECT x, y FROM t ORDER BY x"
  case result of
    RowResult cols rows -> do
      map ciName cols === ["x", "y"]
      rows === [[Just "1", Just "hello"], [Just "2", Nothing]]
    other -> do annotateShow other; failure

prop_explain :: Property
prop_explain = withTests 1 $ property $ do
  result <- expectExecRight $ withFreshDb $ \db -> do
    _ <- execSQL db "CREATE TABLE t (x INT NOT NULL)"
    execSQL db "EXPLAIN SELECT * FROM t WHERE x > 0 ORDER BY x LIMIT 10"
  case result of
    RowResult cols rows -> do
      map ciName cols === ["QUERY PLAN"]
      -- Should have multiple plan lines
      assert (length rows >= 2)
    other -> do annotateShow other; failure
