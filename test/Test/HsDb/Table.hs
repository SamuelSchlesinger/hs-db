{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Table (tableTests) where

import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Control.Concurrent.STM
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import qualified Data.Vector as V

import HsDb.Types
import HsDb.Table
import Test.HsDb.Generators

-- Helper: unwrap Right in IO, failing the test on Left
unwrapRight :: (Show e, MonadTest m) => Either e a -> m a
unwrapRight (Right a) = return a
unwrapRight (Left e)  = do annotateShow e; failure

tableTests :: Group
tableTests = Group "Table"
  [ ("prop_create_table", prop_create_table)
  , ("prop_create_duplicate_table", prop_create_duplicate_table)
  , ("prop_drop_nonexistent", prop_drop_nonexistent)
  , ("prop_insert_select_roundtrip", prop_insert_select_roundtrip)
  , ("prop_insert_assigns_unique_ids", prop_insert_assigns_unique_ids)
  , ("prop_update_row", prop_update_row)
  , ("prop_delete_row", prop_delete_row)
  , ("prop_delete_nonexistent", prop_delete_nonexistent)
  , ("prop_validate_wrong_column_count", prop_validate_wrong_column_count)
  , ("prop_validate_wrong_type", prop_validate_wrong_type)
  , ("prop_validate_null_not_allowed", prop_validate_null_not_allowed)
  , ("prop_insert_with_id", prop_insert_with_id)
  , ("prop_drop_and_recreate", prop_drop_and_recreate)
  ]

prop_create_table :: Property
prop_create_table = property $ do
  name <- forAll genTableName
  schema <- forAll genSchema
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    createTable catalog name schema
  table <- unwrapRight result
  tableSchema table === schema

prop_create_duplicate_table :: Property
prop_create_duplicate_table = property $ do
  name <- forAll genTableName
  schema <- forAll genSchema
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    _ <- createTable catalog name schema
    createTable catalog name schema
  case result of
    Left (TableAlreadyExists n) -> n === name
    Left e  -> do annotateShow e; failure
    Right _ -> do annotate "Expected TableAlreadyExists"; failure

prop_drop_nonexistent :: Property
prop_drop_nonexistent = property $ do
  name <- forAll genTableName
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    dropTable catalog name
  result === Left (TableNotFound name)

prop_insert_select_roundtrip :: Property
prop_insert_select_roundtrip = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    rid <- insertRow table row
    rs <- lift $ selectAll table
    return (rid, rs)
  (rid, rows) <- unwrapRight result
  rid === 0
  rows === [(0, row)]

prop_insert_assigns_unique_ids :: Property
prop_insert_assigns_unique_ids = property $ do
  schema <- forAll genSchema
  n <- forAll $ Gen.int (Range.linear 1 20)
  rowList <- forAll $ Gen.list (Range.singleton n) (genRowForSchema schema)
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    mapM (insertRow table) rowList
  ids <- unwrapRight result
  length ids === n
  ids === [0 .. n - 1]

prop_update_row :: Property
prop_update_row = property $ do
  schema <- forAll genSchema
  row1 <- forAll (genRowForSchema schema)
  row2 <- forAll (genRowForSchema schema)
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    rid <- insertRow table row1
    updateRow table "test" rid row2
    lift $ selectAll table
  rows <- unwrapRight result
  rows === [(0, row2)]

prop_delete_row :: Property
prop_delete_row = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    rid <- insertRow table row
    deleteRow table "test" rid
    lift $ selectAll table
  rows <- unwrapRight result
  rows === []

prop_delete_nonexistent :: Property
prop_delete_nonexistent = property $ do
  schema <- forAll genSchema
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    deleteRow table "test" 42
  case result of
    Left (RowNotFound _ 42) -> success
    _ -> do annotateShow result; failure

prop_validate_wrong_column_count :: Property
prop_validate_wrong_column_count = property $ do
  schema <- forAll genSchema
  let badRow = V.fromList []
  case validateRow schema badRow of
    Left (SchemaViolation _) -> success
    other -> do annotateShow other; failure

prop_validate_wrong_type :: Property
prop_validate_wrong_type = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let badRow = V.fromList [VText "hello"]
  case validateRow schema badRow of
    Left (SchemaViolation _) -> success
    other -> do annotateShow other; failure

prop_validate_null_not_allowed :: Property
prop_validate_null_not_allowed = property $ do
  let schema = V.fromList [Column "x" TInt32 False]
  let badRow = V.fromList [VNull]
  case validateRow schema badRow of
    Left (SchemaViolation _) -> success
    other -> do annotateShow other; failure

prop_insert_with_id :: Property
prop_insert_with_id = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    table <- createTable catalog "test" schema
    insertRowWithId table "test" 42 row
    rs <- lift $ selectAll table
    counter <- lift $ readTVar (tableCounter table)
    return (rs, counter)
  (rows, nextId) <- unwrapRight result
  rows === [(42, row)]
  nextId === 43

prop_drop_and_recreate :: Property
prop_drop_and_recreate = property $ do
  schema1 <- forAll genSchema
  schema2 <- forAll genSchema
  result <- evalIO $ atomically $ runExceptT $ do
    catalog <- lift newTableCatalog
    _ <- createTable catalog "test" schema1
    dropTable catalog "test"
    createTable catalog "test" schema2
  table <- unwrapRight result
  tableSchema table === schema2
