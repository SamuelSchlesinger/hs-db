{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Table (tableTests) where

import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Control.Concurrent.STM
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
  result <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    createTable catalog name schema
  table <- unwrapRight result
  tableSchema table === schema

prop_create_duplicate_table :: Property
prop_create_duplicate_table = property $ do
  name <- forAll genTableName
  schema <- forAll genSchema
  result <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    _ <- createTable catalog name schema
    createTable catalog name schema
  case result of
    Left (TableAlreadyExists n) -> n === name
    Left e  -> do annotateShow e; failure
    Right _ -> do annotate "Expected TableAlreadyExists"; failure

prop_drop_nonexistent :: Property
prop_drop_nonexistent = property $ do
  name <- forAll genTableName
  result <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    dropTable catalog name
  result === Left (TableNotFound name)

prop_insert_select_roundtrip :: Property
prop_insert_select_roundtrip = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  (rid, rows) <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e  -> error (show e)
      Right table -> do
        res2 <- insertRow table row
        case res2 of
          Left e  -> error (show e)
          Right rid -> do
            rs <- selectAll table
            return (rid, rs)
  rid === 0
  rows === [(0, row)]

prop_insert_assigns_unique_ids :: Property
prop_insert_assigns_unique_ids = property $ do
  schema <- forAll genSchema
  n <- forAll $ Gen.int (Range.linear 1 20)
  rowList <- forAll $ Gen.list (Range.singleton n) (genRowForSchema schema)
  ids <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e -> error (show e)
      Right table ->
        mapM (\r -> do
          res2 <- insertRow table r
          case res2 of
            Left e  -> error (show e)
            Right rid -> return rid
          ) rowList
  length ids === n
  ids === [0 .. n - 1]

prop_update_row :: Property
prop_update_row = property $ do
  schema <- forAll genSchema
  row1 <- forAll (genRowForSchema schema)
  row2 <- forAll (genRowForSchema schema)
  rows <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e -> error (show e)
      Right table -> do
        res2 <- insertRow table row1
        case res2 of
          Left e -> error (show e)
          Right rid -> do
            res3 <- updateRow table "test" rid row2
            case res3 of
              Left e -> error (show e)
              Right () -> selectAll table
  rows === [(0, row2)]

prop_delete_row :: Property
prop_delete_row = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  rows <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e -> error (show e)
      Right table -> do
        res2 <- insertRow table row
        case res2 of
          Left e -> error (show e)
          Right rid -> do
            res3 <- deleteRow table "test" rid
            case res3 of
              Left e -> error (show e)
              Right () -> selectAll table
  rows === []

prop_delete_nonexistent :: Property
prop_delete_nonexistent = property $ do
  schema <- forAll genSchema
  result <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e -> error (show e)
      Right table -> deleteRow table "test" 42
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
  let schema = [Column "x" TInt32 False]
  let badRow = V.fromList [VText "hello"]
  case validateRow schema badRow of
    Left (SchemaViolation _) -> success
    other -> do annotateShow other; failure

prop_validate_null_not_allowed :: Property
prop_validate_null_not_allowed = property $ do
  let schema = [Column "x" TInt32 False]
  let badRow = V.fromList [VNull]
  case validateRow schema badRow of
    Left (SchemaViolation _) -> success
    other -> do annotateShow other; failure

prop_insert_with_id :: Property
prop_insert_with_id = property $ do
  schema <- forAll genSchema
  row <- forAll (genRowForSchema schema)
  (rows, nextId) <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema
    case res of
      Left e -> error (show e)
      Right table -> do
        res2 <- insertRowWithId table "test" 42 row
        case res2 of
          Left e -> error (show e)
          Right () -> do
            rs <- selectAll table
            counter <- readTVar (tableCounter table)
            return (rs, counter)
  rows === [(42, row)]
  nextId === 43

prop_drop_and_recreate :: Property
prop_drop_and_recreate = property $ do
  schema1 <- forAll genSchema
  schema2 <- forAll genSchema
  result <- evalIO $ atomically $ do
    catalog <- newTableCatalog
    res <- createTable catalog "test" schema1
    case res of
      Left e -> error (show e)
      Right _ -> do
        res2 <- dropTable catalog "test"
        case res2 of
          Left e -> error (show e)
          Right () -> createTable catalog "test" schema2
  table <- unwrapRight result
  tableSchema table === schema2
