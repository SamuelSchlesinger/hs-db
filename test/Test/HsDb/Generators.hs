{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.Generators
  ( genValue
  , genValueForType
  , genColumnType
  , genColumn
  , genSchema
  , genRowForSchema
  , genTableName
  ) where

import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Vector as V

import HsDb.Types

genTableName :: Gen TableName
genTableName = T.pack <$> Gen.string (Range.linear 1 20) Gen.alpha

genColumnType :: Gen ColumnType
genColumnType = Gen.element [TInt32, TInt64, TFloat64, TText, TBool, TBytea]

genColumn :: Gen Column
genColumn = Column
  <$> (T.pack <$> Gen.string (Range.linear 1 15) Gen.alpha)
  <*> genColumnType
  <*> Gen.bool

genSchema :: Gen Schema
genSchema = Gen.list (Range.linear 1 8) genColumn

genValueForType :: Column -> Gen Value
genValueForType col = do
  let gen = case columnType col of
        TInt32   -> VInt32 <$> Gen.int32 Range.linearBounded
        TInt64   -> VInt64 <$> Gen.int64 Range.linearBounded
        TFloat64 -> VFloat64 <$> Gen.double (Range.linearFrac (-1e6) 1e6)
        TText    -> VText . T.pack <$> Gen.string (Range.linear 0 50) Gen.unicode
        TBool    -> VBool <$> Gen.bool
        TBytea   -> VBytea . BS.pack <$> Gen.list (Range.linear 0 50) (Gen.word8 Range.linearBounded)
  if columnNullable col
    then Gen.frequency [(5, gen), (1, pure VNull)]
    else gen

genRowForSchema :: Schema -> Gen Row
genRowForSchema schema = V.fromList <$> traverse genValueForType schema

genValue :: Gen Value
genValue = Gen.choice
  [ VInt32 <$> Gen.int32 Range.linearBounded
  , VInt64 <$> Gen.int64 Range.linearBounded
  , VFloat64 <$> Gen.double (Range.linearFrac (-1e6) 1e6)
  , VText . T.pack <$> Gen.string (Range.linear 0 50) Gen.unicode
  , VBool <$> Gen.bool
  , VBytea . BS.pack <$> Gen.list (Range.linear 0 50) (Gen.word8 Range.linearBounded)
  , pure VNull
  ]
