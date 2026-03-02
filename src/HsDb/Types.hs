module HsDb.Types
  ( Value(..)
  , ColumnType(..)
  , Column(..)
  , Schema
  , Row
  , RowId
  , TableName
  , DbError(..)
  , assert64Bit
  ) where

import Data.Bits (finiteBitSize)
import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Vector (Vector)

-- | 64-bit platform assertion. Call at startup (e.g. in openDatabase) to
-- ensure the platform meets hs-db's Int width requirement.
assert64Bit :: ()
assert64Bit
  | finiteBitSize (0 :: Int) >= 64 = ()
  | otherwise = error "hs-db requires a 64-bit platform (Int must be >= 64 bits)"

-- | Column value sum type, aligned with PostgreSQL basics.
data Value
  = VInt32 !Int32
  | VInt64 !Int64
  | VFloat64 !Double
  | VText !Text
  | VBool !Bool
  | VBytea !ByteString
  | VNull
  deriving (Show, Eq)

-- | Column type tags, corresponding to Value constructors.
data ColumnType
  = TInt32
  | TInt64
  | TFloat64
  | TText
  | TBool
  | TBytea
  deriving (Show, Eq)

-- | Column definition: name, type, and whether nulls are allowed.
data Column = Column
  { columnName     :: !Text
  , columnType     :: !ColumnType
  , columnNullable :: !Bool
  } deriving (Show, Eq)

-- | A schema is a list of column definitions.
type Schema = [Column]

-- | A row is a vector of values, indexed by column position.
type Row = Vector Value

-- | Row identifier. Int is 64-bit on the target platform.
type RowId = Int

-- | Table name.
type TableName = Text

-- | Errors that can occur during database operations.
data DbError
  = TableAlreadyExists !TableName
  | TableNotFound !TableName
  | RowNotFound !TableName !RowId
  | SchemaViolation !Text
  | WALCorrupted !Text
  | WALVersionMismatch !Int !Int  -- ^ expected, actual
  | WALSequenceError !Text
  | DatabaseNotWritable !Text
  | DuplicateRowId !TableName !RowId
  deriving (Show, Eq)
