module HsDb.SQL.Types
  ( Statement(..)
  , Expr(..)
  , BinOp(..)
  , SqlType(..)
  , ColumnDef(..)
  , Literal(..)
  , SelectTarget(..)
  ) where

import Data.Text (Text)

-- | A parsed SQL statement.
data Statement
  = CreateTable !Text [ColumnDef]
  | DropTable !Text
  | Insert !Text [Text] [[Literal]]
    -- ^ table, column names, rows of values
  | Select [SelectTarget] !Text (Maybe Expr)
    -- ^ columns (or star), table, optional WHERE
  | Update !Text [(Text, Expr)] (Maybe Expr)
    -- ^ table, SET assignments, optional WHERE
  | Delete !Text (Maybe Expr)
    -- ^ table, optional WHERE
  deriving (Show, Eq)

-- | What columns a SELECT targets.
data SelectTarget
  = Star
  | Column !Text
  deriving (Show, Eq)

-- | SQL column definition in CREATE TABLE.
data ColumnDef = ColumnDef
  { cdName     :: !Text
  , cdType     :: !SqlType
  , cdNullable :: !Bool
    -- ^ True if nullable (default); False if NOT NULL specified
  } deriving (Show, Eq)

-- | SQL type names we support, mapped to HsDb ColumnType.
data SqlType
  = SqlInt
  | SqlBigInt
  | SqlFloat
  | SqlText
  | SqlBool
  | SqlBytea
  deriving (Show, Eq)

-- | Literal values in SQL expressions.
data Literal
  = LitInt !Integer
  | LitFloat !Double
  | LitText !Text
  | LitBool !Bool
  | LitNull
  deriving (Show, Eq)

-- | Binary comparison/logical operators.
data BinOp
  = OpEq
  | OpNeq
  | OpLt
  | OpGt
  | OpLte
  | OpGte
  | OpAnd
  | OpOr
  deriving (Show, Eq)

-- | SQL expressions (WHERE clauses, value expressions).
data Expr
  = ExprLit !Literal
  | ExprColumn !Text
  | ExprBinOp !BinOp Expr Expr
  | ExprIsNull Expr
  | ExprIsNotNull Expr
  deriving (Show, Eq)
