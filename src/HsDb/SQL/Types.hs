-- | Abstract syntax types for the SQL dialect supported by hs-db.
module HsDb.SQL.Types
  ( Statement(..)
  , Expr(..)
  , BinOp(..)
  , SqlType(..)
  , ColumnDef(..)
  , Literal(..)
  , SelectTarget(..)
  , SortOrder(..)
  , OrderByClause(..)
  , AggFunc(..)
  ) where

import Data.Text (Text)

-- | A parsed SQL statement.
data Statement
  = CreateTable !Text [ColumnDef]
    -- ^ @CREATE TABLE name (columns)@
  | DropTable !Text
    -- ^ @DROP TABLE name@
  | Insert !Text [Text] [[Literal]]
    -- ^ table, column names, rows of values
  | Select !Bool [SelectTarget] !Text (Maybe Expr) [OrderByClause] (Maybe Int) (Maybe Int)
    -- ^ DISTINCT flag, columns (or star), table, WHERE, ORDER BY, LIMIT, OFFSET
  | Update !Text [(Text, Expr)] (Maybe Expr)
    -- ^ table, SET assignments, optional WHERE
  | Delete !Text (Maybe Expr)
    -- ^ table, optional WHERE
  | AlterTableAddColumn !Text ColumnDef
    -- ^ @ALTER TABLE name ADD COLUMN coldef@
  | Explain Statement
    -- ^ @EXPLAIN statement@
  | Begin
  | Commit
  | Rollback
  deriving (Show, Eq)

-- | What columns a SELECT targets.
data SelectTarget
  = Star
  | Column !Text
  | ColumnAs !Text !Text
    -- ^ column name, alias
  | Agg !AggFunc !(Maybe Text)
    -- ^ aggregate function, optional alias
  deriving (Show, Eq)

-- | Aggregate functions.
data AggFunc
  = AggCount       -- ^ COUNT(*)
  | AggCountCol !Text  -- ^ COUNT(col)
  | AggSum !Text   -- ^ SUM(col)
  | AggAvg !Text   -- ^ AVG(col)
  | AggMin !Text   -- ^ MIN(col)
  | AggMax !Text   -- ^ MAX(col)
  deriving (Show, Eq)

-- | Sort order for ORDER BY clauses.
data SortOrder = Asc | Desc deriving (Show, Eq)

-- | A single ORDER BY clause: column name and sort direction.
data OrderByClause = OrderByClause !Text !SortOrder
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
  | OpLike
  | OpILike
  deriving (Show, Eq)

-- | SQL expressions (WHERE clauses, value expressions).
data Expr
  = ExprLit !Literal
  | ExprColumn !Text
  | ExprBinOp !BinOp Expr Expr
  | ExprIsNull Expr
  | ExprIsNotNull Expr
  | ExprIn Expr [Expr]
  | ExprNot Expr
  deriving (Show, Eq)
