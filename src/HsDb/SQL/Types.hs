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
  , FromClause(..)
  , JoinType(..)
  , InRHS(..)
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
  | Select !Bool [SelectTarget] FromClause (Maybe Expr) [Expr] (Maybe Expr) [OrderByClause] (Maybe Int) (Maybe Int)
    -- ^ DISTINCT flag, columns (or star), from clause, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
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
  | SelExpr Expr (Maybe Text)
    -- ^ arbitrary expression, optional alias
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

-- | JOIN types.
data JoinType = InnerJoin | LeftJoin | RightJoin | CrossJoin
  deriving (Show, Eq)

-- | FROM clause source.
data FromClause
  = FromTable !Text !(Maybe Text)
    -- ^ table name, optional alias
  | FromJoin !JoinType FromClause FromClause (Maybe Expr)
    -- ^ join type, left, right, ON condition
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
  | OpAdd
  | OpSub
  | OpMul
  | OpDiv
  | OpMod
  deriving (Show, Eq)

-- | SQL expressions (WHERE clauses, value expressions).
data Expr
  = ExprLit !Literal
  | ExprColumn !Text
  | ExprBinOp !BinOp Expr Expr
  | ExprIsNull Expr
  | ExprIsNotNull Expr
  | ExprIn Expr InRHS
  | ExprNot Expr
  | ExprBetween Expr Expr Expr
  | ExprAgg AggFunc
  | ExprQualColumn !Text !Text
    -- ^ qualifier.column
  deriving (Show, Eq)

-- | Right-hand side of IN expression.
data InRHS
  = InList [Expr]
  | InSubquery Statement
  deriving (Show, Eq)
