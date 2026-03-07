{-# LANGUAGE OverloadedStrings #-}

-- | Hand-written recursive-descent SQL parser. Supports CREATE TABLE, DROP
-- TABLE, INSERT, SELECT, UPDATE, and DELETE with WHERE clauses.
module HsDb.SQL.Parser
  ( parseSQL
  , ParseError(..)
  ) where

import Data.Char (isAlphaNum, isAlpha, isDigit, isSpace)
import qualified Data.Set as Set
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Read as TR

import HsDb.SQL.Types

-- | Parse error with position and message.
data ParseError = ParseError
  { pePosition :: !Int
  , peMessage  :: !Text
  } deriving (Show, Eq)

-- Parser state: remaining input and current position.
data PState = PState
  { psInput :: !Text
  , psPos   :: !Int
  } deriving (Show)

type Parser a = PState -> Either ParseError (a, PState)

-- | Parse a SQL statement from text.
parseSQL :: Text -> Either ParseError Statement
parseSQL input =
  case pStatement (PState input 0) of
    Left err -> Left err
    Right (stmt, s) -> do
      let s' = skipWhitespace s
      if T.null (psInput s')
        then Right stmt
        else Left (ParseError (psPos s') ("Unexpected input after statement: " <> T.take 20 (psInput s')))

-- Combinators

pFail :: Text -> Parser a
pFail msg s = Left (ParseError (psPos s) msg)

pSatisfy :: (Char -> Bool) -> Text -> Parser Char
pSatisfy p msg s = case T.uncons (psInput s) of
  Nothing -> Left (ParseError (psPos s) ("Unexpected end of input, expected " <> msg))
  Just (c, rest)
    | p c       -> Right (c, s { psInput = rest, psPos = psPos s + 1 })
    | otherwise -> Left (ParseError (psPos s) ("Expected " <> msg <> " but got '" <> T.singleton c <> "'"))

pOptional :: Parser a -> Parser (Maybe a)
pOptional p s = case p s of
  Left _        -> Right (Nothing, s)
  Right (a, s') -> Right (Just a, s')

skipWhitespace :: PState -> PState
skipWhitespace s = case T.uncons (psInput s) of
  Just (c, rest) | isSpace c -> skipWhitespace (s { psInput = rest, psPos = psPos s + 1 })
  _ -> s

-- Case-insensitive keyword match. Must not be followed by an alphanumeric char.
pKeyword :: Text -> Parser ()
pKeyword kw s0 = do
  let s = skipWhitespace s0
      kwLen = T.length kw
      (prefix, rest) = T.splitAt kwLen (psInput s)
  if T.toLower prefix == T.toLower kw
    then case T.uncons rest of
      Just (c, _) | isAlphaNum c || c == '_' ->
        Left (ParseError (psPos s) ("Expected keyword " <> kw))
      _ -> Right ((), s { psInput = rest, psPos = psPos s + kwLen })
    else Left (ParseError (psPos s) ("Expected keyword " <> kw <> " but got '" <> T.take kwLen (psInput s) <> "'"))

-- Try to match a keyword, returning True if matched.
pTryKeyword :: Text -> Parser Bool
pTryKeyword kw s = case pKeyword kw s of
  Right ((), s') -> Right (True, s')
  Left _         -> Right (False, s)

pSymbol :: Char -> Parser ()
pSymbol c s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just (ch, rest) | ch == c -> Right ((), s { psInput = rest, psPos = psPos s + 1 })
    Just (ch, _) -> Left (ParseError (psPos s) ("Expected '" <> T.singleton c <> "' but got '" <> T.singleton ch <> "'"))
    Nothing -> Left (ParseError (psPos s) ("Expected '" <> T.singleton c <> "' but got end of input"))

pTrySymbol :: Text -> Parser Bool
pTrySymbol sym s0 = do
  let s = skipWhitespace s0
      symLen = T.length sym
      (prefix, rest) = T.splitAt symLen (psInput s)
  if prefix == sym
    then Right (True, s { psInput = rest, psPos = psPos s + symLen })
    else Right (False, s)

-- Parse a comma-separated list with at least one element.
pCommaSep :: Parser a -> Parser [a]
pCommaSep p s0 = do
  (first, s1) <- p s0
  go [first] s1
  where
    go acc s = case pSymbol ',' s of
      Right ((), s') -> do
        (x, s'') <- p s'
        go (x : acc) s''
      Left _ -> Right (reverse acc, s)

-- Parse an identifier (unquoted or double-quoted).
pIdentifier :: Parser Text
pIdentifier s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('"', rest) -> pQuotedIdent (s { psInput = rest, psPos = psPos s + 1 })
    Just (c, _) | isAlpha c || c == '_' -> pBareIdent s
    Just (c, _) -> Left (ParseError (psPos s) ("Expected identifier but got '" <> T.singleton c <> "'"))
    Nothing -> Left (ParseError (psPos s) "Expected identifier but got end of input")
  where
    pBareIdent st =
      let (ident, rest) = T.span (\c -> isAlphaNum c || c == '_') (psInput st)
      in if isReserved (T.toLower ident)
         then Left (ParseError (psPos st) ("Reserved keyword '" <> ident <> "' cannot be used as identifier"))
         else Right (T.toLower ident, st { psInput = rest, psPos = psPos st + T.length ident })

    pQuotedIdent st =
      let (ident, rest) = T.break (== '"') (psInput st)
      in case T.uncons rest of
        Just ('"', rest') -> Right (ident, st { psInput = rest', psPos = psPos st + T.length ident + 1 })
        _ -> Left (ParseError (psPos st) "Unterminated quoted identifier")

    isReserved w = Set.member w reservedWords

reservedWords :: Set.Set Text
reservedWords = Set.fromList
  [ "select", "from", "where", "insert", "into", "values"
  , "update", "set", "delete", "create", "drop", "table"
  , "and", "or", "not", "null", "is", "true", "false"
  , "int", "integer", "bigint", "float", "double", "text"
  , "varchar", "boolean", "bool", "bytea", "precision"
  , "order", "by", "limit", "offset", "asc", "desc"
  , "begin", "commit", "rollback"
  , "distinct", "as", "count", "sum", "avg", "min", "max"
  , "like", "ilike", "in", "alter", "add", "column", "explain"
  , "between"
  , "join", "inner", "left", "right", "cross", "outer", "on", "full"
  , "group", "having"
  ]

-- Statements

pStatement :: Parser Statement
pStatement s0 = do
  let s = skipWhitespace s0
      remaining = T.toLower (T.take 10 (psInput s))
  if T.isPrefixOf "explain" remaining then pExplain s
  else if T.isPrefixOf "create" remaining then pCreateTable s
  else if T.isPrefixOf "drop" remaining then pDropTable s
  else if T.isPrefixOf "insert" remaining then pInsert s
  else if T.isPrefixOf "select" remaining then pSelect s
  else if T.isPrefixOf "update" remaining then pUpdate s
  else if T.isPrefixOf "delete" remaining then pDelete s
  else if T.isPrefixOf "alter" remaining then pAlterTable s
  else if T.isPrefixOf "begin" remaining then pBegin s
  else if T.isPrefixOf "commit" remaining then pCommit s
  else if T.isPrefixOf "rollback" remaining then pRollback s
  else pFail ("Expected SQL statement but got '" <> T.take 20 (psInput s) <> "'") s

pBegin :: Parser Statement
pBegin s0 = do
  ((), s1) <- pKeyword "BEGIN" s0
  Right (Begin, s1)

pCommit :: Parser Statement
pCommit s0 = do
  ((), s1) <- pKeyword "COMMIT" s0
  Right (Commit, s1)

pRollback :: Parser Statement
pRollback s0 = do
  ((), s1) <- pKeyword "ROLLBACK" s0
  Right (Rollback, s1)

-- EXPLAIN <statement>
pExplain :: Parser Statement
pExplain s0 = do
  ((), s1) <- pKeyword "EXPLAIN" s0
  (stmt, s2) <- pStatement s1
  Right (Explain stmt, s2)

-- CREATE TABLE name (col1 type1 [NOT NULL], ...)
pCreateTable :: Parser Statement
pCreateTable s0 = do
  ((), s1) <- pKeyword "CREATE" s0
  ((), s2) <- pKeyword "TABLE" s1
  (name, s3) <- pIdentifier s2
  ((), s4) <- pSymbol '(' s3
  (cols, s5) <- pCommaSep pColumnDef s4
  ((), s6) <- pSymbol ')' s5
  Right (CreateTable name cols, s6)

pColumnDef :: Parser ColumnDef
pColumnDef s0 = do
  (name, s1) <- pIdentifier s0
  (typ, s2) <- pSqlType s1
  (notNull, s3) <- pTryKeyword "NOT" s2
  s4 <- if notNull
        then do { ((), s') <- pKeyword "NULL" s3; Right s' }
        else Right s3
  Right (ColumnDef name typ (not notNull), s4)

pSqlType :: Parser SqlType
pSqlType s0 = do
  let s = skipWhitespace s0
  -- Try multi-word types first
  case pKeyword "DOUBLE" s of
    Right ((), s1) -> do
      -- DOUBLE PRECISION
      (_, s2) <- pTryKeyword "PRECISION" s1
      Right (SqlFloat, s2)
    Left _ -> pSingleWordType s
  where
    pSingleWordType st = do
      -- Consume the type name as a word
      let (word, rest) = T.span (\c -> isAlphaNum c || c == '_') (psInput st)
      if T.null word
        then pFail "Expected type name" st
        else let lw = T.toLower word
                 st' = st { psInput = rest, psPos = psPos st + T.length word }
             in case lw of
               "int"     -> Right (SqlInt, st')
               "integer" -> Right (SqlInt, st')
               "bigint"  -> Right (SqlBigInt, st')
               "float"   -> Right (SqlFloat, st')
               "text"    -> Right (SqlText, st')
               "varchar" -> Right (SqlText, st')
               "boolean" -> Right (SqlBool, st')
               "bool"    -> Right (SqlBool, st')
               "bytea"   -> Right (SqlBytea, st')
               _         -> pFail ("Unsupported type: " <> word) st

-- DROP TABLE name
pDropTable :: Parser Statement
pDropTable s0 = do
  ((), s1) <- pKeyword "DROP" s0
  ((), s2) <- pKeyword "TABLE" s1
  (name, s3) <- pIdentifier s2
  Right (DropTable name, s3)

-- ALTER TABLE name ADD COLUMN colname type [NOT NULL]
pAlterTable :: Parser Statement
pAlterTable s0 = do
  ((), s1) <- pKeyword "ALTER" s0
  ((), s2) <- pKeyword "TABLE" s1
  (name, s3) <- pIdentifier s2
  ((), s4) <- pKeyword "ADD" s3
  ((), s5) <- pKeyword "COLUMN" s4
  (colDef, s6) <- pColumnDef s5
  Right (AlterTableAddColumn name colDef, s6)

-- INSERT INTO name (cols) VALUES (vals), ...
pInsert :: Parser Statement
pInsert s0 = do
  ((), s1) <- pKeyword "INSERT" s0
  ((), s2) <- pKeyword "INTO" s1
  (name, s3) <- pIdentifier s2
  ((), s4) <- pSymbol '(' s3
  (cols, s5) <- pCommaSep pIdentifier s4
  ((), s6) <- pSymbol ')' s5
  ((), s7) <- pKeyword "VALUES" s6
  (rows, s8) <- pCommaSep pValueRow s7
  Right (Insert name cols rows, s8)

pValueRow :: Parser [Literal]
pValueRow s0 = do
  ((), s1) <- pSymbol '(' s0
  (vals, s2) <- pCommaSep pLiteral s1
  ((), s3) <- pSymbol ')' s2
  Right (vals, s3)

pLiteral :: Parser Literal
pLiteral s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('\'', _) -> pStringLit s
    Just (c, _)
      | isDigit c || c == '-' -> pNumLit s
    _ -> do
      -- try TRUE, FALSE, NULL as keywords
      (isTrue, s1) <- pTryKeyword "TRUE" s
      if isTrue then Right (LitBool True, s1)
      else do
        (isFalse, s2) <- pTryKeyword "FALSE" s
        if isFalse then Right (LitBool False, s2)
        else do
          (isNull, s3) <- pTryKeyword "NULL" s
          if isNull then Right (LitNull, s3)
          else pFail "Expected literal value" s

pStringLit :: Parser Literal
pStringLit s0 = do
  (_, s1) <- pSatisfy (== '\'') "opening quote" s0
  go T.empty s1
  where
    go acc s = case T.uncons (psInput s) of
      Nothing -> pFail "Unterminated string literal" s
      Just ('\'', rest) ->
        -- Check for escaped quote ('')
        case T.uncons rest of
          Just ('\'', rest') ->
            go (acc <> "'") (s { psInput = rest', psPos = psPos s + 2 })
          _ ->
            Right (LitText acc, s { psInput = rest, psPos = psPos s + 1 })
      Just (c, rest) ->
        go (T.snoc acc c) (s { psInput = rest, psPos = psPos s + 1 })

pNumLit :: Parser Literal
pNumLit s0 = do
  let s = skipWhitespace s0
  -- optional minus
  (neg, s1) <- case T.uncons (psInput s) of
    Just ('-', rest) -> Right (True, s { psInput = rest, psPos = psPos s + 1 })
    _                -> Right (False, s)
  let (digits, rest) = T.span isDigit (psInput s1)
  if T.null digits
    then pFail "Expected number" s1
    else do
      let s2 = s1 { psInput = rest, psPos = psPos s1 + T.length digits }
      -- Check for decimal point
      case T.uncons rest of
        Just ('.', rest') -> do
          let (frac, rest'') = T.span isDigit rest'
          let s3 = s2 { psInput = rest'', psPos = psPos s2 + 1 + T.length frac }
              numStr = (if neg then "-" else "") <> digits <> "." <> frac
          case TR.double numStr of
            Right (d, leftover) | T.null leftover -> Right (LitFloat d, s3)
            _         -> pFail ("Invalid number: " <> numStr) s0
        _ ->
          case TR.decimal digits of
            Right (n, leftover) | T.null leftover ->
              let val = if neg then negate n else n
              in Right (LitInt val, s2)
            _ -> pFail ("Invalid number: " <> digits) s0

-- SELECT [DISTINCT] cols FROM table [WHERE expr] [ORDER BY ...] [LIMIT n] [OFFSET n]
pSelect :: Parser Statement
pSelect s0 = do
  ((), s1) <- pKeyword "SELECT" s0
  (isDistinct, s2) <- pTryKeyword "DISTINCT" s1
  (targets, s3) <- pSelectTargets s2
  ((), s4) <- pKeyword "FROM" s3
  (fromClause, s5) <- pFromClause s4
  (wh, s6) <- pOptional pWhere s5
  (mGroupBy, s7) <- pOptional pGroupBy s6
  (mHaving, s8) <- pOptional pHaving s7
  (mOrderBy, s9) <- pOptional pOrderBy s8
  (mLimit, s10) <- pOptional pLimit s9
  (mOffset, s11) <- pOptional pOffset s10
  let orderClauses = maybe [] id mOrderBy
      groupByExprs = maybe [] id mGroupBy
  Right (Select isDistinct targets fromClause wh groupByExprs mHaving orderClauses mLimit mOffset, s11)

pGroupBy :: Parser [Expr]
pGroupBy s0 = do
  ((), s1) <- pKeyword "GROUP" s0
  ((), s2) <- pKeyword "BY" s1
  pCommaSep pExpr s2

pHaving :: Parser Expr
pHaving s0 = do
  ((), s1) <- pKeyword "HAVING" s0
  pExpr s1

pOrderBy :: Parser [OrderByClause]
pOrderBy s0 = do
  ((), s1) <- pKeyword "ORDER" s0
  ((), s2) <- pKeyword "BY" s1
  pCommaSep pOrderByClause s2

pOrderByClause :: Parser OrderByClause
pOrderByClause s0 = do
  (col, s1) <- pIdentifier s0
  (isDesc, s2) <- pTryKeyword "DESC" s1
  if isDesc
    then Right (OrderByClause col Desc, s2)
    else do
      (_, s3) <- pTryKeyword "ASC" s2
      Right (OrderByClause col Asc, s3)

pLimit :: Parser Int
pLimit s0 = do
  ((), s1) <- pKeyword "LIMIT" s0
  (lit, s2) <- pNumLit s1
  case lit of
    LitInt n | n >= 0 -> Right (fromIntegral n, s2)
    _ -> pFail "LIMIT must be a non-negative integer" s0

pOffset :: Parser Int
pOffset s0 = do
  ((), s1) <- pKeyword "OFFSET" s0
  (lit, s2) <- pNumLit s1
  case lit of
    LitInt n | n >= 0 -> Right (fromIntegral n, s2)
    _ -> pFail "OFFSET must be a non-negative integer" s0

pSelectTargets :: Parser [SelectTarget]
pSelectTargets s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('*', rest) -> Right ([Star], s { psInput = rest, psPos = psPos s + 1 })
    _ -> pCommaSep pSelectColumn s

pSelectColumn :: Parser SelectTarget
pSelectColumn s0 = do
  (expr, s1) <- pExpr s0
  (mAlias, s2) <- pOptional pAlias s1
  Right (SelExpr expr mAlias, s2)

pAlias :: Parser Text
pAlias s0 = do
  ((), s1) <- pKeyword "AS" s0
  pIdentifier s1

pFromClause :: Parser FromClause
pFromClause s0 = do
  (left, s1) <- pFromSingle s0
  pJoinRest left s1

pFromSingle :: Parser FromClause
pFromSingle s0 = do
  (name, s1) <- pIdentifier s0
  -- Try AS alias
  case pKeyword "AS" s1 of
    Right ((), s2) -> do
      (alias, s3) <- pIdentifier s2
      Right (FromTable name (Just alias), s3)
    Left _ ->
      -- Try bare alias (non-keyword identifier)
      case pIdentifier s1 of
        Right (alias, s2) -> Right (FromTable name (Just alias), s2)
        Left _ -> Right (FromTable name Nothing, s1)

pJoinRest :: FromClause -> Parser FromClause
pJoinRest left s0 = do
  let s = skipWhitespace s0
  -- Try CROSS JOIN
  case pTryKeyword "CROSS" s of
    Right (True, s1) -> do
      ((), s2) <- pKeyword "JOIN" s1
      (right, s3) <- pFromSingle s2
      pJoinRest (FromJoin CrossJoin left right Nothing) s3
    _ ->
      -- Try LEFT [OUTER] JOIN
      case pTryKeyword "LEFT" s of
        Right (True, s1) -> do
          (_, s2) <- pTryKeyword "OUTER" s1
          ((), s3) <- pKeyword "JOIN" s2
          (right, s4) <- pFromSingle s3
          ((), s5) <- pKeyword "ON" s4
          (cond, s6) <- pExpr s5
          pJoinRest (FromJoin LeftJoin left right (Just cond)) s6
        _ ->
          -- Try RIGHT [OUTER] JOIN
          case pTryKeyword "RIGHT" s of
            Right (True, s1) -> do
              (_, s2) <- pTryKeyword "OUTER" s1
              ((), s3) <- pKeyword "JOIN" s2
              (right, s4) <- pFromSingle s3
              ((), s5) <- pKeyword "ON" s4
              (cond, s6) <- pExpr s5
              pJoinRest (FromJoin RightJoin left right (Just cond)) s6
            _ ->
              -- Try [INNER] JOIN
              case pTryKeyword "INNER" s of
                Right (True, s1) -> do
                  ((), s2) <- pKeyword "JOIN" s1
                  (right, s3) <- pFromSingle s2
                  ((), s4) <- pKeyword "ON" s3
                  (cond, s5) <- pExpr s4
                  pJoinRest (FromJoin InnerJoin left right (Just cond)) s5
                _ ->
                  case pTryKeyword "JOIN" s of
                    Right (True, s1) -> do
                      (right, s2) <- pFromSingle s1
                      ((), s3) <- pKeyword "ON" s2
                      (cond, s4) <- pExpr s3
                      pJoinRest (FromJoin InnerJoin left right (Just cond)) s4
                    _ -> Right (left, s)

-- Try to parse an aggregate function. Returns (Just agg, advanced state) on
-- match, or (Nothing, original state) on no match.
pTryAgg :: PState -> Either ParseError (Maybe AggFunc, PState)
pTryAgg s0 = do
  let s = skipWhitespace s0
  case pTryKeyword "COUNT" s of
    Right (True, s1) -> do
      ((), s2) <- pSymbol '(' s1
      let s2' = skipWhitespace s2
      case T.uncons (psInput s2') of
        Just ('*', rest) -> do
          let s3 = s2' { psInput = rest, psPos = psPos s2' + 1 }
          ((), s4) <- pSymbol ')' s3
          Right (Just AggCount, s4)
        _ -> do
          (col, s3) <- pIdentifier s2
          ((), s4) <- pSymbol ')' s3
          Right (Just (AggCountCol col), s4)
    _ -> case pTryKeyword "SUM" s of
      Right (True, s1) -> parseAggWithCol AggSum s1
      _ -> case pTryKeyword "AVG" s of
        Right (True, s1) -> parseAggWithCol AggAvg s1
        _ -> case pTryKeyword "MIN" s of
          Right (True, s1) -> parseAggWithCol AggMin s1
          _ -> case pTryKeyword "MAX" s of
            Right (True, s1) -> parseAggWithCol AggMax s1
            _ -> Right (Nothing, s0)

parseAggWithCol :: (Text -> AggFunc) -> PState -> Either ParseError (Maybe AggFunc, PState)
parseAggWithCol mkAgg s1 = do
  ((), s2) <- pSymbol '(' s1
  (col, s3) <- pIdentifier s2
  ((), s4) <- pSymbol ')' s3
  Right (Just (mkAgg col), s4)

-- UPDATE table SET col = expr, ... [WHERE expr]
pUpdate :: Parser Statement
pUpdate s0 = do
  ((), s1) <- pKeyword "UPDATE" s0
  (name, s2) <- pIdentifier s1
  ((), s3) <- pKeyword "SET" s2
  (assignments, s4) <- pCommaSep pAssignment s3
  (wh, s5) <- pOptional pWhere s4
  Right (Update name assignments wh, s5)

pAssignment :: Parser (Text, Expr)
pAssignment s0 = do
  (col, s1) <- pIdentifier s0
  ((), s2) <- pSymbol '=' s1
  (val, s3) <- pExpr s2
  Right ((col, val), s3)

-- DELETE FROM table [WHERE expr]
pDelete :: Parser Statement
pDelete s0 = do
  ((), s1) <- pKeyword "DELETE" s0
  ((), s2) <- pKeyword "FROM" s1
  (name, s3) <- pIdentifier s2
  (wh, s4) <- pOptional pWhere s3
  Right (Delete name wh, s4)

-- WHERE clause
pWhere :: Parser Expr
pWhere s0 = do
  ((), s1) <- pKeyword "WHERE" s0
  pExpr s1

-- Expression parser with precedence: OR < AND < NOT < comparison < atom
pExpr :: Parser Expr
pExpr = pOr

pOr :: Parser Expr
pOr s0 = do
  (left, s1) <- pAnd s0
  pOrRest left s1
  where
    pOrRest left s = do
      (isOr, s1) <- pTryKeyword "OR" s
      if isOr
        then do
          (right, s2) <- pAnd s1
          pOrRest (ExprBinOp OpOr left right) s2
        else Right (left, s)

pAnd :: Parser Expr
pAnd s0 = do
  (left, s1) <- pNot s0
  pAndRest left s1
  where
    pAndRest left s = do
      (isAnd, s1) <- pTryKeyword "AND" s
      if isAnd
        then do
          (right, s2) <- pNot s1
          pAndRest (ExprBinOp OpAnd left right) s2
        else Right (left, s)

pNot :: Parser Expr
pNot s0 = do
  (isNot, s1) <- pTryKeyword "NOT" s0
  if isNot
    then do
      (e, s2) <- pNot s1
      Right (ExprNot e, s2)
    else pComparison s0

pComparison :: Parser Expr
pComparison s0 = do
  (left, s1) <- pAddSub s0
  let s1' = skipWhitespace s1
  -- Try IS NULL / IS NOT NULL
  case pTryKeyword "IS" s1' of
    Right (True, s2) -> do
      (isNot, s3) <- pTryKeyword "NOT" s2
      ((), s4) <- pKeyword "NULL" s3
      if isNot
        then Right (ExprIsNotNull left, s4)
        else Right (ExprIsNull left, s4)
    _ -> do
      -- Try BETWEEN lo AND hi
      case pTryKeyword "BETWEEN" s1' of
        Right (True, s2) -> do
          (lo, s3) <- pAddSub s2
          ((), s4) <- pKeyword "AND" s3
          (hi, s5) <- pAddSub s4
          Right (ExprBetween left lo hi, s5)
        _ -> do
          -- Try LIKE / ILIKE
          case pTryKeyword "LIKE" s1' of
            Right (True, s2) -> do
              (right, s3) <- pAddSub s2
              Right (ExprBinOp OpLike left right, s3)
            _ -> case pTryKeyword "ILIKE" s1' of
              Right (True, s2) -> do
                (right, s3) <- pAddSub s2
                Right (ExprBinOp OpILike left right, s3)
              _ ->
                -- Try IN (val1, val2, ...) or IN (SELECT ...)
                case pTryKeyword "IN" s1' of
                  Right (True, s2) -> do
                    ((), s3) <- pSymbol '(' s2
                    -- Peek for SELECT keyword
                    let s3' = skipWhitespace s3
                    case pTryKeyword "SELECT" s3' of
                      Right (True, _) -> do
                        -- Parse subquery
                        (subStmt, s4) <- pStatement s3
                        ((), s5) <- pSymbol ')' s4
                        Right (ExprIn left (InSubquery subStmt), s5)
                      _ -> do
                        (exprs, s4) <- pCommaSep pExpr s3
                        ((), s5) <- pSymbol ')' s4
                        Right (ExprIn left (InList exprs), s5)
                  _ -> do
                    -- Try comparison operators
                    case pCompOp s1' of
                      Right (op, s2) -> do
                        (right, s3) <- pAddSub s2
                        Right (ExprBinOp op left right, s3)
                      Left _ -> Right (left, s1')

pAddSub :: Parser Expr
pAddSub s0 = do
  (left, s1) <- pMulDiv s0
  pAddSubRest left s1
  where
    pAddSubRest left s =
      let s' = skipWhitespace s
      in case T.uncons (psInput s') of
        Just ('+', rest) ->
          let s2 = s' { psInput = rest, psPos = psPos s' + 1 }
          in do (right, s3) <- pMulDiv s2
                pAddSubRest (ExprBinOp OpAdd left right) s3
        Just ('-', rest) ->
          -- Only treat as subtraction if not followed by a digit (ambiguity with negative literals)
          -- Actually, at this point the left side is already parsed, so '-' is subtraction
          let s2 = s' { psInput = rest, psPos = psPos s' + 1 }
          in do (right, s3) <- pMulDiv s2
                pAddSubRest (ExprBinOp OpSub left right) s3
        _ -> Right (left, s)

pMulDiv :: Parser Expr
pMulDiv s0 = do
  (left, s1) <- pUnary s0
  pMulDivRest left s1
  where
    pMulDivRest left s =
      let s' = skipWhitespace s
      in case T.uncons (psInput s') of
        Just ('*', rest) ->
          let s2 = s' { psInput = rest, psPos = psPos s' + 1 }
          in do (right, s3) <- pUnary s2
                pMulDivRest (ExprBinOp OpMul left right) s3
        Just ('/', rest) ->
          let s2 = s' { psInput = rest, psPos = psPos s' + 1 }
          in do (right, s3) <- pUnary s2
                pMulDivRest (ExprBinOp OpDiv left right) s3
        Just ('%', rest) ->
          let s2 = s' { psInput = rest, psPos = psPos s' + 1 }
          in do (right, s3) <- pUnary s2
                pMulDivRest (ExprBinOp OpMod left right) s3
        _ -> Right (left, s)

pUnary :: Parser Expr
pUnary s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('-', rest) ->
      let s1 = s { psInput = rest, psPos = psPos s + 1 }
      in do (e, s2) <- pUnary s1
            Right (ExprBinOp OpSub (ExprLit (LitInt 0)) e, s2)
    _ -> pAtom s0

pCompOp :: Parser BinOp
pCompOp s0 = do
  let s = skipWhitespace s0
  -- Try two-char operators first
  case pTrySymbol "<>" s of
    Right (True, s') -> Right (OpNeq, s')
    _ -> case pTrySymbol "!=" s of
      Right (True, s') -> Right (OpNeq, s')
      _ -> case pTrySymbol "<=" s of
        Right (True, s') -> Right (OpLte, s')
        _ -> case pTrySymbol ">=" s of
          Right (True, s') -> Right (OpGte, s')
          _ -> case T.uncons (psInput s) of
            Just ('=', rest) -> Right (OpEq, s { psInput = rest, psPos = psPos s + 1 })
            Just ('<', rest) -> Right (OpLt, s { psInput = rest, psPos = psPos s + 1 })
            Just ('>', rest) -> Right (OpGt, s { psInput = rest, psPos = psPos s + 1 })
            _ -> pFail "Expected comparison operator" s

pAtom :: Parser Expr
pAtom s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('(', _) -> do
      ((), s1) <- pSymbol '(' s
      (e, s2) <- pExpr s1
      ((), s3) <- pSymbol ')' s2
      Right (e, s3)
    Just ('\'', _) -> do
      (lit, s1) <- pStringLit s
      Right (ExprLit lit, s1)
    Just (c, _)
      | isDigit c -> do
          (lit, s1) <- pNumLit s
          Right (ExprLit lit, s1)
    _ -> do
      -- Try TRUE, FALSE, NULL keywords
      (isTrue, s1) <- pTryKeyword "TRUE" s
      if isTrue then Right (ExprLit (LitBool True), s1)
      else do
        (isFalse, s2) <- pTryKeyword "FALSE" s
        if isFalse then Right (ExprLit (LitBool False), s2)
        else do
          (isNull, s3) <- pTryKeyword "NULL" s
          if isNull then Right (ExprLit LitNull, s3)
          else do
            -- Try aggregate functions
            case pTryAgg s of
              Right (Just agg, s4) -> Right (ExprAgg agg, s4)
              _ -> do
                -- Must be a column reference, possibly qualified
                (name, s4) <- pIdentifier s
                -- Check for dot (qualified reference)
                case T.uncons (psInput s4) of
                  Just ('.', rest) ->
                    let s5 = s4 { psInput = rest, psPos = psPos s4 + 1 }
                    in do (col, s6) <- pIdentifier s5
                          Right (ExprQualColumn name col, s6)
                  _ -> Right (ExprColumn name, s4)
