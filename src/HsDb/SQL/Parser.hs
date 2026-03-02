{-# LANGUAGE OverloadedStrings #-}

module HsDb.SQL.Parser
  ( parseSQL
  , ParseError(..)
  ) where

import Data.Char (isAlphaNum, isAlpha, isDigit, isSpace)
import Data.Text (Text)
import qualified Data.Text as T

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
        go (acc ++ [x]) s''
      Left _ -> Right (acc, s)

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

    isReserved w = w `elem`
      [ "select", "from", "where", "insert", "into", "values"
      , "update", "set", "delete", "create", "drop", "table"
      , "and", "or", "not", "null", "is", "true", "false"
      , "int", "integer", "bigint", "float", "double", "text"
      , "varchar", "boolean", "bool", "bytea", "precision"
      ]

-- Statements

pStatement :: Parser Statement
pStatement s0 = do
  let s = skipWhitespace s0
      remaining = T.toLower (T.take 10 (psInput s))
  if T.isPrefixOf "create" remaining then pCreateTable s
  else if T.isPrefixOf "drop" remaining then pDropTable s
  else if T.isPrefixOf "insert" remaining then pInsert s
  else if T.isPrefixOf "select" remaining then pSelect s
  else if T.isPrefixOf "update" remaining then pUpdate s
  else if T.isPrefixOf "delete" remaining then pDelete s
  else pFail ("Expected SQL statement but got '" <> T.take 20 (psInput s) <> "'") s

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
          case reads (T.unpack numStr) of
            [(d, "")] -> Right (LitFloat d, s3)
            _         -> pFail ("Invalid number: " <> numStr) s0
        _ -> do
          let n = read (T.unpack digits)
              val = if neg then negate n else n
          Right (LitInt val, s2)

-- SELECT cols FROM table [WHERE expr]
pSelect :: Parser Statement
pSelect s0 = do
  ((), s1) <- pKeyword "SELECT" s0
  (targets, s2) <- pSelectTargets s1
  ((), s3) <- pKeyword "FROM" s2
  (name, s4) <- pIdentifier s3
  (wh, s5) <- pOptional (pWhere) s4
  Right (Select targets name wh, s5)

pSelectTargets :: Parser [SelectTarget]
pSelectTargets s0 = do
  let s = skipWhitespace s0
  case T.uncons (psInput s) of
    Just ('*', rest) -> Right ([Star], s { psInput = rest, psPos = psPos s + 1 })
    _ -> pCommaSep pSelectColumn s

pSelectColumn :: Parser SelectTarget
pSelectColumn s = do
  (name, s') <- pIdentifier s
  Right (Column name, s')

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

-- Expression parser with precedence: OR < AND < comparison < atom
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
  (left, s1) <- pComparison s0
  pAndRest left s1
  where
    pAndRest left s = do
      (isAnd, s1) <- pTryKeyword "AND" s
      if isAnd
        then do
          (right, s2) <- pComparison s1
          pAndRest (ExprBinOp OpAnd left right) s2
        else Right (left, s)

pComparison :: Parser Expr
pComparison s0 = do
  (left, s1) <- pAtom s0
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
      -- Try comparison operators
      case pCompOp s1' of
        Right (op, s2) -> do
          (right, s3) <- pAtom s2
          Right (ExprBinOp op left right, s3)
        Left _ -> Right (left, s1')

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
      | isDigit c || c == '-' -> do
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
            -- Must be a column reference
            (name, s4) <- pIdentifier s
            Right (ExprColumn name, s4)
