{-# LANGUAGE OverloadedStrings #-}

module Test.HsDb.SQL.Parser (parserTests) where

import Hedgehog
import HsDb.SQL.Parser
import HsDb.SQL.Types as SQL

parserTests :: Group
parserTests = Group "SQL.Parser"
  [ ("prop_create_table", prop_create_table)
  , ("prop_drop_table", prop_drop_table)
  , ("prop_insert_single", prop_insert_single)
  , ("prop_insert_multi_row", prop_insert_multi_row)
  , ("prop_select_star", prop_select_star)
  , ("prop_select_columns", prop_select_columns)
  , ("prop_select_where", prop_select_where)
  , ("prop_update_set", prop_update_set)
  , ("prop_delete", prop_delete)
  , ("prop_case_insensitive", prop_case_insensitive)
  , ("prop_where_and_or", prop_where_and_or)
  , ("prop_where_is_null", prop_where_is_null)
  , ("prop_where_is_not_null", prop_where_is_not_null)
  , ("prop_comparison_ops", prop_comparison_ops)
  , ("prop_string_escape", prop_string_escape)
  , ("prop_negative_number", prop_negative_number)
  , ("prop_float_literal", prop_float_literal)
  , ("prop_quoted_identifier", prop_quoted_identifier)
  , ("prop_not_null_constraint", prop_not_null_constraint)
  , ("prop_trailing_semicolon", prop_trailing_semicolon)
  , ("prop_order_by_single", prop_order_by_single)
  , ("prop_order_by_desc", prop_order_by_desc)
  , ("prop_order_by_multi", prop_order_by_multi)
  , ("prop_limit", prop_limit)
  , ("prop_limit_offset", prop_limit_offset)
  , ("prop_order_by_limit", prop_order_by_limit)
  , ("prop_where_order_limit_offset", prop_where_order_limit_offset)
  , ("prop_err_empty_input", prop_err_empty_input)
  , ("prop_err_unknown_type", prop_err_unknown_type)
  , ("prop_err_reserved_as_ident", prop_err_reserved_as_ident)
  , ("prop_err_missing_from", prop_err_missing_from)
  , ("prop_err_unterminated_string", prop_err_unterminated_string)
  , ("prop_select_nonexistent_column_parses", prop_select_nonexistent_column_parses)
  , ("prop_select_distinct", prop_select_distinct)
  , ("prop_select_as_alias", prop_select_as_alias)
  , ("prop_select_count_star", prop_select_count_star)
  , ("prop_select_count_col", prop_select_count_col)
  , ("prop_select_sum", prop_select_sum)
  , ("prop_select_avg_with_alias", prop_select_avg_with_alias)
  , ("prop_where_like", prop_where_like)
  , ("prop_where_ilike", prop_where_ilike)
  , ("prop_where_in", prop_where_in)
  , ("prop_where_not", prop_where_not)
  , ("prop_alter_table_add_column", prop_alter_table_add_column)
  , ("prop_explain_select", prop_explain_select)
  , ("prop_arithmetic_add", prop_arithmetic_add)
  , ("prop_arithmetic_precedence", prop_arithmetic_precedence)
  , ("prop_between", prop_between)
  , ("prop_table_alias_bare", prop_table_alias_bare)
  , ("prop_table_alias_as", prop_table_alias_as)
  , ("prop_qualified_column", prop_qualified_column)
  , ("prop_inner_join", prop_inner_join)
  , ("prop_left_join", prop_left_join)
  , ("prop_cross_join", prop_cross_join)
  , ("prop_group_by", prop_group_by)
  , ("prop_group_by_having", prop_group_by_having)
  , ("prop_in_subquery", prop_in_subquery)
  ]

expectRight :: (MonadTest m, Show a) => Either ParseError a -> m a
expectRight (Right a) = return a
expectRight (Left e)  = do annotateShow e; failure

expectLeft :: (MonadTest m, Show a) => Either ParseError a -> m ParseError
expectLeft (Left e)  = return e
expectLeft (Right a) = do annotateShow a; failure

prop_create_table :: Property
prop_create_table = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "CREATE TABLE users (id INT NOT NULL, name TEXT)"
  stmt === CreateTable "users"
    [ ColumnDef "id" SqlInt False
    , ColumnDef "name" SqlText True
    ]

prop_drop_table :: Property
prop_drop_table = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "DROP TABLE users"
  stmt === DropTable "users"

prop_insert_single :: Property
prop_insert_single = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "INSERT INTO users (id, name) VALUES (1, 'hello')"
  stmt === Insert "users" ["id", "name"] [[LitInt 1, LitText "hello"]]

prop_insert_multi_row :: Property
prop_insert_multi_row = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL
    "INSERT INTO t (x) VALUES (1), (2), (3)"
  stmt === Insert "t" ["x"] [[LitInt 1], [LitInt 2], [LitInt 3]]

prop_select_star :: Property
prop_select_star = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM users"
  stmt === Select False [Star] (FromTable "users" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_columns :: Property
prop_select_columns = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT id, name FROM users"
  stmt === Select False [SelExpr (ExprColumn "id") Nothing, SelExpr (ExprColumn "name") Nothing] (FromTable "users" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_where :: Property
prop_select_where = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM users WHERE id = 1"
  stmt === Select False [Star] (FromTable "users" Nothing)
    (Just (ExprBinOp OpEq (ExprColumn "id") (ExprLit (LitInt 1))))
    [] Nothing [] Nothing Nothing

prop_update_set :: Property
prop_update_set = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "UPDATE users SET name = 'bob' WHERE id = 1"
  stmt === SQL.Update "users"
    [("name", ExprLit (LitText "bob"))]
    (Just (ExprBinOp OpEq (ExprColumn "id") (ExprLit (LitInt 1))))

prop_delete :: Property
prop_delete = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "DELETE FROM users WHERE id = 1"
  stmt === SQL.Delete "users"
    (Just (ExprBinOp OpEq (ExprColumn "id") (ExprLit (LitInt 1))))

prop_case_insensitive :: Property
prop_case_insensitive = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "select * from USERS"
  stmt === Select False [Star] (FromTable "users" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_where_and_or :: Property
prop_where_and_or = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE a = 1 AND b = 2 OR c = 3"
  -- OR has lower precedence than AND: (a=1 AND b=2) OR (c=3)
  stmt === Select False [Star] (FromTable "t" Nothing) (Just
    (ExprBinOp OpOr
      (ExprBinOp OpAnd
        (ExprBinOp OpEq (ExprColumn "a") (ExprLit (LitInt 1)))
        (ExprBinOp OpEq (ExprColumn "b") (ExprLit (LitInt 2))))
      (ExprBinOp OpEq (ExprColumn "c") (ExprLit (LitInt 3)))))
    [] Nothing [] Nothing Nothing

prop_where_is_null :: Property
prop_where_is_null = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x IS NULL"
  stmt === Select False [Star] (FromTable "t" Nothing) (Just (ExprIsNull (ExprColumn "x")))
    [] Nothing [] Nothing Nothing

prop_where_is_not_null :: Property
prop_where_is_not_null = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x IS NOT NULL"
  stmt === Select False [Star] (FromTable "t" Nothing) (Just (ExprIsNotNull (ExprColumn "x")))
    [] Nothing [] Nothing Nothing

prop_comparison_ops :: Property
prop_comparison_ops = withTests 1 $ property $ do
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x < 1"
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x > 1"
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x <= 1"
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x >= 1"
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x <> 1"
  _ <- expectRight $ parseSQL "SELECT * FROM t WHERE x != 1"
  success

prop_string_escape :: Property
prop_string_escape = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "INSERT INTO t (x) VALUES ('it''s')"
  stmt === Insert "t" ["x"] [[LitText "it's"]]

prop_negative_number :: Property
prop_negative_number = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "INSERT INTO t (x) VALUES (-42)"
  stmt === Insert "t" ["x"] [[LitInt (-42)]]

prop_float_literal :: Property
prop_float_literal = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "INSERT INTO t (x) VALUES (3.14)"
  stmt === Insert "t" ["x"] [[LitFloat 3.14]]

prop_quoted_identifier :: Property
prop_quoted_identifier = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT \"select\" FROM t"
  stmt === Select False [SelExpr (ExprColumn "select") Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_not_null_constraint :: Property
prop_not_null_constraint = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "CREATE TABLE t (x INT NOT NULL, y TEXT)"
  stmt === CreateTable "t"
    [ ColumnDef "x" SqlInt False
    , ColumnDef "y" SqlText True
    ]

prop_trailing_semicolon :: Property
prop_trailing_semicolon = withTests 1 $ property $ do
  -- Semicolons are not part of our grammar; they should cause a parse error
  _ <- expectLeft $ parseSQL "SELECT * FROM t;"
  success

-- Negative tests

prop_err_empty_input :: Property
prop_err_empty_input = withTests 1 $ property $ do
  _ <- expectLeft $ parseSQL ""
  success

prop_err_unknown_type :: Property
prop_err_unknown_type = withTests 1 $ property $ do
  _ <- expectLeft $ parseSQL "CREATE TABLE t (x DATETIME)"
  success

prop_err_reserved_as_ident :: Property
prop_err_reserved_as_ident = withTests 1 $ property $ do
  _ <- expectLeft $ parseSQL "CREATE TABLE select (x INT)"
  success

prop_err_missing_from :: Property
prop_err_missing_from = withTests 1 $ property $ do
  _ <- expectLeft $ parseSQL "SELECT * users"
  success

prop_err_unterminated_string :: Property
prop_err_unterminated_string = withTests 1 $ property $ do
  _ <- expectLeft $ parseSQL "INSERT INTO t (x) VALUES ('hello)"
  success

-- ORDER BY / LIMIT / OFFSET tests

prop_order_by_single :: Property
prop_order_by_single = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t ORDER BY x"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [OrderByClause "x" Asc] Nothing Nothing

prop_order_by_desc :: Property
prop_order_by_desc = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t ORDER BY x DESC"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [OrderByClause "x" Desc] Nothing Nothing

prop_order_by_multi :: Property
prop_order_by_multi = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t ORDER BY x DESC, y ASC"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing
    [OrderByClause "x" Desc, OrderByClause "y" Asc] Nothing Nothing

prop_limit :: Property
prop_limit = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t LIMIT 10"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [] (Just 10) Nothing

prop_limit_offset :: Property
prop_limit_offset = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t LIMIT 10 OFFSET 5"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [] (Just 10) (Just 5)

prop_order_by_limit :: Property
prop_order_by_limit = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t ORDER BY x LIMIT 5"
  stmt === Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [OrderByClause "x" Asc] (Just 5) Nothing

prop_where_order_limit_offset :: Property
prop_where_order_limit_offset = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x > 0 ORDER BY x DESC LIMIT 10 OFFSET 2"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBinOp OpGt (ExprColumn "x") (ExprLit (LitInt 0))))
    [] Nothing [OrderByClause "x" Desc] (Just 10) (Just 2)

-- This should parse fine (column resolution is at execution time, not parse time)
prop_select_nonexistent_column_parses :: Property
prop_select_nonexistent_column_parses = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT nonexistent FROM t"
  stmt === Select False [SelExpr (ExprColumn "nonexistent") Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

-- Phase 8 feature tests

prop_select_distinct :: Property
prop_select_distinct = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT DISTINCT x FROM t"
  stmt === Select True [SelExpr (ExprColumn "x") Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_as_alias :: Property
prop_select_as_alias = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT x AS foo, y AS bar FROM t"
  stmt === Select False [SelExpr (ExprColumn "x") (Just "foo"), SelExpr (ExprColumn "y") (Just "bar")] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_count_star :: Property
prop_select_count_star = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT COUNT(*) FROM t"
  stmt === Select False [SelExpr (ExprAgg AggCount) Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_count_col :: Property
prop_select_count_col = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT COUNT(x) FROM t"
  stmt === Select False [SelExpr (ExprAgg (AggCountCol "x")) Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_sum :: Property
prop_select_sum = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT SUM(x) FROM t"
  stmt === Select False [SelExpr (ExprAgg (AggSum "x")) Nothing] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_select_avg_with_alias :: Property
prop_select_avg_with_alias = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT AVG(x) AS average FROM t"
  stmt === Select False [SelExpr (ExprAgg (AggAvg "x")) (Just "average")] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing

prop_where_like :: Property
prop_where_like = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE name LIKE '%foo%'"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBinOp OpLike (ExprColumn "name") (ExprLit (LitText "%foo%"))))
    [] Nothing [] Nothing Nothing

prop_where_ilike :: Property
prop_where_ilike = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE name ILIKE '%foo%'"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBinOp OpILike (ExprColumn "name") (ExprLit (LitText "%foo%"))))
    [] Nothing [] Nothing Nothing

prop_where_in :: Property
prop_where_in = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x IN (1, 2, 3)"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprIn (ExprColumn "x") (InList [ExprLit (LitInt 1), ExprLit (LitInt 2), ExprLit (LitInt 3)])))
    [] Nothing [] Nothing Nothing

prop_where_not :: Property
prop_where_not = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE NOT x = 1"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprNot (ExprBinOp OpEq (ExprColumn "x") (ExprLit (LitInt 1)))))
    [] Nothing [] Nothing Nothing

prop_alter_table_add_column :: Property
prop_alter_table_add_column = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "ALTER TABLE t ADD COLUMN y TEXT"
  stmt === AlterTableAddColumn "t" (ColumnDef "y" SqlText True)

prop_explain_select :: Property
prop_explain_select = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "EXPLAIN SELECT * FROM t"
  stmt === Explain (Select False [Star] (FromTable "t" Nothing) Nothing [] Nothing [] Nothing Nothing)

prop_arithmetic_add :: Property
prop_arithmetic_add = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x + y > 10"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBinOp OpGt
      (ExprBinOp OpAdd (ExprColumn "x") (ExprColumn "y"))
      (ExprLit (LitInt 10))))
    [] Nothing [] Nothing Nothing

prop_arithmetic_precedence :: Property
prop_arithmetic_precedence = withTests 1 $ property $ do
  -- 2 + 3 * 4 should parse as 2 + (3 * 4)
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x = 2 + 3 * 4"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBinOp OpEq (ExprColumn "x")
      (ExprBinOp OpAdd (ExprLit (LitInt 2))
        (ExprBinOp OpMul (ExprLit (LitInt 3)) (ExprLit (LitInt 4))))))
    [] Nothing [] Nothing Nothing

prop_between :: Property
prop_between = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE x BETWEEN 1 AND 5"
  stmt === Select False [Star] (FromTable "t" Nothing)
    (Just (ExprBetween (ExprColumn "x") (ExprLit (LitInt 1)) (ExprLit (LitInt 5))))
    [] Nothing [] Nothing Nothing

prop_table_alias_bare :: Property
prop_table_alias_bare = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM demo d"
  stmt === Select False [Star] (FromTable "demo" (Just "d")) Nothing [] Nothing [] Nothing Nothing

prop_table_alias_as :: Property
prop_table_alias_as = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM demo AS d"
  stmt === Select False [Star] (FromTable "demo" (Just "d")) Nothing [] Nothing [] Nothing Nothing

prop_qualified_column :: Property
prop_qualified_column = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT d.x FROM demo d WHERE d.x > 0"
  stmt === Select False
    [SelExpr (ExprQualColumn "d" "x") Nothing]
    (FromTable "demo" (Just "d"))
    (Just (ExprBinOp OpGt (ExprQualColumn "d" "x") (ExprLit (LitInt 0))))
    [] Nothing [] Nothing Nothing

prop_inner_join :: Property
prop_inner_join = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
  stmt === Select False [Star]
    (FromJoin InnerJoin (FromTable "t1" Nothing) (FromTable "t2" Nothing)
      (Just (ExprBinOp OpEq (ExprQualColumn "t1" "id") (ExprQualColumn "t2" "id"))))
    Nothing [] Nothing [] Nothing Nothing

prop_left_join :: Property
prop_left_join = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"
  stmt === Select False [Star]
    (FromJoin LeftJoin (FromTable "t1" Nothing) (FromTable "t2" Nothing)
      (Just (ExprBinOp OpEq (ExprQualColumn "t1" "id") (ExprQualColumn "t2" "id"))))
    Nothing [] Nothing [] Nothing Nothing

prop_cross_join :: Property
prop_cross_join = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t1 CROSS JOIN t2"
  stmt === Select False [Star]
    (FromJoin CrossJoin (FromTable "t1" Nothing) (FromTable "t2" Nothing) Nothing)
    Nothing [] Nothing [] Nothing Nothing

prop_group_by :: Property
prop_group_by = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT status, COUNT(*) FROM t GROUP BY status"
  stmt === Select False
    [SelExpr (ExprColumn "status") Nothing, SelExpr (ExprAgg AggCount) Nothing]
    (FromTable "t" Nothing) Nothing [ExprColumn "status"] Nothing [] Nothing Nothing

prop_group_by_having :: Property
prop_group_by_having = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT status, COUNT(*) FROM t GROUP BY status HAVING COUNT(*) > 1"
  stmt === Select False
    [SelExpr (ExprColumn "status") Nothing, SelExpr (ExprAgg AggCount) Nothing]
    (FromTable "t" Nothing) Nothing [ExprColumn "status"]
    (Just (ExprBinOp OpGt (ExprAgg AggCount) (ExprLit (LitInt 1))))
    [] Nothing Nothing

prop_in_subquery :: Property
prop_in_subquery = withTests 1 $ property $ do
  stmt <- expectRight $ parseSQL "SELECT * FROM t WHERE id IN (SELECT id FROM t2)"
  case stmt of
    Select _ _ _ (Just (ExprIn _ (InSubquery _))) _ _ _ _ _ -> success
    _ -> do annotateShow stmt; failure
