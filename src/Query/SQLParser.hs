-----------------------------------------------------------------------------
-- |
-- Module      :  Database.TxtSushi.SQLParser
-- Copyright   :  (c) Keith Sheppard 2009-2010
-- License     :  BSD3
-- Maintainer  :  keithshep@gmail.com
-- Stability   :  experimental
-- Portability :  portable
--
-- Module for parsing SQL
--
-----------------------------------------------------------------------------

module Query.SQLParser (
    parseSelectStatement,
    allMaybeTableNames,
    TableExpression(..),
    ColumnIdentifier(..),
    ColumnSelection(..),
    Expression(..),
    SQLFunction(..),
    OrderByItem(..)) where

import Data.Char
import Data.List
import Text.ParserCombinators.Parsec
import Text.ParserCombinators.Parsec.Expr

import Query.ParseUtil
import Query.SQLExpression
import Query.SQLFunctionDefinitions

-- | Parses a SQL select statement
parseSelectStatement :: GenParser Char st TableExpression
parseSelectStatement = try (spaces >> parseToken "SELECT") >> parseSelectBody

-- | Parses all of the stuff that comes after "SELECT "
parseSelectBody :: GenParser Char st TableExpression
parseSelectBody = do
    columnVals <- parseColumnSelections
    -- TODO need a better error message for missing "ON" etc. in
    -- the from part, can do this by grabing "FROM" first
    maybeFrom <- maybeParseFromPart
    maybeWhere <- maybeParseWherePart
    groupByExprs <- maybeParseGroupByPart
    orderBy <- parseOrderByPart
    
    return TableExpression {
        columnSelections    = columnVals,
        maybeFromTable      = maybeFrom,
        maybeWhereFilter    = maybeWhere,
        orderByItems        = orderBy,
        maybeGroupByHaving  = groupByExprs}
    
    where
        maybeParseFromPart =
            ifParseThen (parseToken "FROM") parseTableIdentifier
        
        maybeParseWherePart =
            ifParseThen (parseToken "WHERE") parseExpression

-- | Parses the "ORDER BY ..." part of a select statement returning the list
--   of OrderByItem's that were parsed (this list will be empty if there is no
--   "ORDER BY" parsed
parseOrderByPart :: GenParser Char st [OrderByItem]
parseOrderByPart =
    ifParseThenElse
        -- if we see an "ORDER BY"
        (parseToken "ORDER")
        
        -- then parse the order expressions
        (parseToken "BY" >> sepByAtLeast 1 parseOrderByItem commaSeparator)
        
        -- else there is nothing to sort by
        (return [])
    
    where
        parseOrderByItem :: GenParser Char st OrderByItem
        parseOrderByItem = do
            orderExpr <- parseExpression
            isAscending <- ifParseThenElse
                -- if we parse "DESC"
                (try parseDescending)
                
                -- then return false, it isn't ascending
                (return False)
                
                -- else try to consume "ASC" but even if we don't it's still
                -- ascending so return true unconditionally
                ((parseAscending <|> return []) >> return True)
            
            return $ OrderByItem orderExpr isAscending
        
        parseAscending  = parseToken "ASCENDING" <|> parseToken "ASC"
        parseDescending = parseToken "DESCENDING" <|> parseToken "DESC"

maybeParseGroupByPart :: GenParser Char st (Maybe ([Expression], Maybe Expression))
maybeParseGroupByPart =
    ifParseThen
        -- if we see a "GROUP BY"
        (parseToken "GROUP")
        
        -- then parse the expressions
        (parseToken "BY" >> parseGroupBy)
    
    where
        parseGroupBy = do
            groupExprs <- atLeastOneExpr
            maybeHavingExpr <- ifParseThen (parseToken "HAVING") parseExpression
            return (groupExprs, maybeHavingExpr)

atLeastOneExpr :: GenParser Char st [Expression]
atLeastOneExpr = sepByAtLeast 1 parseExpression commaSeparator

--------------------------------------------------------------------------------
-- Functions for parsing the column names specified after "SELECT"
--------------------------------------------------------------------------------

parseColumnSelections :: GenParser Char st [ColumnSelection]
parseColumnSelections =
    sepBy1 parseAnyColType (try commaSeparator)
    where parseAnyColType =   parseAllCols 
                          <|> try parseAllColsFromTbl 
                          <|> try parseColExpression

parseAllCols :: GenParser Char st ColumnSelection
parseAllCols = parseToken "*" >> return AllColumns

parseAllColsFromTbl :: GenParser Char st ColumnSelection
parseAllColsFromTbl = do
    tableVal <- parseIdentifier
    _ <- string "." >> spaces >> parseToken "*"
    
    return $ AllColumnsFrom tableVal

parseColExpression :: GenParser Char st ColumnSelection
parseColExpression = do
    expr <- parseExpression
    maybeAlias <- maybeParseAlias
    
    return $ ExpressionColumn expr 

parseColumnId :: GenParser Char st ColumnIdentifier
parseColumnId = ColumnIdentifier <$> parseIdentifier

--------------------------------------------------------------------------------
-- Functions for parsing the table part (after "FROM")
--------------------------------------------------------------------------------

parseTableExpression :: GenParser Char a TableExpression
parseTableExpression =
    parenthesize parseTableExpression <?> "Table Expression"

parseTableIdentifier :: GenParser Char st TableIdentifier
parseTableIdentifier = TableIdentifier <$> parseIdentifier

maybeParseAlias :: GenParser Char st (Maybe String)
maybeParseAlias = ifParseThen (parseToken "AS") parseIdentifier

--------------------------------------------------------------------------------
-- Expression parsing: These can be after "SELECT", "WHERE" or "HAVING"
--------------------------------------------------------------------------------

parseExpression :: GenParser Char st Expression
parseExpression =
    let opTable = map (map parseInfixOp) infixFunctions
    in buildExpressionParser opTable parseAnyNonInfixExpression <?> "expression"

parseAnyNonInfixExpression :: GenParser Char st Expression
parseAnyNonInfixExpression =
    parseParenthesizedExpression <|>
    parseBoolConstant <|>
    parseStringConstant <|>
    try parseRealConstant <|>
    try parseIntConstant <|>
    parseAnyNormalFunction <|>
    parseNegateFunction <|>
    parseSubstringFunction <|>
    parseNotFunction <|>
    parseCountStar <|>
    ColumnExpression <$> parseColumnId 

parseParenthesizedExpression :: GenParser Char st Expression
parseParenthesizedExpression =
    parenthesize parseExpression >>=
    \e -> return e {stringRepresentation = "(" ++ stringRepresentation e ++ ")"}

parseBoolConstant :: GenParser Char st Expression
parseBoolConstant =
    (parseToken "TRUE" >>= return . BoolConstantExpression True) <|>
    (parseToken "FALSE" >>= return . BoolConstantExpression False)

parseStringConstant :: GenParser Char st Expression
parseStringConstant =
    (quotedText True '"' <|> quotedText True '\'') >>=
    \str -> return $ StringConstantExpression str ("'" ++ str ++ "'") -- TODO this quoting is not robust!

parseIntConstant :: GenParser Char st Expression
parseIntConstant = parseInt >>= \int -> return $ IntConstantExpression int (show int)

parseRealConstant :: GenParser Char st Expression
parseRealConstant =
    parseReal >>= \real -> return $ RealConstantExpression real (show real)

parseAnyNormalFunction :: GenParser Char st Expression
parseAnyNormalFunction =
    let allParsers = map parseNormalFunction normalSyntaxFunctions
    in choice allParsers

parseNormalFunction :: SQLFunction -> GenParser Char st Expression
parseNormalFunction sqlFunc =
    try (parseToken $ functionName sqlFunc) >>= parseNormalFunctionArgs sqlFunc

parseNormalFunctionArgs :: SQLFunction -> String -> GenParser Char st Expression
parseNormalFunctionArgs sqlFunc sqlFuncStr = do
    args <- parenthesize $ sepBy parseExpression commaSeparator
    return $ FunctionExpression sqlFunc args (sqlFuncStr ++ toArgListString args)
    where
        toArgListString argExprs =
            '(' : intercalate ", " (map expressionToString argExprs) ++ ")"

-- | This function parses the operator part of the infix function and returns
--   a function that excepts a left expression and right expression to form
--   an Expression from the FunctionExpression constructor
parseInfixOp :: SQLFunction -> Operator Char st Expression
parseInfixOp infixFunc =
    -- use the magic infix type, always assuming left associativity
    Infix opParser AssocLeft
    where
        opParser = parseToken (functionName infixFunc) >> return buildExpr
        buildExpr leftSubExpr rightSubExpr = FunctionExpression {
            sqlFunction = infixFunc,
            functionArguments = [leftSubExpr, rightSubExpr],
            stringRepresentation =
                expressionToString leftSubExpr ++ " " ++
                functionName infixFunc ++ " " ++
                expressionToString rightSubExpr}

parseSubstringFunction :: GenParser Char st Expression
parseSubstringFunction = do
    funcStr <- parseToken $ functionName substringFromFunction
    _ <- eatSpacesAfter $ char '('
    strExpr <- parseExpression
    fromStr <- parseToken "FROM"
    startExpr <- parseExpression
    maybeForStrAndLength <- preservingIfParseThen (parseToken "FOR") parseExpression
    _ <- eatSpacesAfter $ char ')'
    
    let funcStrStart =
            funcStr ++ "(" ++ expressionToString strExpr ++ " " ++
            fromStr ++ expressionToString startExpr
    
    return $ case maybeForStrAndLength of
        Nothing -> FunctionExpression
            substringFromFunction
            [strExpr, startExpr]
            (funcStrStart ++ ")")
        Just (forStr, lenExpr) -> FunctionExpression
            substringFromToFunction
            [strExpr, startExpr, lenExpr]
            (funcStrStart ++ " " ++ forStr ++ " " ++ expressionToString lenExpr ++ ")")

parseNegateFunction :: GenParser Char st Expression
parseNegateFunction = do
    funcStr <- parseToken $ functionName negateFunction
    expr <- parseAnyNonInfixExpression
    let funcWithExprsStr = funcStr ++ expressionToString expr
    return $ FunctionExpression negateFunction [expr] funcWithExprsStr

parseNotFunction :: GenParser Char st Expression
parseNotFunction = do
    funcStr <-parseToken $ functionName notFunction
    expr <- parseAnyNonInfixExpression
    let funcWithExprsStr = funcStr ++ expressionToString expr
    return $ FunctionExpression notFunction [expr] funcWithExprsStr

parseCountStar :: GenParser Char st Expression
parseCountStar = do
    funcStr <- try (parseToken $ functionName countFunction)
    _ <- parenthesize (parseToken "*")
    
    return $ FunctionExpression countFunction [IntConstantExpression 0 "*"] (funcStr ++ "(*)")

--------------------------------------------------------------------------------
-- Parse utility functions
--------------------------------------------------------------------------------

parseOpChar :: CharParser st Char
parseOpChar = oneOf opChars

opChars :: String
opChars = "~!@#$%^&*-+=|\\<>/?."

-- | find out if the given string ends with an op char
endsWithOp :: String -> Bool
endsWithOp strToTest = last strToTest `elem` opChars

-- | A token parser that allows either upper or lower case. all trailing
--   whitespace is consumed
parseToken :: String -> GenParser Char st String
parseToken tokStr =
    eatSpacesAfter (try $ if endsWithOp tokStr then parseOpTok else parseAlphaNumTok)
    where
        parseOpTok = withoutTrailing parseOpChar (string tokStr)
        parseAlphaNumTok =
            withoutTrailing (alphaNum <|> char '_') (upperOrLower tokStr)

-- | parses an identifier. you can use a tick '`' as a quote for
--   an identifier with white-space
parseIdentifier :: GenParser Char st String
parseIdentifier = do
    let parseId = do
            let idChar = alphaNum <|> char '_'
            notFollowedBy digit
            quotedText False '`' <|> many1 idChar
    (eatSpacesAfter parseId `genExcept` parseReservedWord) <?> "identifier"

commaSeparator :: GenParser Char st Char
commaSeparator = eatSpacesAfter $ char ','

-- | Wraps braces parsers around the given inner parser
brace :: GenParser Char st a -> GenParser Char st a
brace innerParser = do
    _ <- eatSpacesAfter $ char '['
    innerParseResults <- innerParser
    _ <- eatSpacesAfter $ char ']'
    return innerParseResults

-- | Wraps parentheses parsers around the given inner parser
parenthesize :: GenParser Char st a -> GenParser Char st a
parenthesize innerParser = do
    _ <- eatSpacesAfter $ char '('
    innerParseResults <- innerParser
    _ <- eatSpacesAfter $ char ')'
    return innerParseResults

parseReservedWord :: GenParser Char st String
parseReservedWord =
    let reservedWordParsers = map parseToken reservedWords
    in  choice reservedWordParsers

-- TODO are function names reserved... i don't think so
reservedWords :: [String]
reservedWords =
    map functionName normalSyntaxFunctions ++
    map functionName (concat infixFunctions) ++
    map functionName specialFunctions ++
    ["BY", "CROSS", "FROM", "FOR", "GROUP", "HAVING", "IN", "ON",
     "ORDER", "SELECT", "WHERE", "TRUE", "FALSE"]

-- | tries parsing both the upper and lower case versions of the given string
upperOrLower :: String -> GenParser Char st String
upperOrLower stringToParse =
    string (map toUpper stringToParse) <|>
    string (map toLower stringToParse) <?> stringToParse
