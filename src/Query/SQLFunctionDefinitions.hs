-----------------------------------------------------------------------------
-- |
-- Module      :  Database.TxtSushi.SQLFunctionDefinitions
-- Copyright   :  (c) Keith Sheppard 2009-2010
-- License     :  BSD3
-- Maintainer  :  keithshep@gmail.com
-- Stability   :  experimental
-- Portability :  portable
--
-- SQL Function definitions. This is where you can extend TxtSushi by adding
-- new function definitions.
--
-----------------------------------------------------------------------------

module Query.SQLFunctionDefinitions (
    SQLFunction(..),
    normalSyntaxFunctions,
    infixFunctions,
    specialFunctions,
    
    -- TODO remove these
    negateFunction,
    countFunction,
    substringFromToFunction,
    substringFromFunction,
    notFunction) where

import Data.Char
import Data.List
import Data.Maybe
import Text.Regex.Posix

import Query.EvaluatedExpression
import Query.SQLExpression

-- Functions with "normal" syntax --
normalSyntaxFunctions :: [SQLFunction]
normalSyntaxFunctions =
    [absFunction, 
     -- all aggregates except count which accepts a (*)
     avgFunction, firstFunction, lastFunction, maxFunction,
     minFunction, sumFunction]

absFunction :: SQLFunction
absFunction = SQLFunction {
    functionName        = "ABS",
    minArgCount         = 1,
    argCountIsFixed     = True,
    applyFunction       = applyUnaryNumeric absFunction abs abs,
    functionGrammar     = normalGrammar absFunction,
    functionDescription = "absolute value function"}

-- aggregates
avgFunction :: SQLFunction
avgFunction = SQLFunction {
    functionName        = "AVG",
    minArgCount         = 1,
    argCountIsFixed     = False,
    applyFunction       = avgFun . checkArgCount avgFunction,
    functionGrammar     = normalGrammar avgFunction,
    functionDescription = "aggregate average function"}
    -- TODO this AVG(...) holds the whole arg list in memory. reimplement!
    where
        avgFun args = RealExpression $
            foldl1' (+) (map coerceReal args) / fromIntegral (length args)

countFunction :: SQLFunction
countFunction = SQLFunction {
    functionName        = "COUNT",
    minArgCount         = 0,
    argCountIsFixed     = False,
    applyFunction       = IntExpression . length,
    functionGrammar     = normalGrammar countFunction,
    functionDescription = "aggregate function for calculating group size"}

firstFunction :: SQLFunction
firstFunction = SQLFunction {
    functionName        = "FIRST",
    minArgCount         = 1,
    argCountIsFixed     = False,
    applyFunction       = head . checkArgCount firstFunction,
    functionGrammar     = normalGrammar firstFunction,
    functionDescription = "aggregate function returning only the first element of every group"}

lastFunction :: SQLFunction
lastFunction = SQLFunction {
    functionName        = "LAST",
    minArgCount         = 1,
    argCountIsFixed     = False,
    applyFunction       = last . checkArgCount lastFunction,
    functionGrammar     = normalGrammar lastFunction,
    functionDescription = "aggregate function returning only the last element of every group"}

maxFunction :: SQLFunction
maxFunction = SQLFunction {
    functionName        = "MAX",
    minArgCount         = 1,
    argCountIsFixed     = False,
    applyFunction       = maximum . checkArgCount maxFunction,
    functionGrammar     = normalGrammar maxFunction,
    functionDescription = "aggregate function returning the maximum element of every group"}

minFunction :: SQLFunction
minFunction = SQLFunction {
    functionName        = "MIN",
    minArgCount         = 1,
    argCountIsFixed     = False,
    applyFunction       = minimum . checkArgCount minFunction,
    functionGrammar     = normalGrammar minFunction,
    functionDescription = "aggregate function returning the minimum element of every group"}

sumFunction :: SQLFunction
sumFunction = SQLFunction {
    functionName        = "SUM",
    minArgCount         = 0,
    argCountIsFixed     = False,
    applyFunction       = foldl stepSum (IntExpression 0),
    functionGrammar     = normalGrammar sumFunction,
    functionDescription = "aggregate function which summs all elements in each group"}
    where
        stepSum prevSum currArg =
            if useRealAlgebra prevSum || useRealAlgebra currArg
                then RealExpression $ coerceReal prevSum + coerceReal currArg
                else IntExpression $ coerceInt prevSum + coerceInt currArg

-- Infix functions --
infixFunctions :: [[SQLFunction]]
infixFunctions =
    [[multiplyFunction, divideFunction],
     [plusFunction, minusFunction],
     [concatenateFunction],
     [isFunction, isNotFunction, lessThanFunction, lessThanOrEqualToFunction,
      greaterThanFunction, greaterThanOrEqualToFunction, regexMatchFunction],
     [andFunction],
     [orFunction]]

-- Algebraic
multiplyFunction :: SQLFunction
multiplyFunction = SQLFunction {
    functionName        = "*",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryNumeric multiplyFunction (*) (*),
    functionGrammar     = binaryInfixGrammar multiplyFunction,
    functionDescription = "multiplies the left and right expressions"}

divideFunction :: SQLFunction
divideFunction = SQLFunction {
    functionName        = "/",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = divFun . checkArgCount divideFunction,
    functionGrammar     = binaryInfixGrammar divideFunction,
    functionDescription = "divides the left expression by the right expression"}
    where
        divFun [numExpr, denomExpr] = RealExpression $ coerceReal numExpr / coerceReal denomExpr
        divFun _ = internalError

plusFunction :: SQLFunction
plusFunction = SQLFunction {
    functionName        = "+",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryNumeric plusFunction (+) (+),
    functionGrammar     = binaryInfixGrammar plusFunction,
    functionDescription = "adds the left and right expressions"}

minusFunction :: SQLFunction
minusFunction = SQLFunction {
    functionName        = "-",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryNumeric minusFunction (-) (-),
    functionGrammar     = binaryInfixGrammar minusFunction,
    functionDescription = "subtracts the right expression from the left expression"}

-- Boolean
isFunction :: SQLFunction
isFunction = SQLFunction {
    functionName        = "=",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison isFunction (==),
    functionGrammar     = binaryInfixGrammar isFunction,
    functionDescription = "tests the left and right expressions for equality"}

isNotFunction :: SQLFunction
isNotFunction = SQLFunction {
    functionName        = "<>",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison isNotFunction (/=),
    functionGrammar     = binaryInfixGrammar isNotFunction,
    functionDescription = "evaluates as true if the left and right expressions are not equal"}

lessThanFunction :: SQLFunction
lessThanFunction = SQLFunction {
    functionName        = "<",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison lessThanFunction (<),
    functionGrammar     = binaryInfixGrammar lessThanFunction,
    functionDescription = "evaluates as true if the left expression is \"less than\" the right expression"}

lessThanOrEqualToFunction :: SQLFunction
lessThanOrEqualToFunction = SQLFunction {
    functionName        = "<=",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison lessThanOrEqualToFunction (<=),
    functionGrammar     = binaryInfixGrammar lessThanOrEqualToFunction,
    functionDescription = "evaluates as true if the left expression is \"less than or equal to\" the right expression"}

greaterThanFunction :: SQLFunction
greaterThanFunction = SQLFunction {
    functionName        = ">",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison greaterThanFunction (>),
    functionGrammar     = binaryInfixGrammar greaterThanFunction,
    functionDescription = "evaluates as true if the left expression is \"greater than\" the right expression"}

greaterThanOrEqualToFunction :: SQLFunction
greaterThanOrEqualToFunction = SQLFunction {
    functionName        = ">=",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryComparison greaterThanOrEqualToFunction (>=),
    functionGrammar     = binaryInfixGrammar greaterThanOrEqualToFunction,
    functionDescription = "evaluates as true if the left expression is \"greater than or equal to\" the right expression"}

andFunction :: SQLFunction
andFunction = SQLFunction {
    functionName        = "AND",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryBooleanTest andFunction (&&),
    functionGrammar     = binaryInfixGrammar andFunction,
    functionDescription = "evaluates as true if and only if both the left and right expressions are true"}

orFunction :: SQLFunction
orFunction = SQLFunction {
    functionName        = "OR",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = applyBinaryBooleanTest orFunction (||),
    functionGrammar     = binaryInfixGrammar orFunction,
    functionDescription = "evaluates as true if and only if either the left or right expressions are true"}

concatenateFunction :: SQLFunction
concatenateFunction = SQLFunction {
    functionName        = "||",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = catExprs . checkArgCount concatenateFunction,
    functionGrammar     = binaryInfixGrammar concatenateFunction,
    functionDescription = "performs string concatenation of the left and right strings"}
    where
        catExprs [arg1, arg2] = StringExpression $ coerceString arg1 ++ coerceString arg2
        catExprs _ = internalError

regexMatchFunction :: SQLFunction
regexMatchFunction = SQLFunction {
    functionName        = "=~",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = regexMatch . checkArgCount regexMatchFunction,
    functionGrammar     = binaryInfixGrammar regexMatchFunction,
    functionDescription = "evaluates as true if and only if the text on the left matches the regular expression on the right"}
    where
        regexMatch [arg1, arg2] = BoolExpression $ coerceString arg1 =~ coerceString arg2
        regexMatch _ = internalError

-- Functions with special syntax --
specialFunctions :: [SQLFunction]
specialFunctions = [substringFromFunction,
                    substringFromToFunction,
                    negateFunction,
                    notFunction]

negateFunction :: SQLFunction
negateFunction = SQLFunction {
    functionName        = "-",
    minArgCount         = 1,
    argCountIsFixed     = True,
    applyFunction       = applyUnaryNumeric negateFunction negate negate,
    functionGrammar     = "-numeric_expression",
    functionDescription = "unary negation"}

-- | SUBSTRING(extraction_string FROM starting_position [FOR length]
--             [COLLATE collation_name])
--   TODO implement COLLATE part
substringFromFunction :: SQLFunction
substringFromFunction = SQLFunction {
    functionName        = "SUBSTRING",
    minArgCount         = 2,
    argCountIsFixed     = True,
    applyFunction       = substringFrom . checkArgCount substringFromFunction,
    functionGrammar     = "SUBSTRING(string_expression FROM start_index [FOR length_expression])",
    functionDescription = "returns substring of string_expression going from " ++
                          "start_index using 1-based indexing for a length of length_expression " ++
                          " or to the end of string_expression if the FOR part is omitted"}
    where
        substringFrom [strExpr, fromExpr] = StringExpression $
            drop (coerceInt fromExpr - 1) (coerceString strExpr)
        substringFrom _ = internalError

substringFromToFunction :: SQLFunction
substringFromToFunction = SQLFunction {
    functionName        = "SUBSTRING",
    minArgCount         = 3,
    argCountIsFixed     = True,
    applyFunction       = substringFromTo . checkArgCount substringFromToFunction,
    functionGrammar     = "SUBSTRING(string_expression FROM start_index [FOR length_expression])",
    functionDescription = "returns substring of string_expression going from " ++
                          "start_index using 1-based indexing for a length of length_expression " ++
                          " or to the end of string_expression if the FOR part is omitted"}
    where
        substringFromTo [strExpr, fromExpr, toExpr] = StringExpression $
            take (coerceInt toExpr) (drop (coerceInt fromExpr - 1) (coerceString strExpr))
        substringFromTo _ = internalError

notFunction :: SQLFunction
notFunction = SQLFunction {
    functionName        = "NOT",
    minArgCount         = 1,
    argCountIsFixed     = True,
    applyFunction       = applyUnaryBool notFunction not,
    functionGrammar     = "NOT bool_expression",
    functionDescription = "evaluates as true if and only if bool_expression is false"}

-- some evaluation helper functions

applyUnaryString ::
    SQLFunction
    -> (String -> String)
    -> [EvaluatedExpression]
    -> EvaluatedExpression
applyUnaryString sqlFun f =
    StringExpression . f . coerceString . head . checkArgCount sqlFun

applyBinaryBooleanTest ::
    SQLFunction
    -> (Bool -> Bool -> Bool)
    -> [EvaluatedExpression]
    -> EvaluatedExpression
applyBinaryBooleanTest _ f [arg1, arg2] =
        BoolExpression $ f (coerceBool arg1) (coerceBool arg2)
applyBinaryBooleanTest sqlFun _ args  = badArgCountError sqlFun args

applyBinaryComparison ::
    SQLFunction
    -> (t -> t -> Bool)
    -> [t]
    -> EvaluatedExpression
applyBinaryComparison _      cmp [arg1, arg2] = BoolExpression $ cmp arg1 arg2
applyBinaryComparison sqlFun _   args         = badArgCountError sqlFun args

applyUnaryNumeric ::
    SQLFunction
    -> (Int -> Int)
    -> (Double -> Double)
    -> [EvaluatedExpression]
    -> EvaluatedExpression
applyUnaryNumeric _ intFunc realFunc [arg] =
    if useRealAlgebra arg then
        RealExpression $ realFunc (coerceReal arg)
    else
        IntExpression $ intFunc (coerceInt arg)
applyUnaryNumeric sqlFun _ _ args  = badArgCountError sqlFun args

applyBinaryNumeric ::
    SQLFunction
    -> (Int -> Int -> Int)
    -> (Double -> Double -> Double)
    -> [EvaluatedExpression]
    -> EvaluatedExpression
applyBinaryNumeric _ intFunc realFunc [arg1, arg2] =
    if useRealAlgebra arg1 || useRealAlgebra arg2 then
        RealExpression $ realFunc (coerceReal arg1) (coerceReal arg2)
    else
        IntExpression $ intFunc (coerceInt arg1) (coerceInt arg2)
applyBinaryNumeric sqlFun _ _ args  = badArgCountError sqlFun args

applyUnaryBool ::
    SQLFunction
    -> (Bool -> Bool)
    -> [EvaluatedExpression]
    -> EvaluatedExpression
applyUnaryBool _      f [arg] = BoolExpression $ f (coerceBool arg)
applyUnaryBool sqlFun _ args  = badArgCountError sqlFun args

checkArgCount :: SQLFunction -> [a] -> [a]
checkArgCount sqlFun args =
    if argCountOK then args else badArgCountError sqlFun args
    where
        minArgs = minArgCount sqlFun
        
        argCountOK =
            if argCountIsFixed sqlFun
                then lengthEquals args minArgs
                else lengthAtLeast args minArgs
            where
                lengthEquals xs len = go xs 0
                    where
                        go [] cumLen = cumLen == len
                        go (_:yt) cumLen = (cumLen < len) && go yt (cumLen + 1)
                
                lengthAtLeast xs len = go xs 0
                    where
                        go [] cumLen = cumLen >= len
                        go (_:yt) cumLen = (cumLen >= len) || go yt (cumLen + 1)

badArgCountError :: SQLFunction -> [a] -> b
badArgCountError sqlFun args =
    if argCountIsFixed sqlFun then error $
        "Error: bad argument count in " ++ functionName sqlFun ++
        " expected " ++ show (minArgCount sqlFun) ++
        " argument(s) but was given " ++ show received
    else error $
        "Error: bad argument count in " ++ functionName sqlFun ++
        " expected at least " ++ show (minArgCount sqlFun) ++
        " argument(s) but was given " ++ show received
    where received = length args

internalError :: a
internalError = error "Internal Error: this should never occur"

useRealAlgebra :: EvaluatedExpression -> Bool
useRealAlgebra (RealExpression _) = True
useRealAlgebra expr = case maybeCoerceInt expr of
    Nothing -> True
    Just _  -> False

-- | trims leading and trailing spaces
trimSpace :: String -> String
trimSpace = f . f
    where f = reverse . dropWhile isSpace

-- | some grammar helper functions
normalGrammar :: SQLFunction -> String
normalGrammar sqlFun = functionName sqlFun ++ "(" ++ argStr ++ ")"
    where
        argStrPrefix = intercalate ", " minArgs
            where minArgs =map ((++) "arg" . show) [1 .. minArgCount sqlFun] 
        
        argStr 
          | argCountIsFixed sqlFun = argStrPrefix
          | minArgCount sqlFun >= 1 = argStrPrefix ++ ", ..."
          | otherwise = "..."

binaryInfixGrammar :: SQLFunction -> String
binaryInfixGrammar sqlFun = "leftExpr " ++ functionName sqlFun ++ " rightExpr"
