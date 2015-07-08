-----------------------------------------------------------------------------
-- |
-- Module      :  Database.TxtSushi.SQLParser
-- Copyright   :  (c) Keith Sheppard 2009-2010
-- License     :  BSD3
-- Maintainer  :  keithshep@gmail.com
-- Stability   :  experimental
-- Portability :  portable
--
-- SQL Expressions
--
-----------------------------------------------------------------------------

module Query.SQLExpression (
    allMaybeTableNames,
    TableExpression(..),
    TableIdentifier(..),
    ColumnIdentifier(..),
    ColumnSelection(..),
    Expression(..),
    SQLFunction(..),
    OrderByItem(..),
    isAggregate,
    selectStatementContainsAggregates,
    expressionToString,
    columnToString) where

import           Query.EvaluatedExpression

--------------------------------------------------------------------------------
-- The data definition for select statements
--------------------------------------------------------------------------------

data TableIdentifier = TableIdentifier String deriving (Show)

-- | represents a select statement
--   TODO this should be moved inside the TableExpression type
data TableExpression = TableExpression
    { columnSelections   :: [ColumnSelection]
    , maybeFromTable     :: Maybe TableIdentifier
    , maybeWhereFilter   :: Maybe Expression
    , maybeGroupByHaving :: Maybe ([Expression], Maybe Expression)
    , orderByItems       :: [OrderByItem]
    } deriving (Show)

-- | convenience function for extracting all of the table names used by the
--   given table expression
allMaybeTableNames :: (Maybe TableIdentifier) -> String
allMaybeTableNames Nothing = []
allMaybeTableNames (Just tblExp) = allTableNames tblExp

allTableNames :: TableIdentifier -> String
allTableNames (TableIdentifier selectStmt) = selectStmt

data ColumnSelection
    = AllColumns
    | AllColumnsFrom {sourceTableName :: String}
    | ExpressionColumn
      { expression       :: Expression
      }
    deriving (Show)

data ColumnIdentifier =
    ColumnIdentifier
      { tableName :: String
      } deriving (Show, Eq)

data Expression
    = FunctionExpression
        { sqlFunction          :: SQLFunction
        , functionArguments    :: [Expression]
        , stringRepresentation :: String
        }
    | ColumnExpression
       { column               :: ColumnIdentifier
       }
    | StringConstantExpression
      { stringConstant       :: String
      , stringRepresentation :: String
      }
    | IntConstantExpression
      { intConstant          :: Int
      , stringRepresentation :: String
      }
    | RealConstantExpression
      { realConstant         :: Double
      , stringRepresentation :: String
      }
    | BoolConstantExpression
      { boolConstant         :: Bool
      , stringRepresentation :: String
      }
    deriving (Show)

data SQLFunction = SQLFunction
    { functionName        :: String
    , minArgCount         :: Int
    , argCountIsFixed     :: Bool
    , functionGrammar     :: String
    , functionDescription :: String
    , applyFunction       :: [EvaluatedExpression] -> EvaluatedExpression
    }

instance Show SQLFunction where
    show f = functionName f

-- | an aggregate function is one whose min function count is 1 and whose
--   arg count is not fixed
isAggregate :: SQLFunction -> Bool
isAggregate = not . argCountIsFixed

containsAggregates :: Expression -> Bool
containsAggregates (FunctionExpression sqlFun args _) =
    isAggregate sqlFun || any containsAggregates args
containsAggregates _ = False

selectionContainsAggregates :: ColumnSelection -> Bool
selectionContainsAggregates (ExpressionColumn expr) =
    containsAggregates expr
selectionContainsAggregates _ = False

orderByItemContainsAggregates :: OrderByItem -> Bool
orderByItemContainsAggregates (OrderByItem expr _) =
    containsAggregates expr

selectStatementContainsAggregates :: TableExpression -> Bool
selectStatementContainsAggregates select =
    any selectionContainsAggregates (columnSelections select) ||
    any orderByItemContainsAggregates (orderByItems select)

expressionToString :: Expression -> String
expressionToString (FunctionExpression _ _ strRep) = strRep
expressionToString (ColumnExpression (ColumnIdentifier strRep)) = strRep
expressionToString (StringConstantExpression _ strRep) = strRep
expressionToString (IntConstantExpression _ strRep) = strRep
expressionToString (RealConstantExpression _ strRep) = strRep
expressionToString (BoolConstantExpression _ strRep) = strRep

columnToString :: ColumnIdentifier -> String
columnToString (ColumnIdentifier tblName ) = tblName 

data OrderByItem = OrderByItem
    { orderExpression :: Expression
    , orderAscending  :: Bool
    } deriving (Show)
