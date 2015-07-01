{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE GADTs #-}
module Types where

import           Data.Aeson
import           Data.ByteString       (ByteString)
import           Data.ByteString.Char8 (unpack)
import           Data.Map.Strict       (Map)
import           Data.Text.Encoding
import           Data.Time.Clock
import           Data.Time.Format
import           Prelude               hiding (FilePath)
import           Turtle                (FilePath)
import           Web.PathPieces
import qualified Control.Monad.State.Strict as ST
import Control.Monad.State.Strict (StateT)
import           Web.Spock.Safe (runQuery, HasSpock(..))

type Bucket = ByteString

data Point = Point
  { bucket :: Bucket
  , keys   :: Map ByteString ByteString
  , time   :: UTCTime
  , values :: Map ByteString ByteString
  } deriving (Eq)

instance Show Point where
    show p = "Point: \nbucket - " ++ unpack (bucket p)
          ++ "\nkeys - " ++ show (keys p)
          ++ "\nvalues - " ++ show (values p)
          ++ "\ntime - " ++ (formatTime defaultTimeLocale "%c" (time p))

class Stored a where
    parse :: ByteString -> Either String a

data Config = Config 
    { home  :: FilePath
    , base  :: DBName
    } deriving (Show, Eq)

data Context = Context
  { config    :: Config 
  , currentDB :: DBName
  }

type Base = StateT Context IO

runBase :: HasSpock m => StateT (SpockConn m) IO a -> m a
runBase = runQuery . ST.evalStateT

type Home = FilePath

class Store handle where
    open   :: Config -> IO handle
    close  :: handle -> IO ()
    initDB :: Config -> DBName -> IO handle

class Construct points where
    construct :: points -> Constructed

type Key = ByteString
type Values = ByteString

data Constructed = Single Key Values
                 | Many [Constructed]
                 deriving (Show, Eq)


newtype DBName = DBName String deriving (Show, Eq)

data Query = Query Bucket
  deriving (Show, Eq)

instance PathPiece Query where
    fromPathPiece x = Just (Query $ encodeUtf8 x)
    toPathPiece (Query x) = decodeUtf8 x
