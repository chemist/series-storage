{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
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
import qualified Control.Monad.Reader as R
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

type Home = FilePath

data Config = Config 
    { home  :: Home
    , base  :: DBName
    } deriving (Show, Eq)


data DB = DB
  { config    :: Config 
  , currentDB :: DBName
  }

type WithDB = R.ReaderT DB IO

class Store handle where
    open   :: Config -> IO handle
    close  :: handle -> IO ()
    initDB :: Config -> DBName -> IO handle
    write  :: forall points . (Show points, Construct points) => points -> handle -> IO Value
    query  :: Query -> handle -> IO Value

class Construct points where
    construct :: points -> WithDB Constructed


type Key = ByteString
type Values = ByteString

data Constructed = Single Key Values
                 | Many [Constructed]
                 deriving (Show, Eq)


newtype DBName = DBName { unDBName :: String } deriving (Show, Eq)

data Query = Query Bucket
  deriving (Show, Eq)

instance PathPiece Query where
    fromPathPiece x = Just (Query $ encodeUtf8 x)
    toPathPiece (Query x) = decodeUtf8 x
