{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
module Types where

import           Data.Aeson
import           Data.ByteString       (ByteString)
import           Data.ByteString.Char8 (unpack)
import           Data.Map.Strict       (Map)
import           Data.Text.Encoding
import           Data.Time.Clock
import           Data.Time.Format
import qualified Data.Text as T
import           Prelude               hiding (FilePath)
import           Turtle                (FilePath)
import           Web.PathPieces
import qualified Control.Monad.State.Strict as ST
import qualified Control.Monad.Reader as R
import Control.Monad.State.Strict (StateT)
import           Web.Spock.Safe (runQuery, HasSpock(..), WebStateM, ActionT)
import           Turtle hiding (home, time)

import           Database.LevelDB.Higher hiding (Value, Key)
import qualified Database.LevelDB.Higher as LDB

type Bucket = ByteString

data Point = Point
  { bucket :: Bucket
  , keys   :: Map ByteString ByteString
  , time   :: Maybe UTCTime
  , values :: Map ByteString ByteString
  } deriving (Eq)

instance Show Point where
    show p = "Point: \nbucket - " ++ unpack (bucket p)
          ++ "\nkeys - " ++ show (keys p)
          ++ "\nvalues - " ++ show (values p)
          ++ "\ntime - " ++ show (formatTime defaultTimeLocale "%c" <$> (time p))

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

-- type WithDB = R.ReaderT DB IO

type WithDB a = R.ReaderT DB (LevelDBT IO) a

runWithDB :: WithDB a -> DB -> IO a
runWithDB f db = runCreateLevelDB fp "" $ R.runReaderT f db
  where
  fp = fpToStr $ (home $ config db) </> (fromString $ unDBName $ currentDB db) </> "buckets"

fpToStr :: FilePath -> String
fpToStr = either T.unpack T.unpack . toText

withDB :: WithDB a -> ActionT (WebStateM DB String Config) a
withDB = runQuery . runWithDB

class Store handle where
    open   :: Config -> IO handle
    close  :: handle -> IO ()
    initDB :: Config -> DBName -> IO handle

class Construct points where
    construct :: points -> WithDB (Map Bucket Constructed)
    unconstruct :: Constructed -> WithDB points


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
