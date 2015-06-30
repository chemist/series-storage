{-# LANGUAGE RankNTypes #-}
module Types where

import           Data.Aeson
import           Data.ByteString       (ByteString)
import           Data.ByteString.Char8 (unpack)
import           Data.Map.Strict       (Map)
import           Data.Text.Encoding
import           Data.Time.Clock
import           Data.Time.Format
import           Web.PathPieces

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

data Config = Config {
    basePath :: FilePath
    } deriving (Show, Eq)


class Store handle where
    open  :: Config -> IO handle
    close :: handle -> IO ()
    init  :: handle -> DBName -> IO ()
    query :: DBName -> Query -> handle -> IO Value
    write :: forall points . (Show points, Allowed points) => DBName -> points ->  handle -> IO Value

class Allowed points where
    construct :: points -> [Point]


newtype DBName = DBName String deriving (Show, Eq)

data Query = Query Bucket
  deriving (Show, Eq)

instance PathPiece Query where
    fromPathPiece x = Just (Query $ encodeUtf8 x)
    toPathPiece (Query x) = decodeUtf8 x
