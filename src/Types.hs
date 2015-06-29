{-# LANGUAGE RankNTypes #-}
module Types where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 (unpack)
import Data.Map.Strict (Map)
import Data.Time.Clock
import Data.Time.Format
import Data.Aeson
import Web.PathPieces
import Data.Text.Encoding

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


class Store a where
    open  :: Config -> IO a
    close :: a -> IO ()
    init :: a -> DBName -> IO ()
    query :: DBName -> Query -> a -> IO Value
    write :: forall b . (Show b, Allowed b) => DBName -> b ->  a -> IO Value

class Allowed b where
    construct :: b -> [Point]
    

newtype DBName = DBName String deriving (Show, Eq)

data Query = Query Bucket
  deriving (Show, Eq)
         
instance PathPiece Query where
    fromPathPiece x = Just (Query $ encodeUtf8 x)
    toPathPiece (Query x) = decodeUtf8 x
