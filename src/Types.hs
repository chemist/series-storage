module Types where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 (unpack)
import Data.Map.Strict (Map)
import Data.Time.Clock
import Data.Time.Format


data Point = Point
  { bucket :: ByteString
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
