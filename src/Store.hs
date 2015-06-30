{-# LANGUAGE FlexibleInstances #-}
module Store where

import           Data.Aeson
import           Database.LevelDB.Higher
import           Types

data DB = DB

instance Store DB where
    open conf = print ("open db with conf: " ++ show conf) >> return DB
    close _ = print "close db"
    write base x _ = do
        print $ "write query: "
        print x
        return Null
    query _ (Query y) _ = do
        print $ "read query: " ++ show y
        return Null
    init _ (DBName _) = return ()

instance Allowed Point where
    construct x = [x]

instance Allowed [Point] where
    construct = id
