{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
module Store where

import           Data.Aeson
import           Database.LevelDB.Higher hiding (Value)
import           Turtle hiding (home)
import           Types 
import Prelude hiding (FilePath)
import Control.Monad.State.Strict as ST

data DB = DB DBName

{-- store schema:
For every bucket create own directory with leveldb table
For every Point we build key as
agregated fun :: Word16 | time :: Word64 | tag :: Word32 | value :: Word32 |

--}

instance Store Context where
    open conf = do
        liftIO $ print ("open db with conf: " ++ show conf)
        isBaseOk <- testBase conf 
        if uncurry (&&) isBaseOk
           then return $ Context conf (base conf)
           else initDB conf (base conf)
    close _ = liftIO $ print "close db"
    initDB conf db = return $ Context conf db

write  :: forall points . (Show points, Construct points) => points -> Base Value
write x = do
    liftIO $ print $ "write query: "
    liftIO $ print x
    return Null

query  :: Query -> Base Value
query (Query y) = do
    liftIO $ print $ "read query: " ++ show y
    return Null


instance Construct Point where
    construct x = undefined

instance Construct [Point] where
    construct = undefined

testBase :: Config -> IO (Bool, Bool)
testBase conf = (,) <$> testdir (home conf) <*> testdir (home conf </> fromString basePath)
  where
  DBName basePath = base conf

