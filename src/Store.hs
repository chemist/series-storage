{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleContexts #-}
module Store where

import           Data.Aeson hiding (decode, encode)
import           Database.LevelDB.Higher hiding (Value, Key)
import qualified Database.LevelDB.Higher as LDB
import           Turtle hiding (home, time)
import           Types 
import Prelude hiding (FilePath)
import Control.Monad.Reader as R
import Data.Text (unpack)
import Control.Concurrent.MVar
import Data.ByteString (ByteString)
import Data.ByteString.Lazy (toStrict, fromStrict)
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as B
import qualified Data.ByteString.Builder as BS
import qualified Data.Map.Strict as Map
import Data.Foldable (foldlM)
import Data.Word (Word32)
import Data.Binary (decode, encode)
import Data.Maybe
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds, posixSecondsToUTCTime)
import           Data.Time.Clock
import Pack

{-- store schema:
For every bucket create own directory with leveldb table
For every Point we build key as
agregated fun :: Word16 | time :: Word64 | tag :: Word32 | value :: Word32 |

--}

instance Store DB where
    open conf = do
        liftIO $ print ("open db with conf: " ++ show conf)
        isBaseOk <- testBase conf 
        if uncurry (&&) isBaseOk
           then return $ DB conf (base conf)
           else initDB conf (base conf)
    close _ = liftIO $ print "close db"
    initDB = initDB'

write  :: forall points . (Show points, Construct points) => points -> WithDB Value
write points = do
--    liftIO $ print $ "write query: "
--    liftIO $ print points
    bs <- construct points
    mapM_ write' $ Map.toList bs
--    liftIO $ print bs
    return Null
    where
      write' :: (Bucket, Constructed) -> WithDB ()
      write' (bucket, Single key val) = withKeySpace bucket $ LDB.put key val 
      write' (bucket, Many xs) = withKeySpace bucket $ LDB.runBatch $ mapM_ (\(Single key val) -> putB key val) xs

query  :: Query -> WithDB Value
query (Query y) = do
    all <-  map (uncurry Single) <$> withKeySpace "cpu_load" (scan "" queryItems)
--     liftIO $ print $ "read query: " ++ show all
    r <- unconstruct (Many all) :: WithDB [Point]
    liftIO $ print r
    return Null

-- initDB
-- create folder for database
initDB' :: Config -> DBName -> IO DB
initDB' conf db = do
    (isHomeOk, isBaseOk) <- liftIO $ testBase conf
    unless isHomeOk $ mkdir $ home conf 
    let basePath = home conf </> (fromString $ unDBName db)
        buckets = basePath </> "buckets"
        bucketsFilePath = fpToStr buckets
    unless isBaseOk $ do
        mkdir basePath
        mkdir buckets
        runCreateLevelDB bucketsFilePath "____buckets" $ do
            LDB.put "init" "init"
            LDB.delete "init"
    return $ DB conf db

askBucketsFilePath :: WithDB FilePath
askBucketsFilePath = do
    db <- R.ask
    return $ (home $ config db) </> (fromString $ unDBName $ currentDB db) </> "buckets"


testBase :: Config -> IO (Bool, Bool)
testBase conf = (,) <$> testdir (home conf) <*> testdir (home conf </> fromString (unDBName $ base conf))

