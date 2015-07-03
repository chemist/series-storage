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
    (k,v):all <- withKeySpace "cpu_load" (scan "" queryItems)
    -- liftIO $ print $ "read query: " ++ show all
    liftIO $ print $ "read query: " ++ show k
    _ <- unconstruct (Single k v) :: WithDB Point
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



instance Construct Point where
    construct x = do
        bucketsFilePath <- askBucketsFilePath
        tags <- makeTagsBS x
        let convertTime = BS.word32BE . toEnum . truncate . utcTimeToPOSIXSeconds  
        liftIO $ print x
        t <- liftIO $ maybe getCurrentTime return (time x)
        liftIO $ print (BS.toLazyByteString $ convertTime t)
        let key =  toStrict . BS.toLazyByteString $ convertTime t <> tags
            val = toStrict $ encode $ Map.toAscList (values x)
        return $ Map.singleton (bucket x) (Single key val)
    unconstruct (Single key val) = do
        bucketsFilePath <- askBucketsFilePath
        let unpackTime x = posixSecondsToUTCTime . toEnum . fromEnum $ x 
            time = unpackTime $ (decode . fromStrict $ B.take 4 key :: Word32)
        liftIO $ print (decode . fromStrict $ B.take 4 key :: Word32)
        liftIO $ print time
        return $ undefined

askBucketsFilePath :: WithDB FilePath
askBucketsFilePath = do
    db <- R.ask
    return $ (home $ config db) </> (fromString $ unDBName $ currentDB db) </> "buckets"

makeTagsBS :: Point -> WithDB BS.Builder
makeTagsBS point =  tags
  where 
    tags = foldlM build "" (Map.toAscList $ keys point)
    build body (key, val) = do
        k <- askKeyId key
        v <- askValId val
        return $ body <> (BS.word32BE k) <> (BS.word32BE v)

keyById :: ByteString -> WithDB Key
keyById i = liftLevelDB $ withKeySpace "id -> key" $ do
    key <- LDB.get i
    case key of
         Just k -> return k 
         Nothing -> error "unknown id key"

valById :: ByteString -> WithDB Values
valById i = liftLevelDB $ withKeySpace "id -> value" $ do
    key <- LDB.get i
    case key of
         Just k -> return k 
         Nothing -> error "unknown id value"

askKeyId :: Key -> WithDB Word32
askKeyId key = liftLevelDB $ withKeySpace "key -> id" $ do
    keyId <- LDB.get key
    case keyId of
         Just k -> return . decode . fromStrict $ k
         Nothing -> addNewKey key

addNewKey :: Key -> LDB.LevelDB Word32
addNewKey key = do
    mmaxId <- LDB.get "___max___key___id___" :: LDB.LevelDB (Maybe ByteString)
    let maxId = maybe (1 :: Word32) (decode . fromStrict) mmaxId
        nextId = succ maxId :: Word32
    LDB.put key (toStrict . encode $ maxId )
    withKeySpace "id -> key" $ LDB.put (toStrict . encode $ maxId) key
    LDB.put "___max___key___id___" (toStrict . encode $ maxId)
    return $ succ maxId


askValId :: Values -> WithDB Word32
askValId val = liftLevelDB $ withKeySpace "value -> id" $ do
    valId <- LDB.get val
    case valId of
         Just k -> return . decode . fromStrict $ k
         Nothing -> addNewVal val

addNewVal :: Key -> LDB.LevelDB Word32
addNewVal key = do
    mmaxId <- LDB.get "___max___val___id___" :: LDB.LevelDB (Maybe ByteString)
    let maxId = maybe (1 :: Word32) (decode . fromStrict) mmaxId
        nextId = succ maxId :: Word32
    LDB.put key (toStrict . encode $ maxId )
    withKeySpace "id -> value" $ LDB.put key (toStrict . encode $ maxId) 
    LDB.put "___max___val___id___" (toStrict . encode $ maxId)
    return $ succ maxId

instance Construct [Point] where
    construct xs = foldl1 (Map.unionWith fun) <$> mapM construct xs
      where
      fun x@Single{} y@Single{} = Many [x,y]
      fun (Many xs) y@Single{} = Many (y:xs)
      fun y@Single{} (Many xs)  = Many (y:xs)
      fun (Many ys) (Many xs)  = Many $ xs ++ ys

testBase :: Config -> IO (Bool, Bool)
testBase conf = (,) <$> testdir (home conf) <*> testdir (home conf </> fromString (unDBName $ base conf))

