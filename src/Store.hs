{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
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
import qualified Data.ByteString.Builder as BS
import qualified Data.Map.Strict as Map
import Data.Foldable (foldlM)
import Data.Word (Word32)
import Data.Binary (decode, encode)
import Data.Maybe
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)

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
    write = R.runReaderT . write' 
    query = R.runReaderT . query' 

write'  :: forall points . (Show points, Construct points) => points -> WithDB Value
write' points = do
    liftIO $ print $ "write query: "
    liftIO $ print points
    bs <- construct points
    liftIO $ print bs
    return Null

query'  :: Query -> WithDB Value
query' (Query y) = do
    liftIO $ print $ "read query: " ++ show y
    return Null

-- initDB
-- create folder for database
initDB' :: Config -> DBName -> IO DB
initDB' conf db = do
    (isHomeOk, isBaseOk) <- testBase conf
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

fpToStr :: FilePath -> String
fpToStr = either unpack unpack . toText


instance Construct Point where
    construct x = do
        bucketsFilePath <- askBucketsFilePath
        tags <- makeTagsBS x
        let convertTime = BS.word32BE . toEnum . truncate . utcTimeToPOSIXSeconds . time 
        let key =  toStrict . BS.toLazyByteString $ convertTime x <> tags
            val = toStrict $ encode $ Map.toAscList (values x)
        return $ Single key val

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

askKeyId :: Key -> WithDB Word32
askKeyId key = do
    bucketsFilePath <- fpToStr <$> askBucketsFilePath
    liftIO $ runCreateLevelDB bucketsFilePath "___keys" $ do
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
    LDB.put "___max___key___id___" (toStrict . encode $ maxId)
    return $ succ maxId


askValId :: Values -> WithDB Word32
askValId val = do
    bucketsFilePath <- fpToStr <$> askBucketsFilePath
    liftIO $ runCreateLevelDB bucketsFilePath "___values" $ do
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
    LDB.put "___max___val___id___" (toStrict . encode $ maxId)
    return $ succ maxId

instance Construct [Point] where
    construct xs = Many <$> mapM construct xs

testBase :: Config -> IO (Bool, Bool)
testBase conf = (,) <$> testdir (home conf) <*> testdir (home conf </> fromString (unDBName $ base conf))

