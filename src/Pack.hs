{-# LANGUAGE FlexibleInstances    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeSynonymInstances #-}
module Pack where

import           Point
import           Types

import           Control.Monad
import           Control.Monad.IO.Class
import           Data.ByteString hiding (foldl1)
import qualified Data.Map                as Map
import Data.Map (Map)
import           Data.Serialize
import           Data.Time.Clock
import           Data.Time.Clock.POSIX   (posixSecondsToUTCTime,
                                          utcTimeToPOSIXSeconds)
import           Data.Word
import           Database.LevelDB.Higher hiding (Key, Value)
import qualified Database.LevelDB.Higher as LDB
import Prelude hiding (drop)

data SPoint = SPoint [(Word32, Word32)] Word32 deriving (Show, Eq)

instance Serialize SPoint where
    put (SPoint m t) = do
        putWord32be t
        putListOf (putTwoOf putWord32be putWord32be) m
    get = do
        t <- getWord32be 
        m <- getListOf (getTwoOf getWord32be getWord32be)
        return $ SPoint m t

instance Construct Point where
    construct point = do
        key <- encode <$> pointToSPoint point
        let val = encode (values point)
        return $ Map.singleton (bucket point) (Single key val)
    unconstruct (Single key val) = do
        let spoint = either error id $ decode key
            values = either error id $ decode val
        sPointToPoint spoint "" values

instance Construct [Point] where
    construct xs = foldl1 (Map.unionWith fun) <$> mapM construct xs
      where
      fun x@Single{} y@Single{} = Many [x,y]
      fun (Many xs) y@Single{} = Many (y:xs)
      fun y@Single{} (Many xs)  = Many (y:xs)
      fun (Many ys) (Many xs)  = Many $ xs ++ ys
    unconstruct (Many xs) = mapM unconstruct xs

pointToSPoint :: Point -> WithDB SPoint
pointToSPoint point = do
    t <- maybe (liftIO $ getCurrentTime) return (time point)
    let ks = Map.toAscList (keys point)
    ks' <- mapM (\(k,v) -> (,) <$> idByKey k <*> idByVal v) ks
    return $ SPoint ks' (convertTime t)

sPointToPoint :: SPoint -> Bucket -> Map ByteString ByteString -> WithDB Point
sPointToPoint (SPoint m t) bucket values = do
    let t' = Just (unConvertTime t)
    ks <- Map.fromList <$> mapM (\(k,v) -> (,) <$> keyById k <*> valById v) m
    return $ Point bucket ks t' values

convertTime :: UTCTime -> Word32
convertTime = toEnum . truncate . utcTimeToPOSIXSeconds

unConvertTime :: Word32 -> UTCTime
unConvertTime = posixSecondsToUTCTime . fromIntegral

keyById :: Word32 -> WithDB ByteString
keyById w = withKeySpace "id -> key" $ do
    v <- LDB.get (encode w) :: WithDB (Maybe ByteString)
    case v of
         Nothing -> error "keyById"
         Just vv -> return vv

valById :: Word32 -> WithDB ByteString
valById w = withKeySpace "id -> val" $ do
    v <- LDB.get (encode w)
    case v of
         Nothing -> error "valById"
         Just vv -> return vv

idByKey :: ByteString -> WithDB Word32
idByKey key = withKeySpace "key -> id" $ do
    keyId <- LDB.get key
    case keyId of
         Nothing -> addNewKey key
         Just k -> either error return (runGet getWord32be k)

addNewKey :: ByteString -> WithDB Word32
addNewKey key = do
    mmaxId <- LDB.get "___max___key___id___" :: WithDB (Maybe ByteString)
    let maxId = either error id $ maybe (Right 1) (runGet getWord32be) mmaxId
        nextId = succ maxId
    LDB.put key (encode nextId)
    withKeySpace "id -> key" $ LDB.put (encode nextId) key
    LDB.put "___max___key___id___" (encode nextId)
    return $ nextId

idByVal :: ByteString -> WithDB Word32
idByVal key = withKeySpace "value -> id" $ do
    keyId <- LDB.get key
    case keyId of
         Nothing -> addNewVal key
         Just k -> either error return  (runGet getWord32be k)

addNewVal :: ByteString -> WithDB Word32
addNewVal key = do
    mmaxId <- LDB.get "___max___val___id___" :: WithDB (Maybe ByteString)
    let maxId = either error id $ maybe (Right 1) (runGet getWord32be) mmaxId
        nextId = succ maxId
    LDB.put key (encode nextId)
    withKeySpace "id -> val" $ LDB.put (encode nextId) key
    LDB.put "___max___val___id___" (encode nextId)
    return $ nextId

