{-# LANGUAGE OverloadedStrings #-}
module Point where

import           Control.Applicative              ((<|>))
import qualified Data.Attoparsec.ByteString       as A
import           Data.Attoparsec.ByteString.Char8 (decimal)
import qualified Data.Attoparsec.Combinator       as A
import           Data.ByteString                  (ByteString, scanl1)
import qualified Data.Map.Strict                  as Map
import           Data.Time.Clock
import           Data.Time.Clock.POSIX
import           Data.Word
import           Types

import           Debug.Trace
import           Prelude                          hiding (scanl1)

instance Stored Point where
    parse = A.parseOnly point

testPointBS :: ByteString
testPointBS = "cpu,region=us-\\ west,host=serverA,env=prod,target=servers,zone=1c value=1"

point :: A.Parser Point
point = do
    name <- nameP <* comma
    keys <- Map.fromList <$> pairsP <* space
    values <- Map.fromList <$> pairsP
    time <- timeP <* A.endOfInput
    return $ Point name keys time values

commaWord :: Word8
commaWord = fromIntegral $ fromEnum ','

spaceWord :: Word8
spaceWord = fromIntegral $ fromEnum ' '

equalWord :: Word8
equalWord = fromIntegral $ fromEnum '='

escapeWord :: Word8
escapeWord = fromIntegral $ fromEnum '\\'

comma :: A.Parser Word8
comma = A.word8 commaWord

space :: A.Parser Word8
space = A.word8 spaceWord

equal :: A.Parser Word8
equal = A.word8 equalWord

nameP :: A.Parser ByteString
nameP = A.takeTill (== commaWord)

pairsP :: A.Parser [(ByteString, ByteString)]
pairsP = keyValue `A.sepBy` comma

keyValue :: A.Parser (ByteString, ByteString)
keyValue = (,) <$> (A.takeTill (== equalWord) <* equal) <*> escapedValue

timeP :: A.Parser (Maybe UTCTime)
timeP = (pure . convertToUTCTime <$> (space *> decimal)) <|> return Nothing

convertToUTCTime :: Integer -> UTCTime
convertToUTCTime = posixSecondsToUTCTime . fromIntegral

escapedValue :: A.Parser ByteString
escapedValue = A.scan False fun
  where
  fun True 44 = Just False
  fun True 32 = Just False
  fun False 44 = Nothing
  fun False 32 = Nothing
  fun _ 92 = Just True
  fun _ _ = Just False

