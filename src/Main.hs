{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE TemplateHaskell #-}
module Main where

-- import Database.LevelDB.Higher
import Types
import Point
import Web.Spock.Safe
import Control.Monad.IO.Class
-- import Control.Monad.Trans.Ether.State.Strict
-- import Control.Monad.Ether.State.Class
-- import Control.Ether.TH
import Control.Monad.State
import qualified Control.Monad.State as S


data ST = ST
  { name :: String
  , age  :: Integer
  } deriving (Show, Eq)

-- ethereal "App" "app"
-- ethereal "DB" "db"

def = ST "hello" 1

main :: IO ()
main = runSpock 8080 $ spock ss poc def web

type App = SpockM String String ST ()

web :: App 
web = do
    post "/write" $ do
        b <- body
        liftIO $ print (parse b :: Either String Point)
        st <- getState
        let newst = st { age = (age st) + 1 }
        liftIO $ print st
        bytes b
