{-# LANGUAGE OverloadedStrings #-}
module Main where

import           Control.Monad.IO.Class
import           Data.Aeson
import qualified Database.LevelDB.Higher as LDB
import           Point
import           Store
import           Types
import           Web.Spock.Safe
import qualified Control.Monad.State.Strict as ST

conf = Config "./databases" (DBName "base")

db = DBName "db"

main :: IO ()
main = do
    let db = PCConn (ConnBuilder (open conf) (close) (PoolCfg 1 1 100))
    let sessions = SessionCfg "app" 100 0 False "hello" Nothing
    runSpock 8080 $ spock sessions db conf web

type App = SpockM DB String Config ()

web :: App
web = do
    post "/write" $ do
        b <- body
        let Right p = parse b :: Either String Point
        result <- runQuery (write p)
        liftIO $ print result
        bytes b
    get "/query" $ do
        q <- param' "q"
        result <- runQuery (query q)
        liftIO $ print result


