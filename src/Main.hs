{-# LANGUAGE OverloadedStrings #-}
module Main where

import Types
import Point
import Store
import Web.Spock.Safe
import Control.Monad.IO.Class
import Data.Aeson
import qualified Database.LevelDB.Higher  as LDB

conf = Config "./databases"

db = DBName "db"

main :: IO ()
main = do
    let db = PCConn (ConnBuilder (open conf :: IO DB) close (PoolCfg 1 1 100))
    let sessions = SessionCfg "app" 100 0 False "hello" Nothing
    runSpock 8080 $ spock sessions db conf web

type App = SpockM DB String Config ()

web :: App 
web = do
    post "/write" $ do
        b <- body
        let Right p = parse b :: Either String Point
        result <- runQuery (write db p)
        liftIO $ print result
        bytes b
    get "/query" $ do
        q <- param' "q"
        result <- runQuery (query db q)
        liftIO $ print result


