{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}
module Database.InfluxDB.Http
  ( Config(..)
  , Credentials(..), rootCreds
  , Server(..), localServer
  , TimePrecision(..)

  -- * Writing Data
  , formatLine
  , formatLines
  -- ** Updating Points
  , post
  , postWithPrecision

  -- ** Deleting Points
  , deleteSeries

  -- * Querying Data
  , query
  , Stream(..)
  , queryChunked

  -- * Administration & Security
  -- ** Creating and Dropping Databases
  , listDatabases
  , createDatabase
  , dropDatabase


  -- ** Security
  -- *** Database user
  , listUsers
  , addUser
  , updateUserPassword
  , deleteUser
  , grantAdminPrivilegeTo
  , revokeAdminPrivilegeFrom

  -- ** Other API
  , ping
  ) where

import Control.Applicative
import Control.DeepSeq
import Control.Monad.Identity
import Data.IORef
import Data.Maybe (fromJust)
import Data.Text (Text)
import Network.URI (escapeURIString, isAllowedInURI)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Lazy as BL
import qualified Data.List as L
import Data.Map (Map)
import qualified Data.Map as M
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Text.Printf (printf)
import Prelude

import Control.Monad.Catch (Handler(..))
import Control.Retry
import Data.Default.Class (Default(def))
import qualified Data.Aeson as A
import qualified Data.Attoparsec.ByteString as P
import qualified Network.HTTP.Client as HC

import Database.InfluxDB.Decode
import Database.InfluxDB.Types
import Database.InfluxDB.Stream (Stream(..))
import qualified Database.InfluxDB.Stream as S

import GHC.Generics

-- | Configurations for HTTP API client.
data Config = Config
  { configCreds :: !Credentials
  , configServerPool :: !(IORef ServerPool)
  , configHttpManager :: !HC.Manager
  }

-- | Default credentials.
rootCreds :: Credentials
rootCreds = Credentials
  { credsUser = "root"
  , credsPassword = "root"
  }

-- | Default server location.
localServer :: Server
localServer = Server
  { serverHost = "localhost"
  , serverPort = 8086
  , serverSsl = False
  }

data TimePrecision
  = HoursPrecision
  | MinutesPrecision
  | SecondsPrecision
  | MillisecondsPrecision
  | MicrosecondsPrecision
  | NanosecondsPrecision

timePrecString :: TimePrecision -> String
timePrecString HoursPrecision = "h"
timePrecString MinutesPrecision = "m"
timePrecString SecondsPrecision = "s"
timePrecString MillisecondsPrecision = "ms"
timePrecString MicrosecondsPrecision = "u"
timePrecString NanosecondsPrecision = "n"

-----------------------------------------------------------
-- Writing Data

data Line = Line {
  lineMeasurement :: Text,
  lineTags :: Map Text Text,
  lineFields :: Map Text Value,
  lineTime :: Maybe Integer
} deriving (Show,Eq,Generic)

instance NFData Line

formatLine :: Line -> BL.ByteString
formatLine line = BL.concat[BL.fromStrict $ TE.encodeUtf8 $ lineMeasurement line,formatedTags, " ", formatedValues, maybe "" (\ x -> BL.fromStrict $ BS8.concat[" ", BS8.pack $ show x]) $ lineTime line]
  where
    formatedTags =
      case M.null $ lineTags line of
        True -> ""
        False -> BL.concat $ [","] ++ (L.intersperse "," $ fmap (\ (key,value) -> BL.fromStrict $ TE.encodeUtf8 $ T.concat [key,"=",value] ) $ M.toList $ lineTags line)
    formatedValues = BL.concat $ L.intersperse "," $ fmap (\ (key,value) -> BL.concat[BL.fromStrict $ TE.encodeUtf8 key,"=",BL.fromStrict $ formatValue value]) $ M.toList $ lineFields line

    formatValue (Int val) = BS8.pack $ concat[show val,"i"]
    formatValue (String val) = BS8.pack $ concat ["\"",T.unpack val,"\""]
    formatValue (Float val) = BS8.pack $ show val
    formatValue (Bool True) = BS8.pack "t"
    formatValue (Bool False) = BS8.pack "f"

formatLines :: [Line] -> BL.ByteString
formatLines x = BL.concat $ L.intersperse "\n" $ fmap formatLine x


-- | Post a bunch of writes for (possibly multiple) series into a database.
post
  :: Config
  -> Text
  -> [Line]
  -> IO ()
post config databaseName d =
  postGeneric config databaseName Nothing d

-- | Post a bunch of writes for (possibly multiple) series into a database like
-- 'post' but with time precision.
postWithPrecision
  :: Config
  -> Text -- ^ Database name
  -> TimePrecision
  -> [Line]
  -> IO ()
postWithPrecision config databaseName precision =
  postGeneric config databaseName (Just precision)

postGeneric
  :: Config
  -> Text -- ^ Database name
  -> Maybe TimePrecision
  -> [Line]
  -> IO ()
postGeneric Config {..} databaseName precision write = do
  void $ httpLbsWithRetry configServerPool
    (makeRequest write)
    configHttpManager
  return ()
  where
    makeRequest series = def
      { HC.method = "POST"
      , HC.requestBody = HC.RequestBodyLBS $ formatLines series
      , HC.path = "/write"
      , HC.queryString = printFunc
      }
    Credentials {..} = configCreds
    formatString = maybe "u=%s&p=%s&db=%s" (\ _ ->  "u=%s&p=%s&db=%s&precision=%s") precision
    printFunc =
      case precision of
        Just _ -> escapeString $ printf formatString
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
          (timePrecString $ fromJust precision)
        Nothing -> escapeString $ printf formatString
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)

deleteSeries
  :: Config
  -> Text -- ^ Database name
  -> Text -- ^ Series name
  -> IO ()
deleteSeries config databaseName seriesName = runRequest_ config request
  where
    request = def
      { HC.method = "DELETE"
      , HC.path = escapeString $ printf "/db/%s/series/%s"
          (T.unpack databaseName)
          (T.unpack seriesName)
      , HC.queryString = escapeString $ printf "u=%s&p=%s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds config

-----------------------------------------------------------
-- Querying Data

-- | Query a specified database.
--
-- The query format is specified in the
-- <http://influxdb.org/docs/query_language/ InfluxDB Query Language>.
query
  :: Config
  -> Text -- ^ Database name
  -> Text -- ^ Query text
  -> IO Results
query config databaseName q = do
  runRequest config request
  where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&db=%s&q=%s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
          (T.unpack q)
      }
    Credentials {..} = configCreds config

-- | Construct streaming output
responseStream :: A.FromJSON a => HC.BodyReader -> IO (Stream IO a)
responseStream body = demandPayload $ \payload ->
  if BS.null payload
    then return Done
    else decode $ parseAsJson payload
  where
    demandPayload k = HC.brRead body >>= k
    decode (P.Done leftover value) = case A.fromJSON value of
      A.Success a -> return $ Yield a $ if BS.null leftover
        then responseStream body
        else decode $ parseAsJson leftover
      A.Error message -> jsonDecodeError message
    decode (P.Partial k) = demandPayload (decode . k)
    decode (P.Fail _ _ message) = jsonDecodeError message
    parseAsJson = P.parse A.json

-- | Query a specified database like 'query' but in a streaming fashion.
queryChunked
  :: FromSeries a
  => Config
  -> Text -- ^ Database name
  -> Text -- ^ Query text
  -> (Stream IO a -> IO b)
  -- ^ Action to handle the resulting stream of series
  -> IO b
queryChunked Config {..} databaseName q f =
  withPool configServerPool request $ \request' ->
    HC.withResponse request' configHttpManager $
      responseStream . HC.responseBody >=> S.mapM parse >=> f
  where
    parse series = case fromSeries series of
      Left reason -> seriesDecodeError reason
      Right a -> return a
    request = def
      { HC.path = escapeString $ printf "/db/%s/series"
          (T.unpack databaseName)
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=%s&chunked=true"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack q)
      }
    Credentials {..} = configCreds

-----------------------------------------------------------
-- Administration & Security

-- | List existing databases.
listDatabases :: Config -> IO [Database]-- [Database]
listDatabases Config {..} = do
  response <- httpLbsWithRetry configServerPool request configHttpManager
  let parsed :: Maybe Results = A.decode $ HC.responseBody response
  case parsed of
    Just x ->
      return $ L.concat $ fmap (\ y ->
        case serieswrapperSeries y of
          Just z -> L.concat $ fmap (fmap Database . convertList . L.concat . newseriesValues) z
          Nothing -> []) $ resultsResults x
    Nothing -> return []
    where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SHOW DATABASES"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds

    convertList :: [Value] -> [Text]
    convertList = fmap (\ (String x) -> x) . L.filter predicate

    predicate (String _) = True
    predicate _ = False


-- | Create a new database. Requires cluster admin privileges.
createDatabase :: Config -> Text -> IO ()
createDatabase config name = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=CREATE DATABASE %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack name)
      }
    Credentials {..} = configCreds config

-- | Drop a database. Requires cluster admin privileges.
dropDatabase
  :: Config
  -> Text -- ^ Database name
  -> IO ()
dropDatabase config databaseName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=DROP DATABASE %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack databaseName)
      }
    Credentials {..} = configCreds config

-- | List database users.
listUsers
  :: Config
  -> IO [User]
listUsers _config@Config {..} = do
  response <- httpLbsWithRetry configServerPool request configHttpManager
  let parsed :: Maybe Results = A.decode $ HC.responseBody response
  case parsed of
    Just x ->
      return $ L.concat $ fmap (\ y ->
        case serieswrapperSeries y of
          Just z -> L.concat $ fmap (fmap(\ (a,b) -> User a b) . fmap convertList . newseriesValues) z
          Nothing -> []) $ resultsResults x
    Nothing -> return []
    where
    request = def
      { HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SHOW USERS"
          (T.unpack credsUser)
          (T.unpack credsPassword)
      }
    Credentials {..} = configCreds

    convertList :: [Value] -> (Text,Bool)
    convertList [String x, Bool y] = (x,y)

-- | Add an user to the database users.
addUser
  :: Config
  -> Text -- ^ User name
  -> Text -- ^ Password
  -> IO ()
addUser config name password = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=CREATE USER %s WITH PASSWORD '%s'"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack name)
          (T.unpack $ T.replace "'" "\'" password)
      }
    Credentials {..} = configCreds config

-- | Delete an user from the database users.
deleteUser
  :: Config
  -> Text -- ^ User name
  -> IO ()
deleteUser config userName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=DROP USER %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

-- | Update password for the database user.
updateUserPassword
  :: Config
  -> Text -- ^ User name
  -> Text -- ^ New password
  -> IO ()
updateUserPassword config userName password =
  runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=SET PASSWORD FOR %s = '%s'"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
          (T.unpack $ T.replace "'" "\'" password)
      }
    Credentials {..} = configCreds config

-- | Give admin privilege to the user.
grantAdminPrivilegeTo
  :: Config
  -> Text -- ^ User name
  -> IO ()
grantAdminPrivilegeTo config userName = runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=GRANT ALL PRIVILEGES TO %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

-- | Remove admin privilege from the user.
revokeAdminPrivilegeFrom
  :: Config
  -> Text -- ^ User name
  -> IO ()
revokeAdminPrivilegeFrom config userName =
  runRequest_ config request
  where
    request = def
      { HC.method = "GET"
      , HC.path = "/query"
      , HC.queryString = escapeString $ printf "u=%s&p=%s&q=REVOKE ALL PRIVILEGES FROM %s"
          (T.unpack credsUser)
          (T.unpack credsPassword)
          (T.unpack userName)
      }
    Credentials {..} = configCreds config

ping :: Config -> IO ()
ping config = runRequest config request
  where
    request = def
      { HC.path = "/ping"
      }

-----------------------------------------------------------

httpLbsWithRetry
  :: IORef ServerPool
  -> HC.Request
  -> HC.Manager
  -> IO (HC.Response BL.ByteString)
httpLbsWithRetry pool request manager =
  withPool pool request $ \request' ->
    HC.httpLbs request' manager

withPool
  :: IORef ServerPool
  -> HC.Request
  -> (HC.Request -> IO a)
  -> IO a
withPool pool request f = do
  policy <- serverRetryPolicy <$> readIORef pool
  recovering policy handlers $ \ _ -> do
    server <- activeServer pool
    f $ makeRequest server
  where
    makeRequest Server {..} = request
      { HC.host = escapeText serverHost
      , HC.port = serverPort
      , HC.secure = serverSsl
      }
    handlers =
      [ const $ Handler $ \e -> case e of
        HC.FailedConnectionException {} -> retry
        HC.FailedConnectionException2 {} -> retry
        HC.InternalIOException {} -> retry
        HC.ResponseTimeout {} -> retry
        _ -> return False
      ]
    retry = True <$ failover pool

escapeText :: Text -> BS.ByteString
escapeText = escapeString . T.unpack

escapeString :: String -> BS.ByteString
escapeString = BS8.pack . escapeURIString isAllowedInURI

decodeJsonResponse
  :: A.FromJSON a
  => HC.Response BL.ByteString
  -> IO a
decodeJsonResponse response =
  case A.eitherDecode (HC.responseBody response) of
    Left reason -> jsonDecodeError reason
    Right a -> return a

runRequest :: A.FromJSON a => Config -> HC.Request -> IO a
runRequest Config {..} req = do
  response <- httpLbsWithRetry configServerPool req configHttpManager
  decodeJsonResponse response

runRequest_ :: Config -> HC.Request -> IO ()
runRequest_ Config {..} req =
  void $ httpLbsWithRetry configServerPool req configHttpManager

-----------------------------------------------------------
