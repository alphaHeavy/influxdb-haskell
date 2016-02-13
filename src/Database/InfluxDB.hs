module Database.InfluxDB
  (
  -- * Series data types
    Series(..), seriesColumns, seriesPoints
  , SeriesData(..)
  , Value(..)

  -- ** Encoding
  , ToSeriesData(..)
  , ToValue(..)

  -- ** Decoding
  , FromSeries(..), fromSeries
  , FromSeriesData(..), fromSeriesData
  , FromValue(..), fromValue

  , withValues, (.:), (.:?), (.!=)
  , typeMismatch

  -- * HTTP API
  -- ** Data types
  , Config(..)
  , Credentials(..), rootCreds
  , TimePrecision(..)
  , Server(..), localServer
  , ServerPool, newServerPool
  , newServerPoolWithRetryPolicy, newServerPoolWithRetrySettings
  , Database(..)
  , User(..)
  , Admin(..)
  , Ping(..)
  , ShardSpace(..)

  -- *** Exception
  , InfluxException(..)

  -- ** Writing Data
  , formatLine
  -- *** Updating Points
  , post
  , postWithPrecision

  -- *** Deleting Points
  , deleteSeries

  -- ** Querying Data
  , query
  , Stream(..)
  , queryChunked

  -- ** Administration & Security
  -- *** Creating and Dropping Databases
  , listDatabases
  , createDatabase
  , dropDatabase

  -- *** Security
  -- **** Database user
  , listUsers
  , addUser
  , updateUserPassword
  , deleteUser
  , grantAdminPrivilegeTo
  , revokeAdminPrivilegeFrom

  -- *** Other API
  , ping
  ) where

import Database.InfluxDB.Decode
import Database.InfluxDB.Encode
import Database.InfluxDB.Http
import Database.InfluxDB.Types
