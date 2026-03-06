-- | Shared types for the write-ahead log: commands, entries, configuration,
-- and database status.
module HsDb.WAL.Types
  ( WALCommand(..)
  , WALEntry(..)
  , WALQueueItem
  , WALHeader(..)
  , walMagic
  , walVersion
  , DatabaseConfig(..)
  , defaultDatabaseConfig
  , DatabaseStatus(..)
  ) where

import Control.Concurrent.STM (TMVar)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BC
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.Vector (Vector)
import Data.Word (Word64)

import HsDb.Types (Value, RowId, Schema, Column)

-- | Commands that can be recorded in the WAL.
data WALCommand
  = CmdCreateTable !Text !Schema
  | CmdInsertRow !Text !RowId !(Vector Value)
  | CmdUpdateRow !Text !RowId !(Vector Value)
  | CmdDeleteRow !Text !RowId
  | CmdDropTable !Text
  | CmdAlterAddColumn !Text !Column
  deriving (Show, Eq)

-- | A WAL entry with sequence number and timestamp, assigned by the flusher.
data WALEntry = WALEntry
  { walSeqNum    :: !Word64
  , walTimestamp :: !UTCTime
  , walCommand   :: !WALCommand
  } deriving (Show, Eq)

-- | An item in the WAL queue: the command to write, plus a callback for
-- durability confirmation.
type WALQueueItem = (WALCommand, TMVar ())

-- | WAL file header.
data WALHeader = WALHeader
  { headerMagic   :: !ByteString  -- ^ 4 bytes: "HSDB"
  , headerVersion :: !Int         -- ^ 2 bytes: format version
  } deriving (Show, Eq)

-- | Magic bytes for the WAL file header.
walMagic :: ByteString
walMagic = BC.pack "HSDB"

-- | Current WAL format version.
walVersion :: Int
walVersion = 1

-- | Configuration for opening a database.
data DatabaseConfig = DatabaseConfig
  { configQueueCapacity :: !Int        -- ^ TBQueue capacity (backpressure)
  , configWALPath       :: !FilePath   -- ^ Path to the WAL file
  , configCheckpointPath :: !(Maybe FilePath)  -- ^ Path to checkpoint file (Nothing = disabled)
  } deriving (Show, Eq)

-- | Default configuration: queue capacity of 1000, no checkpointing.
defaultDatabaseConfig :: FilePath -> DatabaseConfig
defaultDatabaseConfig path = DatabaseConfig
  { configQueueCapacity  = 1000
  , configWALPath        = path
  , configCheckpointPath = Nothing
  }

-- | Current status of the database for write operations.
data DatabaseStatus
  = DbWritable
  | DbReadOnly !Text  -- ^ Reason the DB became read-only
  | DbShuttingDown
  deriving (Show, Eq)
