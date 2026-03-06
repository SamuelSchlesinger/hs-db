module Main where

import System.Exit (exitFailure, exitSuccess)
import Hedgehog

import qualified Test.HsDb.Table as Table
import qualified Test.HsDb.WAL.Serialize as Serialize
import qualified Test.HsDb.WAL.Replay as Replay
import qualified Test.HsDb.Integration as Integration
import qualified Test.HsDb.SQL.Parser as Parser
import qualified Test.HsDb.SQL.Execute as Execute
import qualified Test.HsDb.Checkpoint as Checkpoint
import qualified Test.HsDb.Server as Server
import qualified Test.HsDb.Logging as Logging
import qualified Test.HsDb.Transaction as Transaction

main :: IO ()
main = do
  results <- sequence
    [ checkParallel Table.tableTests
    , checkParallel Serialize.serializeTests
    , checkParallel Replay.replayTests
    , checkParallel Integration.integrationTests
    , checkParallel Parser.parserTests
    , checkParallel Execute.executeTests
    , checkParallel Checkpoint.checkpointTests
    , checkParallel Server.serverTests
    , checkParallel Logging.loggingTests
    , checkParallel Transaction.transactionTests
    ]
  if and results
    then exitSuccess
    else exitFailure
