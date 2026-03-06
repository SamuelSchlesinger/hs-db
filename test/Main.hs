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
    ]
  if and results
    then exitSuccess
    else exitFailure
