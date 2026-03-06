{-# LANGUAGE ForeignFunctionInterface #-}

-- | Shared low-level IO operations (e.g. fsync via FFI).
module HsDb.IO
  ( fsyncFd
  ) where

import Control.Monad (when)
import Foreign.C.Types (CInt(..))
import System.IO.Error (mkIOError, illegalOperationErrorType)
import System.Posix.Types (Fd(..))

foreign import ccall "fsync" c_fsync :: CInt -> IO CInt

-- | Fsync a file descriptor, throwing an IOError on failure.
fsyncFd :: Fd -> IO ()
fsyncFd (Fd fd) = do
  result <- c_fsync fd
  when (result /= 0) $
    throwIO (mkIOError illegalOperationErrorType "fsync failed" Nothing Nothing)
  where
    throwIO = ioError
