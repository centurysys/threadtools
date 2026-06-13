import results
import move_results

export results
export move_results

type
  ErrorCode* {.pure.} = enum
    InvalidState = (1, "Invalid State")
    Closed = (2, "Closed")
    Timeouted = (3, "Timeouted")
    Cancelled = (4, "Cancelled")
    Full = (5, "Queue Full")
    Empty = (6, "Queue Empty")
    PoolExhausted = (7, "Pool Exhausted")
    PoolInvariantBroken = (8, "Pool Invariant Broken")
    DoubleRelease = (9, "Double Release")
    WrongPool = (10, "Wrong Pool")
    ChannelError = (11, "Channel Error")
    ThreadStartFailed = (12, "Thread Start Failed")
    Unsupported = (13, "Unsupported")
    Bug = (14, "Bug")

  ## Use this only for small/copyable result payloads, such as bool/ref handles.
  ## For owned payloads, prefer ThreadtoolsMoveResult[T].
  ThreadtoolsResult*[T] = Result[T, ErrorCode]

  ## Take-only result for owned payloads such as buffers, frames, packets,
  ## PoolItem[T], or other value objects that must not be copied.
  ThreadtoolsMoveResult*[T] = MoveResult[T, ErrorCode]

  ## Take-only option for owned payloads where absence is not an error.
  ThreadtoolsMoveOption*[T] = MoveOption[T]
