import std/asyncdispatch

import ./thread_queue
import ./lib/errcode

export asyncdispatch
export errcode

const DefaultAsyncPollMs* = 1

type
  ThreadQueueAsyncError* = object of CatchableError

  AsyncOwned*[T] = ref object
    ## One-shot owned-value container for asyncdispatch integration.
    ##
    ## Future[T] / waitFor cannot be used directly with move-only payloads such
    ## as PoolItem[T], because waitFor returns Future.read() by value and thus
    ## may require a copy.  Future[AsyncOwned[T]] is copyable as a ref, while the
    ## payload inside this box remains move-only and must be extracted with
    ## take().
    active: bool
    value: T

proc newAsyncOwned[T](value: sink T): AsyncOwned[T] =
  new result
  result.active = true
  result.value = move value

proc isActive*[T](self: AsyncOwned[T]): bool {.inline.} =
  if self.isNil:
    return false
  return self.active

proc take*[T](self: AsyncOwned[T]): MoveResult[T, ErrorCode] =
  ## Takes the owned payload out of this async container.
  ##
  ## This operation can be performed only once.  It exists so asyncdispatch can
  ## carry move-only values through Future[AsyncOwned[T]] without requiring
  ## Future[T].read/waitFor to copy T.
  if self.isNil:
    return errMove(ErrorCode.InvalidState)

  if not self.active:
    return errMove(ErrorCode.InvalidState)

  self.active = false
  var value = move self.value
  return okMove(value)

proc recvAsyncOwned*[T](self: ThreadQueue[T]; pollMs: int = DefaultAsyncPollMs): Future[AsyncOwned[T]] =
  ## Receives one value from ThreadQueue[T] from asyncdispatch code.
  ##
  ## This is the first, deliberately simple polling bridge.  It does not use the
  ## {.async.} macro internally because async macro locals are lifted into an
  ## environment object; moving ownership values out of that environment can
  ## require an implicit copy.
  ##
  ## The result is Future[AsyncOwned[T]], not Future[T].  asyncdispatch.waitFor()
  ## returns Future.read() by value, which is not compatible with move-only
  ## payloads such as PoolItem[T].  AsyncOwned[T] is a one-shot ref container:
  ## waitFor copies only the ref, then callers must take() the payload.
  let fut = newFuture[AsyncOwned[T]]("threadtools.recvAsyncOwned")

  if self.isNil:
    fut.fail(newException(ThreadQueueAsyncError, "ThreadQueue.recvAsyncOwned: queue is nil"))
    return fut

  if pollMs < 0:
    fut.fail(newException(ThreadQueueAsyncError, "ThreadQueue.recvAsyncOwned: pollMs must be >= 0"))
    return fut

  proc pollOnce() {.closure, gcsafe.} =
    if fut.finished:
      return

    var value: T
    var ret = self.tryReceive(value)

    if ret.isErr:
      fut.fail(newException(
        ThreadQueueAsyncError,
        "ThreadQueue.recvAsyncOwned: tryReceive failed: " & $ret.error,
      ))
      return

    if ret.get():
      fut.complete(newAsyncOwned(move value))
      return

    if pollMs == 0:
      callSoon(pollOnce)
    else:
      let timer = sleepAsync(pollMs)
      timer.addCallback(proc (_: Future[void]) {.closure, gcsafe.} =
        pollOnce()
      )

  callSoon(pollOnce)
  return fut

proc recvAsync*[T](self: ThreadQueue[T]; pollMs: int = DefaultAsyncPollMs): Future[AsyncOwned[T]] {.inline.} =
  ## Alias for recvAsyncOwned().
  ##
  ## The return type intentionally keeps the owned payload boxed.  Callers must
  ## extract the value with take() after await/waitFor.
  return recvAsyncOwned[T](self, pollMs)
