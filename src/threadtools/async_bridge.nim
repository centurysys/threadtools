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

  AsyncThreadQueueBridge*[T] = ref object
    ## Event-based asyncdispatch bridge for ThreadQueue[T].
    ##
    ## The bridge must be created and used from the dispatcher thread.  Sender
    ## threads should use AsyncThreadQueueNotifier[T], which contains only a raw
    ## queue handle and a thread-safe AsyncEvent value.
    queue: ThreadQueue[T]
    event: AsyncEvent
    eventActive: bool
    closed: bool
    pending: seq[Future[AsyncOwned[T]]]

  AsyncThreadQueueNotifier*[T] = object
    ## Thread-side notifier for AsyncThreadQueueBridge[T].
    ##
    ## This object intentionally avoids storing AsyncThreadQueueBridge[T] or
    ## ThreadQueue[T] refs.  It is a small handle consisting of the queue raw
    ## handle and the thread-safe AsyncEvent value.
    ##
    ## AsyncEvent is a distinct value type, not a nil-able ref.  `valid` is used
    ## to represent an absent notifier.  `closedPtr` points to the owning
    ## bridge's closed flag so stale notifier handles can reject notify() after
    ## close().
    ##
    ## Closing while sender threads are still active is still a shutdown-order
    ## bug. Stop senders first, then close the bridge.
    queue: ThreadQueueHandle[T]
    event: AsyncEvent
    valid: bool
    closedPtr: ptr bool


# ------------------------------------------------------------------------------
# Small Result helpers:
# ------------------------------------------------------------------------------
proc okBoolResult(): Result[bool, ErrorCode] =
  return ok(true)

proc errBoolResult(code: ErrorCode): Result[bool, ErrorCode] =
  return err(code)

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

proc failFuture[T](fut: Future[AsyncOwned[T]]; message: string) =
  if fut.isNil or fut.finished:
    return
  fut.fail(newException(ThreadQueueAsyncError, message))

proc completeFuture[T](fut: Future[AsyncOwned[T]]; value: sink T) =
  if fut.isNil or fut.finished:
    return
  fut.complete(newAsyncOwned(move value))

# ------------------------------------------------------------------------------
# Polling bridge:
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# AsyncEvent bridge:
# ------------------------------------------------------------------------------
proc failPending[T](self: AsyncThreadQueueBridge[T]; message: string) =
  if self.isNil:
    return

  for fut in self.pending:
    failFuture(fut, message)

  self.pending.setLen(0)

proc drainPending[T](self: AsyncThreadQueueBridge[T]) =
  ## Completes pending futures with values already available in the queue.
  ##
  ## This proc is intended to run only on the asyncdispatch thread.  Sender
  ## threads notify the dispatcher with AsyncThreadQueueNotifier.notify().
  if self.isNil or self.closed:
    return

  while self.pending.len > 0:
    var value: T
    var ret = self.queue.tryReceive(value)

    if ret.isErr:
      self.failPending("AsyncThreadQueueBridge: tryReceive failed: " & $ret.error)
      return

    if not ret.get():
      return

    let fut = self.pending[0]
    self.pending.delete(0)
    completeFuture(fut, move value)

proc newAsyncThreadQueueBridge*[T](queue: ThreadQueue[T]): Result[AsyncThreadQueueBridge[T], ErrorCode] =
  ## Creates an AsyncEvent-based bridge for ThreadQueue[T].
  ##
  ## The bridge registers an AsyncEvent with the current asyncdispatch dispatcher.
  ## It is therefore expected to be created on the thread that owns/runs that
  ## dispatcher.
  if queue.isNil:
    return err(ErrorCode.InvalidState)

  var bridge: AsyncThreadQueueBridge[T]
  new bridge
  bridge.queue = queue
  bridge.closed = false
  bridge.pending = @[]

  try:
    bridge.event = newAsyncEvent()
    bridge.eventActive = true
    let captured = bridge
    addEvent(bridge.event, proc (_: AsyncFD): bool {.closure, gcsafe.} =
      {.cast(gcsafe).}:
        captured.drainPending()
      return false # keep the event registered
    )
  except OSError:
    return err(ErrorCode.ChannelError)

  return ok(bridge)

proc notifier*[T](self: AsyncThreadQueueBridge[T]): AsyncThreadQueueNotifier[T] {.inline.} =
  ## Returns a small sender-side notifier handle.
  ##
  ## The returned handle is valid only while this bridge and its source
  ## ThreadQueue[T] are alive.  Existing notifier values observe bridge.close()
  ## through a raw closed flag pointer and reject notify()/sendMove() after close.
  if self.isNil or self.closed or not self.eventActive:
    return AsyncThreadQueueNotifier[T](queue: nil, valid: false, closedPtr: nil)

  return AsyncThreadQueueNotifier[T](
    queue: self.queue.handle,
    event: self.event,
    valid: true,
    closedPtr: addr self.closed,
  )

proc isValid*[T](self: AsyncThreadQueueNotifier[T]): bool {.inline.} =
  ## Returns true while this notifier can wake the bridge.
  ##
  ## This is intended for shutdown checks.  It does not make concurrent close vs
  ## send a supported operation; sender threads should be stopped before close().
  if not self.valid or self.queue == nil:
    return false

  if self.closedPtr != nil and self.closedPtr[]:
    return false

  if self.queue.isClosed:
    return false

  return true

proc notify*[T](self: AsyncThreadQueueNotifier[T]): Result[bool, ErrorCode] =
  ## Wakes the asyncdispatch thread after a value has been sent to the queue.
  if not self.valid or self.queue == nil:
    return err(ErrorCode.InvalidState)

  if self.closedPtr != nil and self.closedPtr[]:
    return err(ErrorCode.Closed)

  if self.queue.isClosed:
    return err(ErrorCode.Closed)

  try:
    trigger(self.event)
  except OSError:
    return err(ErrorCode.ChannelError)

  return ok(true)

template sendMove*[T](self: AsyncThreadQueueNotifier[T]; valueExpr: untyped): Result[bool, ErrorCode] =
  ## Sends a value to the underlying ThreadQueue and wakes the async bridge.
  ##
  ## This is the sender-side convenience API.  It preserves sendMove() ownership
  ## transfer semantics and triggers the bridge AsyncEvent only after a
  ## successful queue send.
  block:
    let n = self

    var ret: Result[bool, ErrorCode]

    if not n.valid or n.queue == nil:
      ret = errBoolResult(ErrorCode.InvalidState)
    elif n.closedPtr != nil and n.closedPtr[]:
      ret = errBoolResult(ErrorCode.Closed)
    elif n.queue.isClosed:
      ret = errBoolResult(ErrorCode.Closed)
    else:
      ret = n.queue.sendMove(valueExpr)
      if ret.isOk:
        ret = n.notify()

    ret

proc recvAsyncOwned*[T](self: AsyncThreadQueueBridge[T]): Future[AsyncOwned[T]] =
  ## Receives one value through the AsyncEvent bridge.
  ##
  ## No polling timer is used.  The returned Future is completed when either a
  ## value is already available in the queue or a sender calls notify()/sendMove()
  ## on the corresponding AsyncThreadQueueNotifier[T].
  let fut = newFuture[AsyncOwned[T]]("threadtools.AsyncThreadQueueBridge.recvAsyncOwned")

  if self.isNil:
    fut.fail(newException(ThreadQueueAsyncError, "AsyncThreadQueueBridge.recvAsyncOwned: bridge is nil"))
    return fut

  if self.closed:
    fut.fail(newException(ThreadQueueAsyncError, "AsyncThreadQueueBridge.recvAsyncOwned: bridge is closed"))
    return fut

  self.pending.add(fut)
  self.drainPending()
  return fut

proc recvAsync*[T](self: AsyncThreadQueueBridge[T]): Future[AsyncOwned[T]] {.inline.} =
  return recvAsyncOwned[T](self)

proc cancelPending*[T](self: AsyncThreadQueueBridge[T]; message = "AsyncThreadQueueBridge: pending receive cancelled"): int =
  ## Fails all currently pending recvAsync() futures without closing the bridge.
  ##
  ## This is useful for higher-level shutdown logic that wants to abort current
  ## waits but continue using the bridge afterwards.
  if self.isNil:
    return 0

  result = self.pending.len
  self.failPending(message)

proc close*[T](self: AsyncThreadQueueBridge[T]) =
  ## Closes the bridge and fails pending futures.
  ##
  ## This does not close the underlying ThreadQueue[T].  It unregisters and
  ## closes the AsyncEvent owned by this bridge, and any existing notifier handle
  ## will observe the closed flag and reject notify()/sendMove().
  if self.isNil or self.closed:
    return

  self.closed = true
  discard self.cancelPending("AsyncThreadQueueBridge: bridge is closed")

  if self.eventActive:
    try:
      asyncdispatch.unregister(self.event)
    except OSError:
      discard

    try:
      asyncdispatch.close(self.event)
    except OSError:
      discard

    self.eventActive = false

