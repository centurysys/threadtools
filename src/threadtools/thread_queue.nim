import std/isolation
import threading/channels

import ./lib/errcode

export errcode

type
  ThreadQueueObj[T] = object
    ch: Chan[T]
    closed: bool

  ThreadQueue*[T] = ref ThreadQueueObj[T]

  ## Non-GC handle used by move-only payloads that must cross thread/channel
  ## boundaries.  A PoolItem must not contain ThreadQueue[T] directly because it
  ## is a ref object and therefore cannot be isolated by threading/channels.
  ##
  ## The handle is valid only while the original ThreadQueue[T] object is kept
  ## alive by the owner.  This is the same lifetime requirement as passing queue
  ## references to worker contexts.
  ThreadQueueHandle*[T] = ptr ThreadQueueObj[T]

# ------------------------------------------------------------------------------
# Small Result helpers:
# ------------------------------------------------------------------------------
proc okBool(): Result[bool, ErrorCode] =
  return ok(true)

proc errBool(code: ErrorCode): Result[bool, ErrorCode] =
  return err(code)

# ------------------------------------------------------------------------------
# Constructor:
# ------------------------------------------------------------------------------
proc newThreadQueue*[T](queuelen: int): Result[ThreadQueue[T], ErrorCode] =
  if queuelen <= 0:
    return err(ErrorCode.InvalidState)

  var queue: ThreadQueue[T]
  new queue
  queue.ch = newChan[T](queuelen)
  queue.closed = false

  return ok(queue)

# ------------------------------------------------------------------------------
# Raw handle:
# ------------------------------------------------------------------------------
proc handle*[T](self: ThreadQueue[T]): ThreadQueueHandle[T] {.inline.} =
  ## Returns a non-GC handle to this queue.
  ##
  ## This is intended for ownership tokens such as PoolItem[T].  Storing the
  ## ThreadQueue[T] ref directly inside such a token prevents std/isolation from
  ## proving that the token can be sent through threading/channels.
  if self.isNil:
    return nil

  return unsafeAddr self[]

# ------------------------------------------------------------------------------
# State:
# ------------------------------------------------------------------------------
proc isClosed*[T](self: ThreadQueue[T]): bool {.inline.} =
  if self.isNil:
    return true

  return self.closed

proc isClosed*[T](self: ThreadQueueHandle[T]): bool {.inline.} =
  if self == nil:
    return true

  return self[].closed

proc close*[T](self: ThreadQueue[T]) =
  if self.isNil:
    return

  self.closed = true

proc close*[T](self: ThreadQueueHandle[T]) =
  if self == nil:
    return

  self[].closed = true

# ------------------------------------------------------------------------------
# Send:
# ------------------------------------------------------------------------------
template sendToChanMove(chExpr: untyped; data: untyped): untyped =
  block:
    var owned = ensureMove(data)
    var isolated = isolate(owned)
    chExpr.send(isolated)

template sendMove*[T](self: ThreadQueue[T], data: untyped): Result[bool, ErrorCode] =
  ## Transfers ownership of `data` into the queue.
  ##
  ## `ensureMove()` is used at the API boundary so accidental implicit copies are
  ## rejected at compile time.  When sending an object field, pass it explicitly
  ## as `move field` because Nim 2.2 rejects `ensureMove(object.field)`.
  block:
    let queue = self
    var ret: Result[bool, ErrorCode]

    if queue.isNil:
      ret = errBool(ErrorCode.InvalidState)
    elif queue.closed:
      ret = errBool(ErrorCode.Closed)
    else:
      sendToChanMove(queue.ch, data)
      ret = okBool()

    ret

template sendMove*[T](self: ThreadQueueHandle[T], data: untyped): Result[bool, ErrorCode] =
  ## Transfers ownership of `data` into a queue referenced by a raw handle.
  ##
  ## This overload exists for PoolItem[T], which must not store ThreadQueue[T]
  ## directly because ThreadQueue[T] is a ref object and breaks isolation.
  block:
    let queue = self
    var ret: Result[bool, ErrorCode]

    if queue == nil:
      ret = errBool(ErrorCode.InvalidState)
    elif queue[].closed:
      ret = errBool(ErrorCode.Closed)
    else:
      sendToChanMove(queue[].ch, data)
      ret = okBool()

    ret

template send*[T](self: ThreadQueue[T], data: untyped): Result[bool, ErrorCode] =
  ## Backward-compatible alias for sendMove().
  ##
  ## New ownership-transfer call sites should prefer sendMove() so the transfer
  ## is visible at the call site.
  sendMove(self, data)

template send*[T](self: ThreadQueueHandle[T], data: untyped): Result[bool, ErrorCode] =
  sendMove(self, data)

proc sendCopy*[T](self: ThreadQueue[T], data: T): Result[bool, ErrorCode] =
  ## Sends by normal value semantics.
  ##
  ## This is intentionally separate from sendMove().  It should only be used for
  ## small/copyable values.  For move-only payloads, this proc should fail to
  ## compile because their `=copy` is disabled.
  if self.isNil:
    return err(ErrorCode.InvalidState)

  if self.closed:
    return err(ErrorCode.Closed)

  self.ch.send(data)

  return ok(true)

# ------------------------------------------------------------------------------
# Receive:
# ------------------------------------------------------------------------------
proc receive*[T](self: ThreadQueue[T]): T =
  doAssert not self.isNil, "ThreadQueue.receive: self is nil"
  doAssert not self.closed, "ThreadQueue.receive: queue is closed"

  var value = self.ch.recv()
  return move value

proc get*[T](self: ThreadQueue[T]): T =
  return self.receive()

proc receiveChecked*[T](self: ThreadQueue[T]): MoveResult[T, ErrorCode] =
  ## Blocking receive with error reporting.
  ##
  ## The received value is returned through MoveResult so callers must take()/?.
  if self.isNil:
    return errMove(ErrorCode.InvalidState)

  if self.closed:
    return errMove(ErrorCode.Closed)

  var value = self.ch.recv()
  return okMove(value)

proc receiveResult*[T](self: ThreadQueue[T]): MoveResult[T, ErrorCode] =
  return self.receiveChecked()

# ------------------------------------------------------------------------------
# Try receive:
# ------------------------------------------------------------------------------
proc tryReceive*[T](self: ThreadQueue[T], data: var T): Result[bool, ErrorCode] =
  ## Non-blocking receive into caller-owned storage.
  ##
  ## This API deliberately avoids Option[T]/Result[T, E] wrappers around T.
  if self.isNil:
    return err(ErrorCode.InvalidState)

  if self.closed:
    return err(ErrorCode.Closed)

  return ok(self.ch.tryRecv(data))

proc tryGet*[T](self: ThreadQueue[T], data: var T): Result[bool, ErrorCode] =
  return self.tryReceive(data)

proc tryReceiveMove*[T](self: ThreadQueue[T]): MoveResult[MoveOption[T], ErrorCode] =
  ## Non-blocking receive returning a take-only option.
  ##
  ## Invalid/closed queue state is reported as Err.  Empty queue is reported as
  ## Ok(None).  A value is reported as Ok(Some(value)).
  if self.isNil:
    return errMove(ErrorCode.InvalidState)

  if self.closed:
    return errMove(ErrorCode.Closed)

  var value: T
  if self.ch.tryRecv(value):
    var opt = someMove(value)
    return okMove(opt)

  var opt = noneMove(T)
  return okMove(opt)

proc tryReceiveChecked*[T](self: ThreadQueue[T]): MoveResult[T, ErrorCode] =
  ## Non-blocking receive returning Err(Empty) when no value is available.
  if self.isNil:
    return errMove(ErrorCode.InvalidState)

  if self.closed:
    return errMove(ErrorCode.Closed)

  var value: T
  if self.ch.tryRecv(value):
    return okMove(value)

  return errMove(ErrorCode.Empty)
