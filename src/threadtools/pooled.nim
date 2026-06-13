import std/isolation

import ./thread_queue
import ./pool_item
import ./lib/errcode

export errcode

type
  Pool*[T] = object
    ## Named wrapper around ThreadQueue[T] used as a free-item pool.
    ##
    ## Pool[T] contains unused values.  acquire() removes a T from this queue and
    ## returns it as Pooled[T], which remembers this Pool as its return path.
    freeQ: ThreadQueue[T]

  Pooled*[T] = PoolItem[T]
    ## Public, user-facing name for PoolItem[T].
    ##
    ## A Pooled[T] is a move-only ownership token.  When released or destroyed
    ## while active, it returns its T to the Pool[T] it came from.

  PooledQueue*[T] = object
    ## Named wrapper around ThreadQueue[Pooled[T]].
    ##
    ## This is a communication path for active pooled items, not the pool itself.
    q: ThreadQueue[Pooled[T]]

# ------------------------------------------------------------------------------
# Pool constructor and raw access:
# ------------------------------------------------------------------------------
proc newPool*[T](capacity: int): Result[Pool[T], ErrorCode] =
  ## Creates an empty pool with room for `capacity` values.
  ##
  ## Use addMove() to pre-fill the pool with reusable values.
  let q = newThreadQueue[T](capacity)
  if q.isErr:
    return err(q.error)

  return ok(Pool[T](freeQ: q.get()))

proc freeQueue*[T](self: Pool[T]): ThreadQueue[T] {.inline.} =
  ## Returns the underlying free-item queue.
  ##
  ## This is mostly useful when integrating with lower-level APIs.  Normal code
  ## should prefer addMove(), acquire(), and tryAcquire().
  return self.freeQ

proc isClosed*[T](self: Pool[T]): bool {.inline.} =
  if self.freeQ.isNil:
    return true

  return self.freeQ.isClosed

proc close*[T](self: Pool[T]) =
  if self.freeQ.isNil:
    return

  self.freeQ.close()

# ------------------------------------------------------------------------------
# Pool add/acquire:
# ------------------------------------------------------------------------------
proc addPoolMove[T](self: Pool[T]; value: sink T): Result[bool, ErrorCode] =
  if self.freeQ.isNil:
    return err(ErrorCode.InvalidState)

  return self.freeQ.sendMove(move value)

template addMove*[T](self: Pool[T]; valueExpr: untyped): Result[bool, ErrorCode] =
  ## Adds a value to the pool by consuming `valueExpr`.
  ##
  ## After this call succeeds, the source value must not be used again.
  addPoolMove[T](self, ensureMove(valueExpr))

template add*[T](self: Pool[T]; valueExpr: untyped): Result[bool, ErrorCode] =
  ## Alias for addMove().
  addMove(self, valueExpr)

proc acquire*[T](self: Pool[T]): Pooled[T] =
  ## Blocking acquire from the pool.
  ##
  ## The returned Pooled[T] remembers this pool as its return path.
  doAssert not self.freeQ.isNil, "Pool.acquire: pool is invalid"
  doAssert not self.freeQ.isClosed, "Pool.acquire: pool is closed"

  var value = self.freeQ.receive()
  var item = newPoolItem[T](self.freeQ.handle, move value)
  return move item

proc tryAcquire*[T](self: Pool[T]; item: var Pooled[T]): Result[bool, ErrorCode] =
  ## Non-blocking acquire from the pool.
  ##
  ## If `item` is already active, this returns InvalidState rather than
  ## overwriting it and accidentally returning the previous value by assignment.
  if self.freeQ.isNil:
    return err(ErrorCode.InvalidState)

  if item.isActive:
    return err(ErrorCode.InvalidState)

  var value: T
  let ret = self.freeQ.tryReceive(value)
  if ret.isErr:
    return ret

  if not ret.get():
    return ok(false)

  var newItem = newPoolItem[T](self.freeQ.handle, move value)
  item = move newItem
  return ok(true)

# ------------------------------------------------------------------------------
# Pooled value access aliases:
# ------------------------------------------------------------------------------
proc value*[T](self: var Pooled[T]): var T {.inline.} =
  ## User-facing alias for PoolItem.item.
  result = self.item

proc payload*[T](self: var Pooled[T]): var T {.inline.} =
  ## Alias for value().
  result = self.item

# ------------------------------------------------------------------------------
# PooledQueue constructor and raw access:
# ------------------------------------------------------------------------------
proc newPooledQueue*[T](capacity: int): Result[PooledQueue[T], ErrorCode] =
  ## Creates a queue for moving Pooled[T] values between threads.
  let q = newThreadQueue[Pooled[T]](capacity)
  if q.isErr:
    return err(q.error)

  return ok(PooledQueue[T](q: q.get()))

proc queue*[T](self: PooledQueue[T]): ThreadQueue[Pooled[T]] {.inline.} =
  ## Returns the underlying queue.
  ##
  ## This is useful for lower-level integrations such as AsyncThreadQueueBridge.
  return self.q

proc isClosed*[T](self: PooledQueue[T]): bool {.inline.} =
  if self.q.isNil:
    return true

  return self.q.isClosed

proc close*[T](self: PooledQueue[T]) =
  if self.q.isNil:
    return

  self.q.close()

# ------------------------------------------------------------------------------
# PooledQueue send/receive:
# ------------------------------------------------------------------------------
proc sendPooledMove[T](self: PooledQueue[T]; item: sink Pooled[T]): Result[bool, ErrorCode] =
  if self.q.isNil:
    return err(ErrorCode.InvalidState)

  return self.q.sendMove(move item)

template sendMove*[T](self: PooledQueue[T]; itemExpr: untyped): Result[bool, ErrorCode] =
  ## Transfers ownership of a Pooled[T] into this queue.
  ##
  ## After this call succeeds, the source item must not be used again.
  sendPooledMove[T](self, ensureMove(itemExpr))

template send*[T](self: PooledQueue[T]; itemExpr: untyped): Result[bool, ErrorCode] =
  ## Alias for sendMove().
  sendMove(self, itemExpr)

proc receive*[T](self: PooledQueue[T]): Pooled[T] =
  ## Blocking receive from a PooledQueue.
  doAssert not self.q.isNil, "PooledQueue.receive: queue is invalid"
  doAssert not self.q.isClosed, "PooledQueue.receive: queue is closed"

  var item = self.q.receive()
  return move item

proc get*[T](self: PooledQueue[T]): Pooled[T] =
  return self.receive()

proc receiveChecked*[T](self: PooledQueue[T]): MoveResult[Pooled[T], ErrorCode] =
  ## Blocking receive with error reporting.
  if self.q.isNil:
    return errMove(ErrorCode.InvalidState)

  if self.q.isClosed:
    return errMove(ErrorCode.Closed)

  var item = self.q.receive()
  return okMove(item)

proc receiveResult*[T](self: PooledQueue[T]): MoveResult[Pooled[T], ErrorCode] =
  return self.receiveChecked()

proc tryReceive*[T](self: PooledQueue[T]; item: var Pooled[T]): Result[bool, ErrorCode] =
  ## Non-blocking receive from a PooledQueue.
  ##
  ## If `item` is already active, this returns InvalidState rather than
  ## overwriting it and accidentally returning the previous value by assignment.
  if self.q.isNil:
    return err(ErrorCode.InvalidState)

  if item.isActive:
    return err(ErrorCode.InvalidState)

  var tmp: Pooled[T]
  let ret = self.q.tryReceive(tmp)
  if ret.isErr:
    return ret

  if not ret.get():
    return ok(false)

  item = move tmp
  return ok(true)

proc tryGet*[T](self: PooledQueue[T]; item: var Pooled[T]): Result[bool, ErrorCode] =
  return self.tryReceive(item)

proc tryReceiveMove*[T](self: PooledQueue[T]): MoveResult[MoveOption[Pooled[T]], ErrorCode] =
  ## Non-blocking receive returning a take-only option.
  if self.q.isNil:
    return errMove(ErrorCode.InvalidState)

  if self.q.isClosed:
    return errMove(ErrorCode.Closed)

  var item: Pooled[T]
  let ret = self.q.tryReceive(item)
  if ret.isErr:
    return errMove(ret.error)

  if ret.get():
    var opt = someMove(item)
    return okMove(opt)

  var opt = noneMove(Pooled[T])
  return okMove(opt)
