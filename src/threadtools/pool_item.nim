import ./thread_queue
import ./lib/errcode

export errcode

type
  PoolItem*[T] = object
    active: bool
    ## Raw queue handle, not ThreadQueue[T].
    ##
    ## PoolItem[T] must be sendable through ThreadQueue[PoolItem[T]].  Keeping a
    ## ThreadQueue[T] ref here makes the PoolItem non-isolatable because ref
    ## objects are GC-managed shared references.  The raw handle keeps only the
    ## return path identity; the queue owner must keep the original ThreadQueue
    ## alive for at least as long as any PoolItem that may return to it.
    returnQueue: ThreadQueueHandle[T]
    value: T

# ------------------------------------------------------------------------------
# Copy prevention:
# ------------------------------------------------------------------------------
proc `=copy`*[T](dest: var PoolItem[T], src: PoolItem[T]) {.error: "PoolItem cannot be copied".}

# ------------------------------------------------------------------------------
# Destructor:
# ------------------------------------------------------------------------------
proc `=destroy`*[T](self: var PoolItem[T]) =
  if self.active:
    self.active = false
    discard self.returnQueue.sendMove(move self.value)

# ------------------------------------------------------------------------------
# Constructor:
# ------------------------------------------------------------------------------
proc initPoolItem[T](queue: ThreadQueueHandle[T], value: sink T): PoolItem[T] =
  ## Initializes the private PoolItem representation inside this module.
  ##
  ## newPoolItem() is exported as a template so ensureMove() is evaluated at the
  ## call site.  The field writes themselves must stay in this module.
  result.active = true
  result.returnQueue = queue
  result.value = move value

template newPoolItem*[T](returnQueue: ThreadQueue[T], valueExpr: untyped): PoolItem[T] =
  ## Creates a PoolItem by taking ownership of valueExpr.
  ##
  ## The source value is consumed at construction time.  Reusing it afterwards
  ## should be rejected by Nim's ensureMove checks.
  initPoolItem[T](returnQueue.handle, ensureMove(valueExpr))

template newPoolItem*[T](returnQueue: ThreadQueueHandle[T], valueExpr: untyped): PoolItem[T] =
  ## Creates a PoolItem from an already extracted raw queue handle.
  initPoolItem[T](returnQueue, ensureMove(valueExpr))

# ------------------------------------------------------------------------------
# Access:
# ------------------------------------------------------------------------------
proc isActive*[T](self: var PoolItem[T]): bool {.inline.} =
  return self.active

proc item*[T](self: var PoolItem[T]): var T {.inline.} =
  result = self.value

# ------------------------------------------------------------------------------
# Ownership operations:
# ------------------------------------------------------------------------------
proc release*[T](self: var PoolItem[T]): Result[bool, ErrorCode] =
  ## Returns the contained value to its pool/return queue.
  ##
  ## The item is marked inactive only after the send succeeds.  If the send fails,
  ## the value has not been consumed by sendMove() and the caller still owns this
  ## PoolItem.
  if not self.active:
    return err(ErrorCode.DoubleRelease)

  let ret = self.returnQueue.sendMove(move self.value)
  if ret.isErr:
    return ret

  self.active = false
  return ret

proc take*[T](self: var PoolItem[T]): MoveResult[T, ErrorCode] =
  ## Takes the contained value out of the PoolItem.
  ##
  ## After this succeeds, destructor auto-return is disabled for this PoolItem.
  if not self.active:
    return errMove(ErrorCode.InvalidState)

  self.active = false
  var value = move self.value
  return okMove(value)
