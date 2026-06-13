# threadtools API reference

This document describes the public API style and ownership rules of `threadtools`.

`threadtools` is designed for passing value objects and pool-owned buffers between Nim threads without accidentally copying their backing storage.

The core idea is:

```text
value object
  -> sendMove()
  -> ThreadQueue
  -> receive()
  -> owned value
```

For asyncdispatch integration:

```text
ThreadQueue[T]
  -> AsyncThreadQueueBridge[T]
  -> Future[AsyncOwned[T]]
  -> take()
  -> owned value
```

## Imports

Typical usage:

```nim
import threadtools
```

This should expose the main queue, pool item, error, and async bridge APIs.

## Error model

Most constructor and non-blocking operations return:

```nim
Result[T, ErrorCode]
```

or:

```nim
Result[bool, ErrorCode]
```

Typical helper:

```nim
proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()
```

Blocking receive operations may return the payload directly.

## Ownership model

The important rule is:

> APIs named `sendMove`, `okMove`, `someMove`, `newPoolItem`, or `take` consume ownership.

After calling a consuming API, the source value must not be used again.

Good:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
```

Bad:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
echo frame.data.len  # compile-time error expected
```

If the compiler cannot prove that a value is the final use, make the ownership transfer explicit:

```nim
discard q.sendMove(move frame)
```

This is useful in verbose/debug code where the value is inspected for logging before forwarding.

## ThreadQueue

### Type

```nim
ThreadQueue[T]
```

A bounded thread-safe queue for moving values between threads.

`ThreadQueue[T]` is intended for value objects such as:

```nim
type
  Frame = object
    data: seq[byte]
    index: int
```

### newThreadQueue

```nim
newThreadQueue[T](capacity: int): Result[ThreadQueue[T], ErrorCode]
```

Creates a bounded queue.

Example:

```nim
var q = ?newThreadQueue[Frame](32)
```

The exact error handling style depends on whether the caller is using `Result` helpers or manual checks.

### sendMove

```nim
sendMove[T](queue: ThreadQueue[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

Consumes `valueExpr` and sends it to the queue.

Example:

```nim
var frame = makeFrame()
let ret = q.sendMove(frame)
doAssert ret.isOk
```

The source value must not be used after `sendMove`.

For rvalue construction:

```nim
discard q.sendMove(makeFrame())
```

For explicit final-use forwarding:

```nim
discard q.sendMove(move frame)
```

### receive

```nim
receive[T](queue: ThreadQueue[T]): T
```

Blocking receive.

Example:

```nim
var frame = q.receive()
```

The caller owns the returned value.

### tryReceive

```nim
tryReceive[T](queue: ThreadQueue[T]; outValue: var T): Result[bool, ErrorCode]
```

Attempts to receive without blocking.

Returns:

```text
Ok(true)   value was received into outValue
Ok(false)  queue was empty
Err(...)   queue error
```

Example:

```nim
var frame: Frame
let ret = q.tryReceive(frame)
doAssert ret.isOk

if ret.get():
  process(frame)
```

### ThreadQueueHandle

```nim
ThreadQueueHandle[T]
handle(queue: ThreadQueue[T]): ThreadQueueHandle[T]
```

A raw queue handle used by move-only payloads such as `PoolItem[T]`.

This handle does not own the queue. The original `ThreadQueue[T]` must outlive all handles and all values that store those handles.

Use this when a value needs to carry a return queue without embedding a `ref object` in the moved payload.


## Pool, Pooled, and PooledQueue

These are user-facing wrappers for the lower-level pool item pattern.

They separate the two roles that otherwise look too similar:

```text
Pool[T]
  free reusable T values

PooledQueue[T]
  communication path for active Pooled[T] ownership tokens
```

Internally, the mapping is:

```text
Pool[T]        = wrapper around ThreadQueue[T]
Pooled[T]      = alias for PoolItem[T]
PooledQueue[T] = wrapper around ThreadQueue[PoolItem[T]]
```

Use this API first when building buffer pools or frame pipelines.

### Pool

```nim
Pool[T]
```

A named pool of reusable `T` values.

### newPool

```nim
newPool[T](capacity: int): Result[Pool[T], ErrorCode]
```

Creates an empty pool.

Example:

```nim
var pool = ?newPool[Buf](8)
```

The pool starts empty.  Fill it with `addMove()`:

```nim
for i in 0 ..< 8:
  discard pool.addMove(newBuf())
```

### addMove

```nim
addMove[T](pool: Pool[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

Adds a value to the pool by consuming the source value.

After `addMove(value)`, the source value must not be used again.

### acquire

```nim
acquire[T](pool: Pool[T]): Pooled[T]
```

Blocking acquire.

The returned `Pooled[T]` remembers the pool as its return path.

Example:

```nim
var item = pool.acquire()
item.value.fill(...)
discard item.release()
```

### tryAcquire

```nim
tryAcquire[T](pool: Pool[T]; item: var Pooled[T]): Result[bool, ErrorCode]
```

Non-blocking acquire.

Returns `Ok(true)` when an item was acquired, `Ok(false)` when the pool is empty.

The destination item must be inactive.  This prevents accidental overwrite of an active `Pooled[T]`.

### Pooled

```nim
Pooled[T]
```

User-facing alias for `PoolItem[T]`.

A `Pooled[T]` is a move-only ownership token.  It cannot be copied.

When released, or when destroyed while still active, it returns the owned `T` to its pool.

### value

```nim
value[T](item: var Pooled[T]): var T
payload[T](item: var Pooled[T]): var T
```

User-facing aliases for accessing the owned value.

Example:

```nim
var item = pool.acquire()
item.value.index = 10
```

### PooledQueue

```nim
PooledQueue[T]
```

A queue for moving active `Pooled[T]` values between threads.

This is not the pool.  It is a communication path.

### newPooledQueue

```nim
newPooledQueue[T](capacity: int): Result[PooledQueue[T], ErrorCode]
```

Creates a bounded queue for `Pooled[T]` values.

Example:

```nim
var toWorker = ?newPooledQueue[Buf](8)
var fromWorker = ?newPooledQueue[Buf](8)
```

### PooledQueue.sendMove

```nim
sendMove[T](queue: PooledQueue[T]; itemExpr: untyped): Result[bool, ErrorCode]
```

Moves a `Pooled[T]` ownership token into the queue.

After this call succeeds, the source item must not be used again.

### PooledQueue.receive

```nim
receive[T](queue: PooledQueue[T]): Pooled[T]
```

Blocking receive.

### PooledQueue.tryReceive

```nim
tryReceive[T](queue: PooledQueue[T]; item: var Pooled[T]): Result[bool, ErrorCode]
```

Non-blocking receive.

The destination item must be inactive.

### Typical pooled flow

```text
Pool[Buf]
  -> acquire()
  -> Pooled[Buf]
  -> PooledQueue[Buf].sendMove()
  -> worker receive()
  -> process item.value
  -> item.release()
  -> returns to Pool[Buf]
```

Example:

```nim
var pool = ?newPool[Buf](8)
var toWorker = ?newPooledQueue[Buf](8)

for i in 0 ..< 8:
  discard pool.addMove(newBuf())

var item = pool.acquire()
fill(item.value)

discard toWorker.sendMove(item)
```

Worker side:

```nim
var item = toWorker.receive()
process(item.value)
discard item.release()
```

## PoolItem

### Type

```nim
PoolItem[T]
```

A move-only ownership token for a pooled value.

`PoolItem[T]` cannot be copied.

This is intentional. Copying a pool item would create two owners for the same buffer and could cause double return.

### newPoolItem

```nim
newPoolItem[T](returnQueue: ThreadQueue[T]; valueExpr: untyped): PoolItem[T]
newPoolItem[T](returnQueue: ThreadQueueHandle[T]; valueExpr: untyped): PoolItem[T]
```

Creates a pool item that owns `valueExpr`.

When released or destroyed while active, the contained value is returned to `returnQueue`.

Example:

```nim
var returnQ = ?newThreadQueue[Frame](16)
var frame = makeFrame()

var item = newPoolItem(returnQ, frame)
```

After `newPoolItem`, `frame` must not be used again.

Explicit move is also valid:

```nim
var item = newPoolItem(returnQ, move frame)
```

### isActive

```nim
isActive[T](item: var PoolItem[T]): bool
```

Returns whether the item currently owns a value.

Example:

```nim
doAssert item.isActive
```

### item

```nim
item[T](item: var PoolItem[T]): var T
```

Provides mutable access to the owned value while the `PoolItem` is active.

Example:

```nim
item.item.index = 10
```

Do not store aliases to the returned value beyond the lifetime of the active `PoolItem`.

### release

```nim
release[T](item: var PoolItem[T]): Result[bool, ErrorCode]
```

Returns the owned value to the configured return queue and makes the item inactive.

Example:

```nim
let ret = item.release()
doAssert ret.isOk
```

### take

```nim
take[T](item: var PoolItem[T]): MoveResult[T, ErrorCode]
```

Consumes the active item and returns the owned payload.

Example:

```nim
var ret = item.take()
doAssert ret.isOk

var frame = ret.take()
```

After `take`, the item is inactive and will not auto-return the value.

### Destructor auto-return

If a `PoolItem[T]` is still active when it is destroyed, it attempts to return the owned value to its return queue.

This is intended as a safety net.

Prefer explicit `release()` when the lifetime is important.

### Lifetime rule

A `PoolItem[T]` stores a queue handle. Therefore:

> The return queue must outlive every `PoolItem[T]` that may return values to it.

This includes items in transit through worker queues and async bridges.

## AsyncOwned

### Type

```nim
AsyncOwned[T]
```

A one-shot ref container used by the async bridge.

The async bridge returns:

```nim
Future[AsyncOwned[T]]
```

instead of:

```nim
Future[T]
```

This avoids copy requirements from `Future.read` and `waitFor`, especially for move-only payloads such as `PoolItem[T]`.

### take

```nim
take[T](box: AsyncOwned[T]): MoveResult[T, ErrorCode]
```

Consumes the payload exactly once.

Example:

```nim
let box = waitFor bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var value = ret.take()
```

Do not treat `AsyncOwned[T]` as a read-only container. It is for one-time ownership extraction.

## AsyncThreadQueueBridge

### Type

```nim
AsyncThreadQueueBridge[T]
```

Connects a `ThreadQueue[T]` to `asyncdispatch`.

The bridge lives on the dispatcher/main thread. It owns the `AsyncEvent` registration and completes pending futures on the dispatcher thread.

### newAsyncThreadQueueBridge

```nim
newAsyncThreadQueueBridge[T](queue: ThreadQueue[T]): Result[AsyncThreadQueueBridge[T], ErrorCode]
```

Creates an event-based bridge for a queue.

Example:

```nim
var q = ?newThreadQueue[Frame](32)
var bridge = ?newAsyncThreadQueueBridge[Frame](q)
```

### recvAsync

```nim
recvAsync[T](bridge: AsyncThreadQueueBridge[T]): Future[AsyncOwned[T]]
```

Returns a future that completes when a value is available.

Example with `waitFor`:

```nim
let box = waitFor bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

Example inside async code:

```nim
let box = await bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

### notifier

```nim
notifier[T](bridge: AsyncThreadQueueBridge[T]): AsyncThreadQueueNotifier[T]
```

Creates a sender handle that can be passed to worker threads.

The notifier does not own the bridge.

The bridge must outlive all notifiers.

### close

```nim
close[T](bridge: AsyncThreadQueueBridge[T])
```

Closes the bridge.

This should:

- fail pending `recvAsync()` futures
- unregister/close the underlying `AsyncEvent`
- make future `recvAsync()` calls fail immediately
- make existing notifiers invalid or return `ErrorCode.Closed`

Call `close()` before destroying the owning context.

### cancelPending

```nim
cancelPending[T](bridge: AsyncThreadQueueBridge[T]; message = ...)
```

Fails currently pending receive futures without closing the bridge.

The bridge can be reused afterward.

This is useful when aborting a batch of pending receives but keeping the pipeline alive.

## AsyncThreadQueueNotifier

### Type

```nim
AsyncThreadQueueNotifier[T]
```

A small handle used by worker threads to send values to an async bridge.

It sends to the underlying queue and triggers the bridge's `AsyncEvent`.

### sendMove

```nim
sendMove[T](notifier: AsyncThreadQueueNotifier[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

Consumes the value, sends it to the queue, and wakes the dispatcher.

Example worker:

```nim
proc worker(tx: AsyncThreadQueueNotifier[Frame]) {.thread.} =
  var frame = makeFrame()
  let ret = tx.sendMove(frame)
  doAssert ret.isOk
```

After `sendMove`, `frame` must not be used again.

### notify

```nim
notify[T](notifier: AsyncThreadQueueNotifier[T]): Result[bool, ErrorCode]
```

Triggers the async event without sending a value.

Most users should prefer `sendMove`.

### isValid

```nim
isValid[T](notifier: AsyncThreadQueueNotifier[T]): bool
```

Returns whether the notifier is still valid.

A notifier becomes invalid when its bridge is closed.

## Polling recvAsync

There may also be a simple polling bridge form:

```nim
recvAsync[T](queue: ThreadQueue[T]; pollMs = 1): Future[AsyncOwned[T]]
```

This is useful as a minimal bridge or fallback.

For normal event-based use, prefer `AsyncThreadQueueBridge[T]`.

## Patterns

### Worker thread to async task

```nim
type
  WorkerArgs = object
    tx: AsyncThreadQueueNotifier[Frame]

proc worker(args: WorkerArgs) {.thread.} =
  for i in 0 ..< 100:
    let ret = args.tx.sendMove(makeFrame(i))
    doAssert ret.isOk

var q = ?newThreadQueue[Frame](32)
var bridge = ?newAsyncThreadQueueBridge[Frame](q)
let tx = bridge.notifier()

var th: Thread[WorkerArgs]
createThread(th, worker, WorkerArgs(tx: tx))

for i in 0 ..< 100:
  let box = waitFor bridge.recvAsync()
  var ret = box.take()
  doAssert ret.isOk

  var frame = ret.take()
  process(frame)

joinThread(th)
bridge.close()
```

### Async task to worker chain and back

Start from async code by sending an rvalue to avoid moving an async-proc local:

```nim
let pending = bridge.recvAsync()

discard q1.sendMove(makeFrame(i))

let box = await pending
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

## Common pitfalls

### Do not use a value after sendMove

Bad:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
echo frame.index
```

### Do not copy PoolItem

Bad:

```nim
var item2 = item1
```

This should fail at compile time.

### Do not put PoolItem directly in Future[T]

Prefer:

```nim
Future[AsyncOwned[PoolItem[T]]]
```

Do not rely on:

```nim
Future[PoolItem[T]]
```

`waitFor` / `read` may require copying `T`, which is not valid for move-only payloads.

### Be careful inside async proc

Async macro locals may be lifted into an environment object.

This can make moving local variables harder for the compiler to prove.

Prefer rvalue sends:

```nim
discard q.sendMove(makeFrame(i))
```

instead of:

```nim
var frame = makeFrame(i)
discard q.sendMove(frame)
```

If a value has been inspected for logging and the next operation is the final transfer, explicit move may be needed:

```nim
log(frame.index)
discard q.sendMove(move frame)
```

### Bounded queues apply backpressure

If a queue is full, sending can block.

This is intentional:

```text
worker sends until queue full
worker blocks
receiver drains queue
worker resumes
```

Use queue capacity deliberately.

## Tested behavior

The current test suite covers:

- value object transfer through `ThreadQueue`
- pointer stability for `seq[byte]` payloads
- `PoolItem` release and destructor auto-return
- `PoolItem` thread ping-pong
- compile-fail checks for source reuse and copy attempts
- polling async bridge
- `AsyncEvent` bridge
- bridge close/cancel behavior
- worker thread to async task flow
- multiple pending `recvAsync()` FIFO behavior
- prequeued FIFO drain behavior
- bounded queue backpressure
- async task -> worker -> worker -> worker -> async task round trip

## Stability notes

This API intentionally exposes ownership transfer in names rather than syntax-heavy annotations.

The important conventions are:

```text
sendMove  consumes the source
take      consumes the container payload
PoolItem  cannot be copied
AsyncOwned is one-shot
```
