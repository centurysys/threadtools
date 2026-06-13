# threadtools

`threadtools` is a small Nim library for moving owned value objects between threads.

It is designed for worker pipelines that pass frames, packets, buffers, or pool-owned items without accidentally copying their backing storage.

The design is intentionally simple:

```text
value object
  -> sendMove()
  -> ThreadQueue
  -> receive()
  -> owned value
```

For asyncdispatch integration:

```text
worker thread
  -> AsyncThreadQueueNotifier[T].sendMove(value)
  -> ThreadQueue[T]
  -> AsyncThreadQueueBridge[T]
  -> Future[AsyncOwned[T]]
  -> take()
  -> async task owns value
```

## Documentation

- [API reference](docs/api.md)
- [日本語 README](README.ja.md)
- [日本語 API リファレンス](docs/api.ja.md)

Additional design notes and diagrams:

- [Async bridge backpressure](docs/async_bridge_backpressure.md)
- [Async thread round-trip pipeline](docs/async_thread_roundtrip_pipeline.md)
- [Verbose round-trip demo output](docs/async_thread_roundtrip_verbose_output.md)

## Motivation

Nim value objects can contain owned storage such as `seq[byte]` or `string`.

When these objects are wrapped in copy-style containers or passed through APIs that look like ordinary value passing, it can be unclear whether the backing storage is copied.

`threadtools` takes the opposite approach:

- ownership transfer is explicit in API names
- move-only payloads are rejected at compile time if copied
- thread queues are used as ownership transfer boundaries
- pool items are ownership tokens, not shared references
- asyncdispatch uses `AsyncOwned[T]` instead of direct `Future[T]`

The goal is not to turn Nim into Rust. The goal is to get Rust-like protection at the few boundaries where it matters most, while keeping normal Nim code readable.

## Main concepts

### ThreadQueue

`ThreadQueue[T]` is a bounded thread-safe queue.

Values are sent with `sendMove()`:

```nim
var q = assertOk(newThreadQueue[Frame](32), "newThreadQueue failed")

var frame = makeFrame()
discard q.sendMove(frame)

var received = q.receive()
```

After `sendMove(frame)`, `frame` must not be used again.

### PoolItem

`PoolItem[T]` is a move-only ownership token for pooled values.

It cannot be copied. This prevents double-return of the same buffer.

```nim
var returnQ = assertOk(newThreadQueue[Frame](16), "return queue failed")

var frame = makeFrame()
var item = newPoolItem(returnQ, frame)

# item owns frame here
discard item.release()
```

If an active `PoolItem` is destroyed, it attempts to return its value to the configured return queue.

### AsyncOwned

The async bridge returns:

```nim
Future[AsyncOwned[T]]
```

instead of:

```nim
Future[T]
```

This is important for move-only values such as `PoolItem[T]`, because `Future.read` / `waitFor` may require copying `T`.

Use `take()` to extract the payload:

```nim
let box = waitFor bridge.recvAsync()

var ret = box.take()
doAssert ret.isOk

var value = ret.take()
```

### AsyncThreadQueueBridge

`AsyncThreadQueueBridge[T]` connects `ThreadQueue[T]` to `asyncdispatch`.

A worker thread sends through `AsyncThreadQueueNotifier[T]`:

```nim
proc worker(tx: AsyncThreadQueueNotifier[Frame]) {.thread.} =
  for i in 0 ..< 100:
    let ret = tx.sendMove(makeFrame(i))
    doAssert ret.isOk
```

The async/main thread receives through the bridge:

```nim
let box = await bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

## Minimal example

```nim
import std/asyncdispatch
import std/typedthreads
import threadtools

type
  Frame = object
    data: seq[byte]
    index: int

  WorkerArgs = object
    tx: AsyncThreadQueueNotifier[Frame]

proc makeFrame(index: int): Frame =
  result.data = newSeq[byte](1024)
  result.index = index

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc takeOk[T](box: AsyncOwned[T]; msg: string): T =
  var ret = box.take()
  doAssert ret.isOk, msg & ": " & $ret.error
  return ret.take()

proc worker(args: WorkerArgs) {.thread.} =
  for i in 0 ..< 10:
    let ret = args.tx.sendMove(makeFrame(i))
    doAssert ret.isOk

proc main() =
  var q = assertOk(newThreadQueue[Frame](8), "queue failed")
  var bridge = assertOk(newAsyncThreadQueueBridge[Frame](q), "bridge failed")
  let tx = bridge.notifier()

  var th: Thread[WorkerArgs]
  createThread(th, worker, WorkerArgs(tx: tx))

  for i in 0 ..< 10:
    let box = waitFor bridge.recvAsync()
    var frame = takeOk(box, "recv failed")
    doAssert frame.index == i

  joinThread(th)
  bridge.close()

main()
```

## Common pitfalls

### Do not use a value after sendMove

```nim
var frame = makeFrame()
discard q.sendMove(frame)

# Do not use frame after this point.
```

This should fail at compile time if `frame` is reused.

### Do not copy PoolItem

```nim
var item2 = item1  # compile-time error expected
```

Copying a `PoolItem` would create two owners for the same buffer.

### Keep return queues alive

`PoolItem[T]` stores a queue handle.

The return queue must outlive every `PoolItem[T]` that may return values to it.

### Do not put move-only payloads directly in Future[T]

Prefer:

```nim
Future[AsyncOwned[PoolItem[T]]]
```

Avoid:

```nim
Future[PoolItem[T]]
```

### Be careful inside async proc

`async` macro locals may be lifted into an environment object. Moving such locals later may fail.

Prefer sending rvalues:

```nim
discard q.sendMove(makeFrame(i))
```

instead of:

```nim
var frame = makeFrame(i)
discard q.sendMove(frame)
```

When forwarding after logging or inspection, explicit final move may be needed:

```nim
log(frame.index)
discard q.sendMove(move frame)
```

### Bounded queues apply backpressure

If a queue is full, sender threads block until receivers drain values.

This is intentional and useful for controlling memory growth.

## Tests

The current test suite covers:

- value-object transfer through `ThreadQueue`
- pointer stability for `seq[byte]` payloads
- `PoolItem` release and destructor auto-return
- `PoolItem` thread ping-pong
- compile-fail checks for copy and source reuse
- polling async bridge
- `AsyncEvent` bridge
- close/cancel handling
- worker-thread to async-task transfer
- FIFO behavior for pending receives and prequeued values
- bounded queue backpressure
- async -> thread -> thread -> thread -> async round trip

Run:

```sh
sh run_threadtools_tests.sh
```

For release checks:

```sh
NIM_FLAGS="--threads:on --mm:orc -d:release --path:src --outdir:build/tests" sh run_threadtools_tests.sh
NIM_FLAGS="--threads:on --mm:arc -d:release --path:src --outdir:build/tests" sh run_threadtools_tests.sh
```

## Manual demo

A noisy demo is available for visually confirming the async round-trip pipeline:

```sh
nim c -r -d:release --mm:orc tests/demo_async_thread_roundtrip_verbose.nim
```

It prints each worker receive/send event and pointer value.

## Status

`threadtools` currently provides low-level ownership-transfer primitives.

The next layer is expected to be a worker/pipeline abstraction built on top of:

- `ThreadQueue`
- `PoolItem`
- `AsyncThreadQueueBridge`
- `AsyncOwned`

## License

See the repository license.
