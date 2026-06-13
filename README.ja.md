# threadtools

`threadtools` は、Nim の value object を thread 間で ownership transfer するための小さなライブラリです。

frame、packet、buffer、pool 管理された item などを、backing storage の不用意なコピーなしで worker thread 間に流すことを目的にしています。

基本設計は単純です。

```text
value object
  -> sendMove()
  -> ThreadQueue
  -> receive()
  -> owned value
```

asyncdispatch 連携では次の形になります。

```text
worker thread
  -> AsyncThreadQueueNotifier[T].sendMove(value)
  -> ThreadQueue[T]
  -> AsyncThreadQueueBridge[T]
  -> Future[AsyncOwned[T]]
  -> take()
  -> async task が value を所有
```

## ドキュメント

- [API リファレンス](docs/api.ja.md)
- [English README](README.md)
- [English API reference](docs/api.md)

補足資料・図解:

- [Async bridge backpressure](docs/async_bridge_backpressure.md)
- [Async thread round-trip pipeline](docs/async_thread_roundtrip_pipeline.md)
- [Verbose round-trip demo output](docs/async_thread_roundtrip_verbose_output.md)

## 動機

Nim の value object は `seq[byte]` や `string` のような owned storage を持てます。

このような object を copy-style container に入れたり、普通の値渡しに見える API に通したりすると、backing storage がコピーされるのかどうかが分かりにくくなることがあります。

`threadtools` では逆に、次の方針を取ります。

- ownership transfer を API 名で明示する
- move-only payload が copy されそうなら compile error にする
- thread queue を ownership transfer 境界として使う
- pool item は共有参照ではなく ownership token として扱う
- asyncdispatch では直接 `Future[T]` ではなく `AsyncOwned[T]` を使う

Nim を Rust にすることが目的ではありません。  
危ない境界だけ Rust 的に固めつつ、通常の処理は Nim らしく読みやすく書くことが目的です。

## 主要概念

### ThreadQueue

`ThreadQueue[T]` は bounded な thread-safe queue です。

値は `sendMove()` で送ります。

```nim
var q = assertOk(newThreadQueue[Frame](32), "newThreadQueue failed")

var frame = makeFrame()
discard q.sendMove(frame)

var received = q.receive()
```

`sendMove(frame)` 後に `frame` を使ってはいけません。

### PoolItem

`PoolItem[T]` は、pool 管理された値を所有する move-only token です。

copy はできません。これは、同じ buffer の二重返却を防ぐための制約です。

```nim
var returnQ = assertOk(newThreadQueue[Frame](16), "return queue failed")

var frame = makeFrame()
var item = newPoolItem(returnQ, frame)

# ここで item が frame を所有している
discard item.release()
```

active な `PoolItem` が破棄されると、設定された return queue へ値を戻そうとします。

### AsyncOwned

async bridge は次を返します。

```nim
Future[AsyncOwned[T]]
```

直接 `Future[T]` にはしません。

これは `PoolItem[T]` のような move-only value で重要です。`Future.read` / `waitFor` が `T` の copy を要求する場合があるためです。

payload は `take()` で取り出します。

```nim
let box = waitFor bridge.recvAsync()

var ret = box.take()
doAssert ret.isOk

var value = ret.take()
```

### AsyncThreadQueueBridge

`AsyncThreadQueueBridge[T]` は `ThreadQueue[T]` と `asyncdispatch` を接続します。

worker thread は `AsyncThreadQueueNotifier[T]` 経由で送信します。

```nim
proc worker(tx: AsyncThreadQueueNotifier[Frame]) {.thread.} =
  for i in 0 ..< 100:
    let ret = tx.sendMove(makeFrame(i))
    doAssert ret.isOk
```

async/main thread は bridge から受信します。

```nim
let box = await bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

## 最小例

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

## よくある罠

### sendMove 後に元の値を使わない

```nim
var frame = makeFrame()
discard q.sendMove(frame)

# ここから先で frame を使わない
```

再利用しようとすると compile error になるべきです。

### PoolItem を copy しない

```nim
var item2 = item1  # compile error になるべき
```

`PoolItem` が copy できると、同じ buffer に owner が2つできてしまいます。

### return queue を長生きさせる

`PoolItem[T]` は queue handle を保持します。

そのため return queue は、その queue へ値を戻す可能性があるすべての `PoolItem[T]` より長く生きている必要があります。

### move-only payload を直接 Future[T] に入れない

推奨:

```nim
Future[AsyncOwned[PoolItem[T]]]
```

避ける:

```nim
Future[PoolItem[T]]
```

### async proc 内の local move に注意する

`async` macro によって local 変数が environment object の field に持ち上げられることがあります。  
その local をあとから move しようとすると、コンパイラが安全性を証明できず失敗することがあります。

安全な書き方:

```nim
discard q.sendMove(makeFrame(i))
```

避けたい書き方:

```nim
var frame = makeFrame(i)
discard q.sendMove(frame)
```

ログなどで値を読んだあと、次が最後の transfer なら明示的に `move` が必要な場合があります。

```nim
log(frame.index)
discard q.sendMove(move frame)
```

### bounded queue は backpressure になる

queue が満杯になると、送信側 thread は block します。

これは意図した動作です。

```text
worker が queue が満杯になるまで送る
worker が block する
receiver が queue を drain する
worker が再開する
```

queue capacity は意図して決めてください。

## テスト

現在のテスト群では次を確認しています。

- `ThreadQueue` による value object transfer
- `seq[byte]` payload の pointer stability
- `PoolItem` の release と destructor auto-return
- `PoolItem` の thread ping-pong
- copy / source reuse の compile-fail
- polling async bridge
- `AsyncEvent` bridge
- close / cancel handling
- worker thread から async task への transfer
- pending receive / prequeued value の FIFO
- bounded queue backpressure
- async -> thread -> thread -> thread -> async round trip

実行:

```sh
sh run_threadtools_tests.sh
```

release 確認:

```sh
NIM_FLAGS="--threads:on --mm:orc -d:release --path:src --outdir:build/tests" sh run_threadtools_tests.sh
NIM_FLAGS="--threads:on --mm:arc -d:release --path:src --outdir:build/tests" sh run_threadtools_tests.sh
```

## 手動 demo

async round-trip pipeline の動きを目で確認する verbose demo があります。

```sh
nim c -r -d:release --mm:orc tests/demo_async_thread_roundtrip_verbose.nim
```

各 worker の receive/send と pointer 値を表示します。

## 現在の状態

`threadtools` は現時点では low-level ownership-transfer primitives を提供します。

次の上位レイヤとしては、以下を使った worker / pipeline abstraction が想定されます。

- `ThreadQueue`
- `PoolItem`
- `AsyncThreadQueueBridge`
- `AsyncOwned`

## License

repository の license を参照してください。
