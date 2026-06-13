# threadtools API リファレンス

この文書は `threadtools` の公開 API と ownership ルールをまとめたものです。

`threadtools` は、Nim の value object や pool 管理された buffer を、thread 間で不用意にコピーせずに受け渡すための小さな部品群です。

基本形は次です。

```text
value object
  -> sendMove()
  -> ThreadQueue
  -> receive()
  -> owned value
```

asyncdispatch 連携では次の形になります。

```text
ThreadQueue[T]
  -> AsyncThreadQueueBridge[T]
  -> Future[AsyncOwned[T]]
  -> take()
  -> owned value
```

## import

通常はこれで使います。

```nim
import threadtools
```

これで queue、pool item、error、async bridge の主要 API が見える想定です。

## エラーモデル

constructor や non-blocking 操作の多くは次を返します。

```nim
Result[T, ErrorCode]
```

または、

```nim
Result[bool, ErrorCode]
```

テストやサンプルでは次のような helper を使うと読みやすくなります。

```nim
proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()
```

blocking receive 系は payload を直接返すことがあります。

## ownership model

重要なルールはこれです。

> `sendMove`, `okMove`, `someMove`, `newPoolItem`, `take` という名前の API は ownership を消費する。

これらを呼んだあと、元の値を使ってはいけません。

良い例:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
```

悪い例:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
echo frame.data.len  # compile error になるべき
```

コンパイラが「これが最後の use である」と証明できない場合は、明示的に `move` します。

```nim
discard q.sendMove(move frame)
```

これは、ログ出力などで値を読んだあとに転送する verbose/debug code で必要になることがあります。

## ThreadQueue

### 型

```nim
ThreadQueue[T]
```

thread 間で値を移動するための bounded queue です。

次のような value object を渡す用途を想定しています。

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

bounded queue を作成します。

例:

```nim
var q = ?newThreadQueue[Frame](32)
```

実際の error handling は `Result` helper を使うか、手動で `isOk` を見るかで変わります。

### sendMove

```nim
sendMove[T](queue: ThreadQueue[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

`valueExpr` の ownership を消費して queue へ送ります。

例:

```nim
var frame = makeFrame()
let ret = q.sendMove(frame)
doAssert ret.isOk
```

`sendMove` 後に `frame` を使ってはいけません。

rvalue を直接送る場合:

```nim
discard q.sendMove(makeFrame())
```

転送が最後の use だと明示したい場合:

```nim
discard q.sendMove(move frame)
```

### receive

```nim
receive[T](queue: ThreadQueue[T]): T
```

blocking receive です。

例:

```nim
var frame = q.receive()
```

戻り値の所有権は呼び出し側に移ります。

### tryReceive

```nim
tryReceive[T](queue: ThreadQueue[T]; outValue: var T): Result[bool, ErrorCode]
```

blocking せずに受信を試みます。

戻り値の意味は次です。

```text
Ok(true)   outValue に値を受信した
Ok(false)  queue が空だった
Err(...)   queue error
```

例:

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

`PoolItem[T]` のような move-only payload が内部に保持するための raw queue handle です。

この handle は queue を所有しません。元の `ThreadQueue[T]` は、すべての handle と、その handle を保持する値より長く生きている必要があります。

value object の中に `ref object` を持ち込まず、return queue を指すために使います。


## Pool, Pooled, PooledQueue

これは、低レイヤの `PoolItem` pattern を使いやすくするための上位 wrapper API です。

見た目が似ている2つの役割を名前で分けます。

```text
Pool[T]
  未使用の再利用可能な T を入れておく場所

PooledQueue[T]
  使用中の Pooled[T] ownership token を運ぶ通信経路
```

内部的な対応は次です。

```text
Pool[T]        = ThreadQueue[T] の wrapper
Pooled[T]      = PoolItem[T] の alias
PooledQueue[T] = ThreadQueue[PoolItem[T]] の wrapper
```

buffer pool や frame pipeline を作る場合は、まずこの API を使うのが分かりやすいです。

### Pool

```nim
Pool[T]
```

再利用可能な `T` を保持する pool です。

### newPool

```nim
newPool[T](capacity: int): Result[Pool[T], ErrorCode]
```

空の pool を作ります。

例:

```nim
var pool = ?newPool[Buf](8)
```

作成直後の pool は空です。`addMove()` で値を入れます。

```nim
for i in 0 ..< 8:
  discard pool.addMove(newBuf())
```

### addMove

```nim
addMove[T](pool: Pool[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

source value を consume して pool へ追加します。

`addMove(value)` 後に元の `value` を使ってはいけません。

### acquire

```nim
acquire[T](pool: Pool[T]): Pooled[T]
```

blocking acquire です。

戻り値の `Pooled[T]` は、返却先としてこの pool を覚えています。

例:

```nim
var item = pool.acquire()
item.value.fill(...)
discard item.release()
```

### tryAcquire

```nim
tryAcquire[T](pool: Pool[T]; item: var Pooled[T]): Result[bool, ErrorCode]
```

non-blocking acquire です。

item を取得できたら `Ok(true)`、pool が空なら `Ok(false)` を返します。

destination item は inactive である必要があります。active な `Pooled[T]` を誤って上書きしないためです。

### Pooled

```nim
Pooled[T]
```

`PoolItem[T]` の user-facing alias です。

`Pooled[T]` は move-only ownership token です。copy できません。

`release()` されたとき、または active なまま破棄されたとき、所有している `T` を pool に戻します。

### value

```nim
value[T](item: var Pooled[T]): var T
payload[T](item: var Pooled[T]): var T
```

所有している値に access するための user-facing alias です。

例:

```nim
var item = pool.acquire()
item.value.index = 10
```

### PooledQueue

```nim
PooledQueue[T]
```

active な `Pooled[T]` を thread 間で移動するための queue です。

これは pool そのものではなく、通信経路です。

### newPooledQueue

```nim
newPooledQueue[T](capacity: int): Result[PooledQueue[T], ErrorCode]
```

`Pooled[T]` 用の bounded queue を作ります。

例:

```nim
var toWorker = ?newPooledQueue[Buf](8)
var fromWorker = ?newPooledQueue[Buf](8)
```

### PooledQueue.sendMove

```nim
sendMove[T](queue: PooledQueue[T]; itemExpr: untyped): Result[bool, ErrorCode]
```

`Pooled[T]` ownership token を queue へ move します。

成功後、source item を使ってはいけません。

### PooledQueue.receive

```nim
receive[T](queue: PooledQueue[T]): Pooled[T]
```

blocking receive です。

### PooledQueue.tryReceive

```nim
tryReceive[T](queue: PooledQueue[T]; item: var Pooled[T]): Result[bool, ErrorCode]
```

non-blocking receive です。

destination item は inactive である必要があります。

### 典型的な pooled flow

```text
Pool[Buf]
  -> acquire()
  -> Pooled[Buf]
  -> PooledQueue[Buf].sendMove()
  -> worker receive()
  -> item.value を処理
  -> item.release()
  -> Pool[Buf] に戻る
```

例:

```nim
var pool = ?newPool[Buf](8)
var toWorker = ?newPooledQueue[Buf](8)

for i in 0 ..< 8:
  discard pool.addMove(newBuf())

var item = pool.acquire()
fill(item.value)

discard toWorker.sendMove(item)
```

worker 側:

```nim
var item = toWorker.receive()
process(item.value)
discard item.release()
```

## PoolItem

### 型

```nim
PoolItem[T]
```

pool 管理された値を所有する move-only token です。

`PoolItem[T]` は copy できません。

これは意図した制約です。copy できると、同じ buffer に対する owner が2つでき、二重返却につながります。

### newPoolItem

```nim
newPoolItem[T](returnQueue: ThreadQueue[T]; valueExpr: untyped): PoolItem[T]
newPoolItem[T](returnQueue: ThreadQueueHandle[T]; valueExpr: untyped): PoolItem[T]
```

`valueExpr` を所有する `PoolItem` を作ります。

`release()` されたとき、または active なまま destructor が走ったとき、内部の値は `returnQueue` に戻されます。

例:

```nim
var returnQ = ?newThreadQueue[Frame](16)
var frame = makeFrame()

var item = newPoolItem(returnQ, frame)
```

`newPoolItem` 後に `frame` を使ってはいけません。

明示的に書くなら次も有効です。

```nim
var item = newPoolItem(returnQ, move frame)
```

### isActive

```nim
isActive[T](item: var PoolItem[T]): bool
```

現在値を所有しているかを返します。

例:

```nim
doAssert item.isActive
```

### item

```nim
item[T](item: var PoolItem[T]): var T
```

active な `PoolItem` が所有している値へ mutable access します。

例:

```nim
item.item.index = 10
```

返された値への alias を、active な `PoolItem` の lifetime を越えて保持してはいけません。

### release

```nim
release[T](item: var PoolItem[T]): Result[bool, ErrorCode]
```

所有している値を return queue へ返し、`PoolItem` を inactive にします。

例:

```nim
let ret = item.release()
doAssert ret.isOk
```

### take

```nim
take[T](item: var PoolItem[T]): MoveResult[T, ErrorCode]
```

active な item から payload を取り出します。

例:

```nim
var ret = item.take()
doAssert ret.isOk

var frame = ret.take()
```

`take` 後、item は inactive になり、その値は自動返却されません。

### destructor auto-return

`PoolItem[T]` が active なまま破棄されると、所有している値を return queue へ返そうとします。

これは安全網です。

lifetime が重要な箇所では、明示的な `release()` を優先してください。

### lifetime rule

`PoolItem[T]` は queue handle を保持します。

そのため重要な制約があります。

> return queue は、その queue へ値を返す可能性のあるすべての `PoolItem[T]` より長く生きている必要がある。

これは worker queue や async bridge の中を移動中の item も含みます。

## AsyncOwned

### 型

```nim
AsyncOwned[T]
```

async bridge が使う one-shot の ref container です。

async bridge は次を返します。

```nim
Future[AsyncOwned[T]]
```

直接 `Future[T]` にはしません。

理由は、`Future.read` や `waitFor` が `T` の copy を要求する場合があり、`PoolItem[T]` のような move-only payload と相性が悪いためです。

### take

```nim
take[T](box: AsyncOwned[T]): MoveResult[T, ErrorCode]
```

payload を一度だけ取り出します。

例:

```nim
let box = waitFor bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var value = ret.take()
```

`AsyncOwned[T]` は read-only container ではありません。所有権を一度だけ取り出すための container です。

## AsyncThreadQueueBridge

### 型

```nim
AsyncThreadQueueBridge[T]
```

`ThreadQueue[T]` を asyncdispatch と接続する bridge です。

bridge は dispatcher/main thread 側に置きます。`AsyncEvent` の登録を持ち、pending future の complete は dispatcher thread 側で行います。

### newAsyncThreadQueueBridge

```nim
newAsyncThreadQueueBridge[T](queue: ThreadQueue[T]): Result[AsyncThreadQueueBridge[T], ErrorCode]
```

queue 用の event-based bridge を作ります。

例:

```nim
var q = ?newThreadQueue[Frame](32)
var bridge = ?newAsyncThreadQueueBridge[Frame](q)
```

### recvAsync

```nim
recvAsync[T](bridge: AsyncThreadQueueBridge[T]): Future[AsyncOwned[T]]
```

値が届いたときに complete する future を返します。

`waitFor` を使う例:

```nim
let box = waitFor bridge.recvAsync()
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

async code 内で使う例:

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

worker thread 側へ渡す sender handle を作ります。

notifier は bridge を所有しません。

bridge はすべての notifier より長く生きている必要があります。

### close

```nim
close[T](bridge: AsyncThreadQueueBridge[T])
```

bridge を閉じます。

期待される動作は次です。

- pending 中の `recvAsync()` future を fail する
- underlying `AsyncEvent` を unregister / close する
- 以後の `recvAsync()` は即 failed future を返す
- 既存 notifier は invalid になる、または `ErrorCode.Closed` を返す

所有 context を破棄する前に `close()` してください。

### cancelPending

```nim
cancelPending[T](bridge: AsyncThreadQueueBridge[T]; message = ...)
```

bridge を閉じずに、現在 pending 中の receive future だけを fail します。

その後 bridge は再利用できます。

batch 処理を中断したいが pipeline 自体は維持したい場合に使います。

## AsyncThreadQueueNotifier

### 型

```nim
AsyncThreadQueueNotifier[T]
```

worker thread から async bridge へ値を送るための小さな handle です。

内部では queue へ送信し、bridge の `AsyncEvent` を trigger します。

### sendMove

```nim
sendMove[T](notifier: AsyncThreadQueueNotifier[T]; valueExpr: untyped): Result[bool, ErrorCode]
```

値の ownership を消費し、queue に送り、dispatcher を起こします。

worker の例:

```nim
proc worker(tx: AsyncThreadQueueNotifier[Frame]) {.thread.} =
  var frame = makeFrame()
  let ret = tx.sendMove(frame)
  doAssert ret.isOk
```

`sendMove` 後に `frame` を使ってはいけません。

### notify

```nim
notify[T](notifier: AsyncThreadQueueNotifier[T]): Result[bool, ErrorCode]
```

値は送らず async event だけを trigger します。

通常は `sendMove` を使ってください。

### isValid

```nim
isValid[T](notifier: AsyncThreadQueueNotifier[T]): bool
```

notifier がまだ有効かを返します。

bridge が close されると notifier は invalid になります。

## polling recvAsync

簡易的な polling bridge もあります。

```nim
recvAsync[T](queue: ThreadQueue[T]; pollMs = 1): Future[AsyncOwned[T]]
```

最小構成や fallback として使えます。

通常の event-based 用途では `AsyncThreadQueueBridge[T]` を使ってください。

## 使用パターン

### worker thread から async task へ返す

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

### async task から worker chain を開始し、async task に戻る

async code から開始する場合は、async-proc local を後から move しようとせず、rvalue を直接送るのが安全です。

```nim
let pending = bridge.recvAsync()

discard q1.sendMove(makeFrame(i))

let box = await pending
var ret = box.take()
doAssert ret.isOk

var frame = ret.take()
```

## よくある罠

### sendMove 後に値を使わない

悪い例:

```nim
var frame = makeFrame()
discard q.sendMove(frame)
echo frame.index
```

### PoolItem を copy しない

悪い例:

```nim
var item2 = item1
```

これは compile error になるべきです。

### PoolItem を直接 Future[T] に載せない

推奨:

```nim
Future[AsyncOwned[PoolItem[T]]]
```

避ける:

```nim
Future[PoolItem[T]]
```

`waitFor` / `read` が `T` の copy を要求する場合があり、move-only payload では不適切です。

### async proc 内では local の move に注意する

async macro によって local 変数が environment object の field に持ち上げられることがあります。

その場合、local value の move をコンパイラが証明できないことがあります。

安全な書き方:

```nim
discard q.sendMove(makeFrame(i))
```

避けたい書き方:

```nim
var frame = makeFrame(i)
discard q.sendMove(frame)
```

ログなどで値を読んだあと、次の操作が最後の transfer なら明示的に `move` が必要な場合があります。

```nim
log(frame.index)
discard q.sendMove(move frame)
```

### bounded queue は backpressure になる

queue が満杯なら送信側は block します。

これは意図した動作です。

```text
worker が queue が満杯になるまで送る
worker が block する
receiver が queue を drain する
worker が再開する
```

queue capacity は意図して決めてください。

## テスト済みの挙動

現在のテスト群では次を確認しています。

- `ThreadQueue` による value object transfer
- `seq[byte]` payload の pointer stability
- `PoolItem` の release と destructor auto-return
- `PoolItem` の thread ping-pong
- source reuse や copy attempt の compile-fail
- polling async bridge
- `AsyncEvent` bridge
- bridge close / cancel
- worker thread から async task への送信
- 複数 pending `recvAsync()` の FIFO
- prequeued value の FIFO drain
- bounded queue backpressure
- async task -> worker -> worker -> worker -> async task の round trip

## 安定性メモ

この API は、複雑な記法ではなく API 名で ownership transfer を明示する方針です。

重要な慣習は次です。

```text
sendMove  source を consume する
take      container payload を consume する
PoolItem  copy できない
AsyncOwned は one-shot
```
