import std/asyncdispatch
import std/typedthreads

import threadtools

# This test verifies an end-to-end async -> thread -> thread -> thread -> async
# round trip.
#
# The first send is initiated from an async task.  The value then passes through
# three worker threads and finally returns to the asyncdispatch/main thread
# through AsyncThreadQueueNotifier + AsyncThreadQueueBridge.
#
# To avoid moving async-proc environment fields, the async task sends rvalue
# frames produced by makeFrame()/makeStopFrame() directly into the first
# ThreadQueue.  The moved payload is then checked after it returns from the
# worker chain.

const
  MsgCount = 256
  BufSize = 2048

type
  Frame = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int
    stages: uint8
    stop: bool

  StageArgs = object
    stageBit: uint8
    input: ThreadQueue[Frame]
    output: ThreadQueue[Frame]

  FinalStageArgs = object
    stageBit: uint8
    input: ThreadQueue[Frame]
    output: AsyncThreadQueueNotifier[Frame]

proc dataPtr(data: seq[byte]): uint =
  if data.len == 0:
    return 0'u
  return cast[uint](unsafeAddr data[0])

proc dataPtr(frame: Frame): uint =
  return dataPtr(frame.data)

proc makeFrame(index: int; size: int): Frame =
  result.data = newSeq[byte](size)
  result.index = index
  result.seed = (index * 41 + 5) mod 251
  result.stages = 0'u8
  result.stop = false

  for i in 0 ..< size:
    result.data[i] = byte((result.seed + i) mod 251)

  result.expectedPtr = dataPtr(result)

proc makeStopFrame(): Frame =
  result.stop = true
  result.index = -1
  result.seed = 0
  result.stages = 0'u8
  result.expectedPtr = 0'u

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

proc takeOk[T](box: AsyncOwned[T]; msg: string): T =
  var ret = box.take()
  doAssert ret.isOk, msg & ": " & $ret.error
  return ret.take()

proc markStage(frame: var Frame; stageBit: uint8) =
  if not frame.stop:
    frame.stages = frame.stages or stageBit

proc stageWorker(args: StageArgs) {.thread.} =
  while true:
    var frame = args.input.receive()
    frame.markStage(args.stageBit)

    let shouldStop = frame.stop
    let ret = args.output.sendMove(frame)
    doAssert ret.isOk, "stage worker sendMove failed: " & $ret.error

    if shouldStop:
      break

proc finalStageWorker(args: FinalStageArgs) {.thread.} =
  while true:
    var frame = args.input.receive()
    frame.markStage(args.stageBit)

    let shouldStop = frame.stop
    let ret = args.output.sendMove(frame)
    doAssert ret.isOk, "final stage worker sendMove failed: " & $ret.error

    if shouldStop:
      break

proc runRoundTripAsync() {.async.} =
  var q1 = assertOk(newThreadQueue[Frame](32), "newThreadQueue q1 failed")
  var q2 = assertOk(newThreadQueue[Frame](32), "newThreadQueue q2 failed")
  var q3 = assertOk(newThreadQueue[Frame](32), "newThreadQueue q3 failed")
  var qOut = assertOk(newThreadQueue[Frame](32), "newThreadQueue qOut failed")

  var bridge = assertOk(newAsyncThreadQueueBridge[Frame](qOut), "newAsyncThreadQueueBridge failed")
  let outTx = bridge.notifier()

  var th1: Thread[StageArgs]
  var th2: Thread[StageArgs]
  var th3: Thread[FinalStageArgs]

  createThread(th1, stageWorker, StageArgs(
    stageBit: 0b0000_0001'u8,
    input: q1,
    output: q2,
  ))

  createThread(th2, stageWorker, StageArgs(
    stageBit: 0b0000_0010'u8,
    input: q2,
    output: q3,
  ))

  createThread(th3, finalStageWorker, FinalStageArgs(
    stageBit: 0b0000_0100'u8,
    input: q3,
    output: outTx,
  ))

  for i in 0 ..< MsgCount:
    let pending = bridge.recvAsync()

    # Do not store the frame in an async-proc local and move it later.
    # async macro locals are lifted into an environment object.  Sending an
    # rvalue frame avoids moving a field out of that environment object.
    let sendRet = q1.sendMove(makeFrame(i, BufSize))
    doAssert sendRet.isOk, "async start sendMove failed: " & $sendRet.error

    let box = await pending
    var frame = takeOk(box, "round trip AsyncOwned[Frame].take failed")

    doAssert not frame.stop, "unexpected stop frame while receiving data"
    doAssert frame.index == i, "round trip returned wrong index"
    doAssert frame.stages == 0b0000_0111'u8, "round trip did not pass all stages"
    doAssert dataPtr(frame) == frame.expectedPtr, "round trip changed seq backing pointer"
    doAssert frame.data.len == BufSize, "round trip returned wrong data length"
    doAssert frame.data[0] == byte(frame.seed), "round trip returned wrong payload"

  block:
    let pending = bridge.recvAsync()
    let sendRet = q1.sendMove(makeStopFrame())
    doAssert sendRet.isOk, "async stop sendMove failed: " & $sendRet.error

    let box = await pending
    var frame = takeOk(box, "round trip stop frame take failed")

    doAssert frame.stop, "expected stop frame"
    doAssert frame.stages == 0'u8, "stop frame should not be marked as data"

  joinThread(th1)
  joinThread(th2)
  joinThread(th3)

  bridge.close()

proc main() =
  waitFor runRoundTripAsync()

main()
echo "OK: async thread round-trip pipeline tests passed"
