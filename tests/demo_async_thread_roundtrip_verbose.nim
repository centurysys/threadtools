import std/asyncdispatch
import std/strformat
import std/strutils
import std/typedthreads

import threadtools

# Verbose demo/test for:
#
#   async task -> worker1 -> worker2 -> worker3 -> async task
#
# This file is intentionally noisy.  Keep it out of the normal test runner if
# you want CI output to stay quiet.
#
# It logs each worker receive/send event through a separate ThreadQueue[LogLine]
# so output is centralized on the main thread rather than interleaved directly
# from worker threads.

const
  MsgCount = 5
  BufSize = 128

type
  Frame = object
    data: seq[byte]
    expectedPtr: uint
    index: int
    seed: int
    stages: uint8
    stop: bool

  LogAction = enum
    laRecv
    laSend
    laStopRecv
    laStopSend

  LogLine = object
    stage: int
    action: LogAction
    index: int
    stages: uint8
    ptrValue: uint

  StageArgs = object
    stageNo: int
    stageBit: uint8
    input: ThreadQueue[Frame]
    output: ThreadQueue[Frame]
    logQ: ThreadQueueHandle[LogLine]

  FinalStageArgs = object
    stageNo: int
    stageBit: uint8
    input: ThreadQueue[Frame]
    output: AsyncThreadQueueNotifier[Frame]
    logQ: ThreadQueueHandle[LogLine]

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

proc actionText(action: LogAction): string =
  case action
  of laRecv: "received"
  of laSend: "sending"
  of laStopRecv: "received STOP"
  of laStopSend: "forwarding STOP"

proc stageMaskText(stages: uint8): string =
  &"0b{int(stages):03b}"

proc ptrHex(p: uint): string =
  when sizeof(uint) == 8:
    "0x" & toHex(uint64(p), 16)
  else:
    "0x" & toHex(uint32(p), 8)

proc sendLogValues(
    logQ: ThreadQueueHandle[LogLine];
    stage: int;
    action: LogAction;
    index: int;
    stages: uint8;
    ptrValue: uint
) =
  var line = LogLine(
    stage: stage,
    action: action,
    index: index,
    stages: stages,
    ptrValue: ptrValue,
  )

  let ret = logQ.sendMove(line)
  doAssert ret.isOk, "logQ.sendMove failed: " & $ret.error

proc sendStopLog(logQ: ThreadQueueHandle[LogLine]; stage: int; action: LogAction) =
  sendLogValues(logQ, stage, action, -1, 0'u8, 0'u)

proc drainLogs(logQ: var ThreadQueue[LogLine]) =
  while true:
    var line: LogLine
    let ret = logQ.tryReceive(line)
    doAssert ret.isOk, "logQ.tryReceive failed: " & $ret.error

    if not ret.get():
      break

    if line.index >= 0:
      echo &"  worker{line.stage}: {actionText(line.action)} frame #{line.index}, stages={stageMaskText(line.stages)}, ptr={ptrHex(line.ptrValue)}"
    else:
      echo &"  worker{line.stage}: {actionText(line.action)}"

proc markStage(frame: var Frame; stageBit: uint8) =
  if not frame.stop:
    frame.stages = frame.stages or stageBit

proc stageWorker(args: StageArgs) {.thread.} =
  while true:
    var frame = args.input.receive()

    if frame.stop:
      sendStopLog(args.logQ, args.stageNo, laStopRecv)

      # Explicit move helps the compiler see that this is the final use of the
      # owned frame after earlier logging reads in this loop iteration.
      let ret = args.output.sendMove(move frame)
      doAssert ret.isOk, "stage worker stop sendMove failed: " & $ret.error

      sendStopLog(args.logQ, args.stageNo, laStopSend)
      break

    let recvIndex = frame.index
    let recvStages = frame.stages
    let recvPtr = dataPtr(frame)
    sendLogValues(args.logQ, args.stageNo, laRecv, recvIndex, recvStages, recvPtr)

    frame.markStage(args.stageBit)

    let sendIndex = frame.index
    let sendStages = frame.stages
    let sendPtr = dataPtr(frame)
    sendLogValues(args.logQ, args.stageNo, laSend, sendIndex, sendStages, sendPtr)

    # Explicit move keeps this verbose demo separate from the normal API
    # consume tests.  The real API still supports sendMove(value) and has
    # compile-fail coverage for reuse after send.
    let ret = args.output.sendMove(move frame)
    doAssert ret.isOk, "stage worker sendMove failed: " & $ret.error

proc finalStageWorker(args: FinalStageArgs) {.thread.} =
  while true:
    var frame = args.input.receive()

    if frame.stop:
      sendStopLog(args.logQ, args.stageNo, laStopRecv)

      let ret = args.output.sendMove(move frame)
      doAssert ret.isOk, "final stage stop sendMove failed: " & $ret.error

      sendStopLog(args.logQ, args.stageNo, laStopSend)
      break

    let recvIndex = frame.index
    let recvStages = frame.stages
    let recvPtr = dataPtr(frame)
    sendLogValues(args.logQ, args.stageNo, laRecv, recvIndex, recvStages, recvPtr)

    frame.markStage(args.stageBit)

    let sendIndex = frame.index
    let sendStages = frame.stages
    let sendPtr = dataPtr(frame)
    sendLogValues(args.logQ, args.stageNo, laSend, sendIndex, sendStages, sendPtr)

    let ret = args.output.sendMove(move frame)
    doAssert ret.isOk, "final stage worker sendMove failed: " & $ret.error

proc runRoundTripVerboseAsync() {.async.} =
  var q1 = assertOk(newThreadQueue[Frame](8), "newThreadQueue q1 failed")
  var q2 = assertOk(newThreadQueue[Frame](8), "newThreadQueue q2 failed")
  var q3 = assertOk(newThreadQueue[Frame](8), "newThreadQueue q3 failed")
  var qOut = assertOk(newThreadQueue[Frame](8), "newThreadQueue qOut failed")
  var logQ = assertOk(newThreadQueue[LogLine](256), "newThreadQueue logQ failed")

  var bridge = assertOk(newAsyncThreadQueueBridge[Frame](qOut), "newAsyncThreadQueueBridge failed")
  let outTx = bridge.notifier()

  var th1: Thread[StageArgs]
  var th2: Thread[StageArgs]
  var th3: Thread[FinalStageArgs]

  createThread(th1, stageWorker, StageArgs(
    stageNo: 1,
    stageBit: 0b0000_0001'u8,
    input: q1,
    output: q2,
    logQ: logQ.handle,
  ))

  createThread(th2, stageWorker, StageArgs(
    stageNo: 2,
    stageBit: 0b0000_0010'u8,
    input: q2,
    output: q3,
    logQ: logQ.handle,
  ))

  createThread(th3, finalStageWorker, FinalStageArgs(
    stageNo: 3,
    stageBit: 0b0000_0100'u8,
    input: q3,
    output: outTx,
    logQ: logQ.handle,
  ))

  echo "async: pipeline started"
  echo "async: q1 -> worker1 -> q2 -> worker2 -> q3 -> worker3 -> async bridge"

  for i in 0 ..< MsgCount:
    let pending = bridge.recvAsync()

    echo &"async: sending frame #{i} to q1"

    # Do not move an async-proc local.  Sending an rvalue avoids moving a field
    # from the async macro environment object.
    let sendRet = q1.sendMove(makeFrame(i, BufSize))
    doAssert sendRet.isOk, "async start sendMove failed: " & $sendRet.error

    let box = await pending
    var frame = takeOk(box, "round trip AsyncOwned[Frame].take failed")

    drainLogs(logQ)

    echo &"async: received frame #{frame.index}, stages={stageMaskText(frame.stages)}, ptr={ptrHex(dataPtr(frame))}"
    echo ""

    doAssert not frame.stop, "unexpected stop frame while receiving data"
    doAssert frame.index == i, "round trip returned wrong index"
    doAssert frame.stages == 0b0000_0111'u8, "round trip did not pass all stages"
    doAssert dataPtr(frame) == frame.expectedPtr, "round trip changed seq backing pointer"
    doAssert frame.data.len == BufSize, "round trip returned wrong data length"
    doAssert frame.data[0] == byte(frame.seed), "round trip returned wrong payload"

  block:
    let pending = bridge.recvAsync()

    echo "async: sending STOP to q1"

    let sendRet = q1.sendMove(makeStopFrame())
    doAssert sendRet.isOk, "async stop sendMove failed: " & $sendRet.error

    let box = await pending
    var frame = takeOk(box, "round trip stop frame take failed")

    drainLogs(logQ)

    echo "async: received STOP from final bridge"
    echo ""

    doAssert frame.stop, "expected stop frame"
    doAssert frame.stages == 0'u8, "stop frame should not be marked as data"

  joinThread(th1)
  joinThread(th2)
  joinThread(th3)

  drainLogs(logQ)

  bridge.close()

proc main() =
  waitFor runRoundTripVerboseAsync()

main()
echo "OK: verbose async thread round-trip demo passed"
