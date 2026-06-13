import threadtools

type
  Buf = object
    data: seq[byte]

proc makeBuf(n: int): Buf =
  result.data = newSeq[byte](n)

proc assertOk[T](r: Result[T, ErrorCode]; msg: string): T =
  doAssert r.isOk, msg & ": " & $r.error
  return r.get()

var returnQ = assertOk(newThreadQueue[Buf](4), "newThreadQueue returnQ failed")
var outQ = assertOk(newThreadQueue[PoolItem[Buf]](4), "newThreadQueue outQ failed")
var buf = makeBuf(32)
var item = newPoolItem[Buf](returnQ, move buf)

# Expected compile failure:
# sendMove consumes the ownership token. Reusing item after sendMove
# would require keeping a copy or using a moved-from value.
discard outQ.sendMove(item)

discard item.isActive()
