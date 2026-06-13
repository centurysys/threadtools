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
var buf = makeBuf(32)
var item1 = newPoolItem[Buf](returnQ, move buf)

# Expected compile failure:
# PoolItem is an ownership token and must not be copyable.
# This assignment should require copying item1, because item1 is used again below.
var item2 = item1

discard item2.isActive()
discard item1.isActive()
