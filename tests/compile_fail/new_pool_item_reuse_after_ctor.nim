import threadtools

type
  Buf = object
    data: seq[byte]

proc makeBuf(): Buf =
  result.data = newSeq[byte](1024)

var returnQ = newThreadQueue[Buf](1).expect("newThreadQueue failed")
var buf = makeBuf()
var item = newPoolItem[Buf](returnQ, buf)

# This must not compile.  newPoolItem() consumes buf through ensureMove().
discard buf.data.len

discard item.isActive
