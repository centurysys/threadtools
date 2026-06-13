# Verbose async thread round-trip demo output

This demo is intentionally noisy.  It is useful for manually confirming the
pipeline flow.

Run:

```sh
nim c -r -d:release --mm:orc tests/demo_async_thread_roundtrip_verbose.nim
```

Example output shape:

```text
async: pipeline started
async: q1 -> worker1 -> q2 -> worker2 -> q3 -> worker3 -> async bridge
async: sending frame #0 to q1
  worker1: received frame #0, stages=0b000, ptr=0x...
  worker1: sending frame #0, stages=0b001, ptr=0x...
  worker2: received frame #0, stages=0b001, ptr=0x...
  worker2: sending frame #0, stages=0b011, ptr=0x...
  worker3: received frame #0, stages=0b011, ptr=0x...
  worker3: sending frame #0, stages=0b111, ptr=0x...
async: received frame #0, stages=0b111, ptr=0x...

async: sending STOP to q1
  worker1: received STOP
  worker1: forwarding STOP
  worker2: received STOP
  worker2: forwarding STOP
  worker3: received STOP
  worker3: forwarding STOP
async: received STOP from final bridge

OK: verbose async thread round-trip demo passed
```

The pointer value should stay the same for a given frame across all worker stages and the final async receive.
