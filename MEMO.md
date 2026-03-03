# PhysicalClock Memo

## Quick Usage (MyClock)

`OmniPaxosConfig::build` and `ClusterConfig::build_for_server` take a `PhysicalClock`
instance by reference.

You can use any struct that implements the `PhysicalClock` trait.
Here is an example usage of `PhysicalClock` with `OmniPaxosConfig::build`:
```rust
use omnipaxos::{OmniPaxos, OmniPaxosConfig};
use omnipaxos::util::PhysicalClock;
use omnipaxos_storage::memory_storage::MemoryStorage;

struct MyClock;
impl PhysicalClock for MyClock {
    fn get_time(&self) -> i64 { 0 }
    fn get_uncertainty(&self) -> i64 { 0 }
}

let storage = MemoryStorage::default();
let my_clock = MyClock;
let clock = &my_clock;
let mut omni_paxos: OmniPaxos<'_, MyEntry, MemoryStorage<MyEntry>, MyClock> =
    OmniPaxosConfig::default().build(storage, clock).unwrap();
```

## Access From SequencePaxos (internal)

`SequencePaxos` stores a reference to the same clock, so methods can call it directly:

```rust
// inside SequencePaxos impl
let now_ns: i64 = self.clock.get_time();
let eps_ns: i64 = self.clock.get_uncertainty();
```
    
