use serde::{Deserialize, Serialize};
use crate::messages::sequence_paxos::DomPropose;
use crate::storage::{Entry, Snapshot};

#[derive(Clone, Debug)]
pub(crate) struct TestEntry;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct TestSnapshot;

impl Snapshot<TestEntry> for TestSnapshot {
    fn create(_entries: &[TestEntry]) -> Self {
        TestSnapshot
    }

    fn merge(&mut self, _delta: Self) {}

    fn use_snapshots() -> bool {
        false
    }
}

impl Entry for TestEntry {
    type Snapshot = TestSnapshot;
}

/// Returns true if the two proposals have the same deadline and entry id.
pub(crate) fn compare_proposes(prop1: DomPropose<TestEntry>, prop2: DomPropose<TestEntry>) -> bool {
    prop1.deadline == prop2.deadline && prop1.entry_id == prop2.entry_id
}
