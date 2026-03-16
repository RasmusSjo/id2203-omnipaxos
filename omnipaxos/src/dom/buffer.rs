use crate::messages::sequence_paxos::{DomPropose, EntryId};
use crate::storage::Entry;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap};

/// Key used to order entries in the early buffer.
/// Ordering is based on deadline, with entry id as a tie-breaker.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) struct BufferKey {
    deadline: i64,
    entry_id: EntryId,
}

impl<T: Entry> From<&DomPropose<T>> for BufferKey {
    fn from(proposal: &DomPropose<T>) -> Self {
        Self {
            deadline: proposal.deadline,
            entry_id: proposal.entry_id,
        }
    }
}

/// Entry in the early buffer. Ordering is based only on the buffer key:
/// deadline first, then entry id as a tie-breaker.
struct EBEntry<T: Entry> {
    key: BufferKey,
    proposal: DomPropose<T>,
}

impl<T: Entry> PartialEq for EBEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<T: Entry> Eq for EBEntry<T> {}

impl<T: Entry> PartialOrd for EBEntry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Entry> Ord for EBEntry<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.key.cmp(&self.key) // Reverse order, smallest deadline first
    }
}

impl<T: Entry> EBEntry<T> {
    fn new(proposal: DomPropose<T>) -> Self {
        let key = BufferKey::from(&proposal);

        Self { key, proposal }
    }
}

/// A buffer that stores client requests whose deadline has not yet passed.
pub(crate) struct EarlyBuffer<T: Entry> {
    heap: BinaryHeap<EBEntry<T>>,
    last_released: Option<BufferKey>,
}

impl<T: Entry> EarlyBuffer<T> {
    /// Creates a new early buffer.
    pub(crate) fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            last_released: None,
        }
    }

    /// Inserts the proposal into the early buffer.
    /// Returns an error if the proposal is late.
    pub(crate) fn insert(
        &mut self,
        proposal: DomPropose<T>,
    ) -> Result<(), EarlyBufferInsertError<T>> {
        // TODO check for duplicates
        if self.is_late(&proposal) {
            return Err(EarlyBufferInsertError::Late(proposal));
        }

        self.heap.push(EBEntry::new(proposal));
        Ok(())
    }

    pub(crate) fn last_released_deadline(&self) -> Option<i64> {
        self.last_released.map(|key| key.deadline)
    }

    fn is_late(&self, proposal: &DomPropose<T>) -> bool {
        self.last_released
            .is_some_and(|last| BufferKey::from(proposal) <= last)
    }

    /// Returns the earliest proposal in the early buffer, if any.
    pub(crate) fn peek(&self) -> Option<&DomPropose<T>> {
        self.heap.peek().map(|entry| &entry.proposal)
    }

    /// Removes and returns the earliest proposal in the early buffer, if any.
    pub(crate) fn pop(&mut self) -> Option<DomPropose<T>> {
        let entry = self.heap.pop()?;
        self.last_released = Some(entry.key);
        Some(entry.proposal)
    }

    /// Removes and returns the earliest proposal in the early buffer that is
    /// ready to be released, if any.
    pub(crate) fn pop_ready(&mut self, now: i64) -> Option<DomPropose<T>> {
        if self.peek().is_some_and(|next| next.deadline <= now) {
            self.pop()
        } else {
            None
        }
    }

    pub(crate) fn remove(&mut self, entry_id: EntryId) -> Option<DomPropose<T>> {
        // This is inefficient, but we expect the early buffer to be small.
        let mut removed = None;
        let mut new_heap = BinaryHeap::new();
        while let Some(entry) = self.heap.pop() {
            if entry.proposal.entry_id == entry_id {
                removed = Some(entry.proposal);
            } else {
                new_heap.push(entry);
            }
        }
        self.heap = new_heap;
        removed
    }
}

/// Error signalling something went wrong when inserting a request into the early buffer.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub(crate) enum EarlyBufferInsertError<T: Entry> {
    /// The request was late and could not be inserted.
    Late(DomPropose<T>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dom::test_utils::*;
    
    #[test]
    fn early_buffer_correct_insert_test() {
        let mut buffer: EarlyBuffer<TestEntry> = EarlyBuffer::new();

        let propose1 = DomPropose {
            entry_id: EntryId { client_id: 0, command_id: 0 },
            sender: 0,
            sent_time: (10, 5),
            deadline: 20,
            entry: TestEntry,
        };

        let res = buffer.insert(propose1.clone());
        assert!(res.is_ok(), "First proposal should be accepted");

        let propose2 = DomPropose {
            entry_id: EntryId { client_id: 1, command_id: 0 },
            sender: 0,
            sent_time: (20, 5),
            deadline: 30,
            entry: TestEntry,
        };

        let propose3 = DomPropose {
            entry_id: EntryId { client_id: 0, command_id: 1 },
            sender: 0,
            sent_time: (20, 5),
            deadline: 30,
            entry: TestEntry,
        };

        let res = buffer.insert(propose2.clone());
        assert!(res.is_ok(), "Second proposal should be accepted");
        let res = buffer.insert(propose3.clone());
        assert!(res.is_ok(), "Third proposal should be accepted");

        let not_ready = buffer.pop_ready(10).is_none();
        assert!(not_ready, "No proposals should be ready yet");

        let first= buffer.pop_ready(20).unwrap();
        assert!(compare_proposes(first, propose1), "First proposal should be released");

        let second = buffer.pop_ready(40).unwrap();
        let third = buffer.pop_ready(40).unwrap();
        assert!(compare_proposes(second, propose3), "Tie-break should order this before the second proposal");
        assert!(compare_proposes(third, propose2), "Tie-break should order this after the third proposal");

        let empty = buffer.peek();
        assert!(empty.is_none(), "Early buffer should be empty");
    }

    #[test]
    fn early_buffer_reject_late_test() {
        let mut buffer: EarlyBuffer<TestEntry> = EarlyBuffer::new();

        let first_propose = DomPropose {
            entry_id: EntryId { client_id: 0, command_id: 0 },
            sender: 0,
            sent_time: (10, 5),
            deadline: 20,
            entry: TestEntry,
        };

        let res = buffer.insert(first_propose);
        assert!(res.is_ok(), "First proposal should be accepted");

        let ready = buffer.pop_ready(25).unwrap();
        assert_eq!(buffer.last_released.unwrap().deadline, ready.deadline);

        let late_propose = DomPropose {
            entry_id: EntryId { client_id: 1, command_id: 0 },
            sender: 1,
            sent_time: (5, 1),
            deadline: ready.deadline - 1,
            entry: TestEntry,
        };

        let err = buffer.insert(late_propose);
        assert!(err.is_err(), "Late proposal should not be accepted");
    }
}
