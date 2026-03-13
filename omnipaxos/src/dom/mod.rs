use std::cmp::max;
use crate::dom::buffer::{EarlyBuffer, EarlyBufferInsertError, LateBuffer};
use crate::dom::owd_estimator::{OutgoingOwdTracker, OwdEstimator, OwdSample};
use crate::messages::sequence_paxos::{DomAck, DomPropose, EntryId, ClockTimestamp};
use crate::sequence_paxos::Role;
use crate::storage::Entry;
use crate::util::{NodeId, PhysicalClock};

mod buffer;
mod owd_estimator;
pub(crate) use owd_estimator::{OwdEstimatorConfig, EstimatorStrategy};

const ONE_MICRO_SECOND: i64 = 1000;

/// The Deadline-Ordered-Multicast abstraction.
pub(crate) struct Dom<'a, T, C>
where
    T: Entry,
    C: PhysicalClock,
{
    pid: NodeId,
    clock: &'a C,
    incoming_owd_estimates: OwdEstimator,
    outgoing_owd_estimates: OutgoingOwdTracker,
    eb: EarlyBuffer<T>,
    lb: LateBuffer<T>,
}

impl<'a, T, C> Dom<'a, T, C>
where
    T: Entry,
    C: PhysicalClock,
{
    /// Creates a new instance of the DOM abstraction.
    pub(crate) fn new(config: OwdEstimatorConfig, clock: &'a C, pid: NodeId) -> Self {
        Self {
            pid,
            clock,
            incoming_owd_estimates: OwdEstimator::with_config(config),
            outgoing_owd_estimates: OutgoingOwdTracker::new(config.max_owd()),
            eb: EarlyBuffer::new(),
            lb: LateBuffer::new(),
        }
    }

    /// Creates a new DOM proposal for the given entry.
    pub(crate) fn create_dom_propose(&self, entry: T, entry_id: EntryId) -> DomPropose<T> {
        let sent_time: ClockTimestamp = self.clock.get_time_with_uncertainty();

        DomPropose {
            entry_id,
            sender: self.pid,
            sent_time,
            deadline: self.get_deadline(sent_time.0),
            entry,
        }
    }

    fn get_deadline(&self, time: i64) -> i64 {
        // TODO do we need this to be more pessimistic? E.g. deadline = OWD + uncertainty
        time + self.outgoing_owd_estimates.get_max_owd()
    }

    /// Returns all proposals in the early buffer with a deadline that is
    /// before the current time.
    /// TODO Should we make the return type Vec<T> instead?
    pub(crate) fn release_ready(&mut self) -> Vec<DomPropose<T>> {
        let now = self.clock.get_time_with_uncertainty();
        let mut proposals = Vec::new();

        while let Some(prop) = self.eb.pop_ready(now.0 - now.1) {
            proposals.push(prop);
        }

        proposals
    }

    /// Handles a DOM proposal for the given role. Returns an ack with the
    /// estimated one-way delay of the proposal to be returned to the sender
    pub(crate) fn handle_dom_propose(
        &mut self,
        prop: DomPropose<T>,
        role: Role,
    ) -> DomAck {
        let received_time = self.clock.get_time_with_uncertainty();
        let owd_sample = OwdSample::new(
            prop.sent_time.0,
            received_time.0,
            prop.sent_time.1,
            received_time.1,
        );

        self.incoming_owd_estimates.update(prop.sender, owd_sample);

        let ack = DomAck {
            sender: self.pid,
            estimated_owd: self.incoming_owd_estimates.estimate_for(prop.sender),
        };

        match self.eb.insert(prop) {
            Ok(_) => { },
            Err(EarlyBufferInsertError::Late(prop)) => {
                self.lb.insert(prop.clone());
                if role == Role::Leader {
                    let prop = self.extend_deadline(prop);
                    self.eb.insert(prop).expect("proposal cannot be late");
                }
            }
        };

        ack
    }

    fn extend_deadline(&self, prop: DomPropose<T>) -> DomPropose<T> {
        let last_deadline = self.eb.last_released_deadline();
        let now = self.clock.get_time_with_uncertainty();
        let now_with_uncertainty = now.0 + now.1;
        
        let new_deadline = match last_deadline {
            Some(last) => max(now_with_uncertainty, last + ONE_MICRO_SECOND),
            None => now_with_uncertainty,
        };

        DomPropose {
            entry_id: prop.entry_id,
            sender: prop.sender,
            sent_time: prop.sent_time,
            deadline: new_deadline,
            entry: prop.entry,
        }
    }

    /// Handles a DOM ack.
    pub(crate) fn handle_dom_ack(&mut self, ack: DomAck) {
        self.outgoing_owd_estimates.update(ack.sender, ack.estimated_owd);
    }

    /// Removes the proposal with the given entry id from the buffers. Returns the proposal if it was found in any of the buffers.
    pub(crate) fn remove_from_buffers(&mut self, entry_id: EntryId) -> Option<DomPropose<T>> {
        if let Some(prop) = self.eb.remove(entry_id) {
            Some(prop)
        } else {
            self.lb.remove(entry_id)
        }
    }
}
