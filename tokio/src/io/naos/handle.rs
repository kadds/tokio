use crate::io::interest::Interest;
use crate::io::ready::Ready;
use crate::runtime::io::ReadyEvent;
use crate::runtime::io::Registration;
use crate::runtime::scheduler;

use std::io;
use std::task::{Context, Poll};

/// A non-owning capability registration attached to Tokio's shared selector.
///
/// The capability handle is owned by the caller and is never closed here.
/// Dropping this value only removes its registration from the current Tokio
/// reactor.
#[derive(Debug)]
pub struct Readiness {
    source: mio::naos::Source,
    registration: Registration,
}

impl Readiness {
    /// Registers a capability handle with the current Tokio reactor.
    pub fn new(handle: u64) -> io::Result<Self> {
        let mut source = mio::naos::Source::new(handle);
        let registration = Registration::new_with_interest_and_handle(
            &mut source,
            Interest::READABLE,
            scheduler::Handle::current(),
        )?;
        Ok(Self {
            source,
            registration,
        })
    }

    /// Returns the borrowed capability handle represented by this registration.
    pub fn handle(&self) -> u64 {
        self.source.handle()
    }

    /// Polls the read-side readiness stream for this capability.
    pub fn poll_readable(&self, cx: &mut Context<'_>) -> Poll<io::Result<Event>> {
        self.registration
            .poll_read_ready(cx)
            .map_ok(Event::from_ready)
    }

    /// Clears Tokio's cached readiness after the caller drained the source.
    /// NaOS registrations are level-triggered, so this does not issue a
    /// kernel rearm operation.
pub fn clear_readiness(&self, event: Event) {
        self.registration.clear_readiness(event.kernel_event);
    }
}

impl Drop for Readiness {
    fn drop(&mut self) {
        let _ = self.registration.deregister(&mut self.source);
    }
}

/// A readiness event delivered by Tokio's shared selector.
#[derive(Clone, Copy, Debug)]
pub struct Event {
    /// Readiness flags delivered by Tokio's selector.
    pub ready: Ready,
    kernel_event: ReadyEvent,
}

impl Event {
    fn from_ready(readiness: ReadyEvent) -> Self {
        Self {
            ready: readiness.ready,
            kernel_event: readiness,
        }
    }
}
