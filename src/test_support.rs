//! Cross-module synchronization for `cargo test` only.
//!
//! Compiled solely under `#[cfg(test)]` for the library test binary; no
//! integration test and no shipped binary ever links it.

use std::sync::{Mutex, MutexGuard};

/// Serializes every test that holds a file lock directly against every code
/// path that forks a real child process.
///
/// `fork(2)` duplicates the *whole process's* fd table across every thread,
/// not just the caller's — so a flock another `cargo test` thread holds at
/// that instant is briefly visible (as still held) to the forked child, and
/// vice versa, until the child's `execve` closes its `O_CLOEXEC`-marked fds.
/// `cargo test`'s default thread parallelism runs the flock-assertion tests in
/// [`crate::service`] concurrently with tests elsewhere in the crate that
/// spawn real processes (notably [`crate::participant`]'s subprocess and
/// external-agent tests), so without this guard the two occasionally race: a
/// lock reads back as still held, or a fresh mailbox open is transiently
/// refused. This never happens in production — a real `baton service run`
/// process never shares an address space with unrelated flock-holding code —
/// it is purely a same-process test-parallelism artifact.
static FORK_LOCK_SERIALIZE: Mutex<()> = Mutex::new(());

/// Takes the shared guard described on [`FORK_LOCK_SERIALIZE`]. A poisoned
/// mutex is recovered rather than propagated: the guard protects nothing but
/// fd-table timing, so an unrelated test's panic must not cascade.
pub(crate) fn serialize_forks_and_locks() -> MutexGuard<'static, ()> {
    FORK_LOCK_SERIALIZE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
