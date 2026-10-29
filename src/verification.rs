//! Shared per-store verification concurrency and I/O rate budgets.
use anyhow::Result;
use parking_lot::{Condvar, Mutex};
use std::time::{Duration, Instant};

pub(crate) struct Control {
    state: Mutex<State>,
    changed: Condvar,
}
struct State {
    active: usize,
    next_io: Instant,
}
pub(crate) struct Permit<'a>(&'a Control);
impl Drop for Permit<'_> {
    fn drop(&mut self) {
        let mut state = self.0.state.lock();
        state.active -= 1;
        self.0.changed.notify_all();
    }
}
impl Default for Control {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                active: 0,
                next_io: Instant::now(),
            }),
            changed: Condvar::new(),
        }
    }
}
impl Control {
    pub(crate) fn acquire(&self, max: usize, check: impl Fn() -> Result<()>) -> Result<Permit<'_>> {
        let mut state = self.state.lock();
        loop {
            check()?;
            if state.active < max {
                state.active += 1;
                return Ok(Permit(self));
            }
            self.changed.wait_for(&mut state, Duration::from_millis(20));
        }
    }
    pub(crate) fn throttle(
        &self,
        bytes: u64,
        rate_mib: u64,
        check: impl Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        if rate_mib == 0 || bytes == 0 {
            return Ok(());
        }
        let until = {
            let mut state = self.state.lock();
            let start = state.next_io.max(Instant::now());
            state.next_io =
                start + Duration::from_secs_f64(bytes as f64 / (rate_mib as f64 * 1048576.0));
            state.next_io
        };
        loop {
            check()?;
            let now = Instant::now();
            if now >= until {
                return Ok(());
            }
            std::thread::sleep((until - now).min(Duration::from_millis(20)));
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn budget_limits_wait_are_cancellable_and_permits_release() {
        let c = Control::default();
        let permit = c.acquire(1, || Ok(())).unwrap();
        assert!(c.acquire(1, || anyhow::bail!("cancelled")).is_err());
        drop(permit);
        let _next = c.acquire(1, || Ok(())).unwrap();
        let start = Instant::now();
        c.throttle(64 * 1024, 1, || Ok(())).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(60));
        assert!(
            c.throttle(64 * 1024, 1, || anyhow::bail!("cancelled"))
                .is_err()
        );
    }
}
