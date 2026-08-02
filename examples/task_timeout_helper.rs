//! A tiny Unix-only child used by the task timeout unit test.
//!
//! The test controls whether `SIGTERM` exits the child or leaves it alive for
//! `SIGKILL`. This example is built on demand by the service test and is not
//! part of the shipped CLI.

#[cfg(unix)]
mod unix {
    use std::env;
    use std::fs;
    use std::os::raw::c_int;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    const SIGTERM: c_int = 15;
    static TERM_RECEIVED: AtomicBool = AtomicBool::new(false);

    // `signal(2)` is the common POSIX interface available on Linux and macOS.
    // The handler is represented as `usize` by both platforms' C ABI.
    unsafe extern "C" {
        fn signal(signum: c_int, handler: usize) -> usize;
    }

    extern "C" fn exit_on_term(_: c_int) {
        TERM_RECEIVED.store(true, Ordering::Relaxed);
    }

    extern "C" fn ignore_term(_: c_int) {}

    fn install_term_handler(handler: extern "C" fn(c_int)) -> Result<(), String> {
        // SAFETY: `handler` has C ABI and remains a valid function pointer for
        // the lifetime of this process. `SIGTERM` is a valid POSIX signal.
        let previous = unsafe { signal(SIGTERM, handler as usize) };
        if previous == usize::MAX {
            Err("could not install SIGTERM handler".to_string())
        } else {
            Ok(())
        }
    }

    fn usage() -> ! {
        eprintln!(
            "usage: task_timeout_helper --mode <exit-on-term|ignore-term> --ready-file <path>"
        );
        std::process::exit(2);
    }

    pub(super) fn run() -> Result<(), String> {
        let mut mode = None;
        let mut ready_file = None;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--mode" => mode = args.next().or_else(|| usage()),
                "--ready-file" => ready_file = args.next().or_else(|| usage()),
                _ => usage(),
            }
        }
        let mode = match mode {
            Some(mode) => mode,
            None => usage(),
        };
        let ready_file = match ready_file {
            Some(ready_file) => ready_file,
            None => usage(),
        };

        let handler = match mode.as_str() {
            "exit-on-term" => exit_on_term,
            "ignore-term" => ignore_term,
            _ => usage(),
        };
        install_term_handler(handler)?;
        fs::write(&ready_file, b"ready\n")
            .map_err(|err| format!("could not write readiness file {ready_file:?}: {err}"))?;

        loop {
            if mode == "exit-on-term" && TERM_RECEIVED.load(Ordering::Relaxed) {
                return Ok(());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(unix)]
fn main() {
    if let Err(err) = unix::run() {
        eprintln!("task_timeout_helper: {err}");
        std::process::exit(1);
    }
}

#[cfg(not(unix))]
fn main() {
    eprintln!("task_timeout_helper requires Unix");
    std::process::exit(1);
}
