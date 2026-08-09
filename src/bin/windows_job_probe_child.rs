#[cfg(windows)]
#[allow(clippy::zombie_processes)]
fn main() {
    use std::env;
    use std::ffi::c_void;
    use std::mem::size_of;
    use std::thread;
    use std::time::Duration;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE};
    use windows_sys::Win32::System::JobObjects::{
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
        QueryInformationJobObject,
    };

    let mut args = env::args().skip(1);
    match args.next().as_deref() {
        Some("--sleep") => thread::sleep(Duration::from_secs(60)),
        Some("--check-job-handle") => {
            let handle = args
                .next()
                .expect("job handle argument")
                .parse::<isize>()
                .expect("numeric job handle") as HANDLE;
            let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
                TotalUserTime: 0,
                TotalKernelTime: 0,
                ThisPeriodTotalUserTime: 0,
                ThisPeriodTotalKernelTime: 0,
                TotalPageFaultCount: 0,
                TotalProcesses: 0,
                ActiveProcesses: 0,
                TotalTerminatedProcesses: 0,
            };
            // SAFETY: The inherited numeric handle is passed back to the
            // kernel with a writable buffer of the documented structure size.
            let query_ok = unsafe {
                QueryInformationJobObject(
                    handle,
                    JobObjectBasicAccountingInformation,
                    &mut info as *mut _ as *mut c_void,
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    std::ptr::null_mut(),
                ) != 0
            };
            println!("job_handle_inherited={query_ok}");
            if query_ok {
                // SAFETY: A successful query proves this is a live kernel
                // handle owned by this process, so closing it is valid.
                unsafe { CloseHandle(handle) };
            }
        }
        Some("--spawn-grandchild") => {
            let child = std::process::Command::new(env::current_exe().expect("current exe"))
                .arg("--sleep")
                .spawn()
                .expect("spawn grandchild");
            println!("{}", child.id());
        }
        other => panic!("unknown probe child mode: {other:?}"),
    }
}

#[cfg(not(windows))]
fn main() {}
