//! C ABI. Only the version for now; the rest goes over HTTP.

use std::ffi::{c_char, CStr};

pub use cascade_core::{ActivityId, CascadeError, Payload, Result, RunId, WorkflowId};

const VERSION: &CStr =
    match CStr::from_bytes_with_nul(concat!(env!("CARGO_PKG_VERSION"), "\0").as_bytes()) {
        Ok(v) => v,
        Err(_) => panic!("version has an interior nul"),
    };

/// Static string, never free it.
#[no_mangle]
pub extern "C" fn cascade_version() -> *const c_char {
    VERSION.as_ptr()
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_matches_crate() {
        let v = unsafe { std::ffi::CStr::from_ptr(super::cascade_version()) };
        assert_eq!(v.to_str().unwrap(), env!("CARGO_PKG_VERSION"));
    }
}
