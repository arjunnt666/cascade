//! FFI surface for language bindings.

pub use cascade_core::{
    Payload, WorkflowId, RunId, ActivityId, CascadeError, Result,
};

#[no_mangle]
pub extern "C" fn cascade_version() -> *const std::os::raw::c_char {
    b"0.1.0\0".as_ptr() as *const _
}
