use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
static _LOCK: AtomicBool = AtomicBool::new(false);

static _TEST_ID: AtomicUsize = AtomicUsize::new(0);
/// -------Funciones especificas para no tener problemas al correr los test ---------

/// A simple function to acquire a spinlock.
/// This function sets a global lock variable to `true`
/// using atomic operations. It blocks the current thread in
/// a loop until the lock becomes available.
pub fn _acquire_write() {
    while _LOCK.swap(true, Ordering::Acquire) {}
}

/// A simple function to release a spinlock.
///
/// This function sets a global lock variable to `false`
/// using atomic operations, allowing other threads to acquire it.

pub fn _release_write() {
    _LOCK.store(false, Ordering::Release);
}
