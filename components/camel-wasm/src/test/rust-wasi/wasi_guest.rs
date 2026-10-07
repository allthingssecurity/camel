//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

//! Test guests for camel-wasm, built twice by build.sh:
//!
//! * with `--features command`: `wasi_guest.wasm`, a WASI preview1 command module (mode=wasi)
//! * without: `limits_guest.wasm`, a module with no imports for the function mode (alloc/dealloc ABI)
//!
//! The command module's first argument after the program name selects what the program does:
//!
//! | argument     | behaviour                                                         |
//! |--------------|-------------------------------------------------------------------|
//! | (none)/upper | copy stdin to stdout in upper case                                |
//! | exit N       | write "exiting" to stderr and exit with code N                    |
//! | stderr       | copy stdin to stderr, write "ok" to stdout                        |
//! | args         | write every argument on its own line                              |
//! | env          | write every environment variable on its own line                  |
//! | fs           | report whether file descriptor 3 is a preopened directory         |
//! | hog          | grow linear memory one page at a time until it fails, print pages |
//! | spin         | loop for ever                                                     |
//! | trap         | execute `unreachable`                                             |

#![no_std]

#[panic_handler]
fn panic(_: &core::panic::PanicInfo) -> ! {
    core::arch::wasm32::unreachable()
}

#[cfg(feature = "command")]
mod command {
    use core::arch::wasm32;

    #[repr(C)]
    struct IoVec {
        buf: *const u8,
        len: usize,
    }

    #[link(wasm_import_module = "wasi_snapshot_preview1")]
    extern "C" {
        fn fd_read(fd: i32, iovs: *const IoVec, iovs_len: i32, nread: *mut usize) -> i32;
        fn fd_write(fd: i32, iovs: *const IoVec, iovs_len: i32, nwritten: *mut usize) -> i32;
        fn fd_prestat_get(fd: i32, prestat: *mut u8) -> i32;
        fn args_sizes_get(argc: *mut usize, buf_size: *mut usize) -> i32;
        fn args_get(argv: *mut *mut u8, buf: *mut u8) -> i32;
        fn environ_sizes_get(count: *mut usize, buf_size: *mut usize) -> i32;
        fn environ_get(environ: *mut *mut u8, buf: *mut u8) -> i32;
        fn proc_exit(code: i32) -> !;
    }

    const STDIN: i32 = 0;
    const STDOUT: i32 = 1;
    const STDERR: i32 = 2;

    fn write(fd: i32, mut data: &[u8]) {
        while !data.is_empty() {
            let iov = IoVec {
                buf: data.as_ptr(),
                len: data.len(),
            };
            let mut n = 0usize;
            if unsafe { fd_write(fd, &iov, 1, &mut n) } != 0 {
                exit(71);
            }
            data = &data[n..];
        }
    }

    fn read(fd: i32, buf: &mut [u8]) -> usize {
        let iov = IoVec {
            buf: buf.as_mut_ptr(),
            len: buf.len(),
        };
        let mut n = 0usize;
        if unsafe { fd_read(fd, &iov, 1, &mut n) } != 0 {
            exit(72);
        }
        n
    }

    fn exit(code: i32) -> ! {
        unsafe { proc_exit(code) }
    }

    fn write_number(fd: i32, mut v: u32) {
        let mut digits = [0u8; 10];
        let mut i = digits.len();
        loop {
            i -= 1;
            digits[i] = b'0' + (v % 10) as u8;
            v /= 10;
            if v == 0 {
                break;
            }
        }
        write(fd, &digits[i..]);
    }

    fn parse_number(s: &[u8]) -> i32 {
        let mut v: i32 = 0;
        for b in s {
            if b.is_ascii_digit() {
                v = v.wrapping_mul(10).wrapping_add((b - b'0') as i32);
            }
        }
        v
    }

    /// Reads a NUL-separated list (argv or environ) into `buf` and returns the number of entries.
    fn read_list(
        buf: &mut [u8],
        ptrs: &mut [*mut u8],
        sizes: unsafe extern "C" fn(*mut usize, *mut usize) -> i32,
        get: unsafe extern "C" fn(*mut *mut u8, *mut u8) -> i32,
    ) -> usize {
        let mut count = 0usize;
        let mut size = 0usize;
        if unsafe { sizes(&mut count, &mut size) } != 0 || count > ptrs.len() || size > buf.len() {
            exit(70);
        }
        if unsafe { get(ptrs.as_mut_ptr(), buf.as_mut_ptr()) } != 0 {
            exit(70);
        }
        count
    }

    /// Returns entry `i` of a list read by `read_list`, without its terminating NUL.
    fn entry<'a>(buf: &'a [u8], ptrs: &[*mut u8], i: usize) -> &'a [u8] {
        let start = ptrs[i] as usize - buf.as_ptr() as usize;
        let len = buf[start..]
            .iter()
            .position(|b| *b == 0)
            .unwrap_or(buf.len() - start);
        &buf[start..start + len]
    }

    #[no_mangle]
    pub extern "C" fn _start() {
        let mut arg_buf = [0u8; 4096];
        let mut arg_ptrs = [core::ptr::null_mut::<u8>(); 64];
        let argc = read_list(&mut arg_buf, &mut arg_ptrs, args_sizes_get, args_get);
        let command: &[u8] = if argc > 1 {
            entry(&arg_buf, &arg_ptrs, 1)
        } else {
            b"upper"
        };

        let mut io = [0u8; 4096];
        match command {
            b"upper" => loop {
                let n = read(STDIN, &mut io);
                if n == 0 {
                    break;
                }
                io[..n].make_ascii_uppercase();
                write(STDOUT, &io[..n]);
            },
            b"stderr" => {
                loop {
                    let n = read(STDIN, &mut io);
                    if n == 0 {
                        break;
                    }
                    write(STDERR, &io[..n]);
                }
                write(STDOUT, b"ok");
            }
            b"exit" => {
                write(STDERR, b"exiting");
                let code = if argc > 2 {
                    parse_number(entry(&arg_buf, &arg_ptrs, 2))
                } else {
                    1
                };
                exit(code);
            }
            b"args" => {
                for i in 0..argc {
                    write(STDOUT, entry(&arg_buf, &arg_ptrs, i));
                    write(STDOUT, b"\n");
                }
            }
            b"env" => {
                let mut env_buf = [0u8; 8192];
                let mut env_ptrs = [core::ptr::null_mut::<u8>(); 128];
                let n = read_list(&mut env_buf, &mut env_ptrs, environ_sizes_get, environ_get);
                for i in 0..n {
                    write(STDOUT, entry(&env_buf, &env_ptrs, i));
                    write(STDOUT, b"\n");
                }
            }
            b"fs" => {
                let mut prestat = [0u8; 8];
                let errno = unsafe { fd_prestat_get(3, prestat.as_mut_ptr()) };
                if errno == 0 {
                    write(STDOUT, b"preopened");
                } else {
                    write(STDOUT, b"no preopened directory, errno ");
                    write_number(STDOUT, errno as u32);
                }
            }
            b"hog" => {
                while wasm32::memory_grow(0, 1) != usize::MAX {}
                write_number(STDOUT, wasm32::memory_size(0) as u32);
            }
            b"spin" => {
                let mut i = 0u64;
                loop {
                    // volatile so the loop is not optimised away
                    unsafe { core::ptr::write_volatile(&mut i, i.wrapping_add(1)) };
                }
            }
            b"trap" => wasm32::unreachable(),
            _ => {
                write(STDERR, b"unknown command");
                exit(64);
            }
        }
    }
}

/// Function mode guest: `alloc`/`dealloc` plus `misbehave(ptr, len) -> u64`, which returns its input unchanged unless
/// the input contains `spin` (loops for ever) or `trap` (grows memory by 8 pages, then traps; if memory cannot grow,
/// returns the error "out of memory" through the error flag instead).
#[cfg(not(feature = "command"))]
mod function {
    use core::arch::wasm32;

    const ARENA_SIZE: usize = 64 * 1024;
    static mut ARENA: [u8; ARENA_SIZE] = [0; ARENA_SIZE];
    static mut NEXT: usize = 0;
    static OOM: &[u8] = b"out of memory";

    #[no_mangle]
    pub extern "C" fn alloc(size: u32) -> *mut u8 {
        // bump allocator; tests send small messages and one call at a time
        unsafe {
            let size = size as usize;
            if NEXT + size > ARENA_SIZE {
                NEXT = 0;
            }
            let p = core::ptr::addr_of_mut!(ARENA).cast::<u8>().add(NEXT);
            NEXT += size;
            p
        }
    }

    #[no_mangle]
    pub extern "C" fn dealloc(_ptr: *mut u8, _len: i32) {}

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    #[no_mangle]
    pub extern "C" fn misbehave(ptr: u32, len: u32) -> u64 {
        let input = unsafe { core::slice::from_raw_parts(ptr as *const u8, len as usize) };
        if contains(input, b"spin") {
            let mut i = 0u64;
            loop {
                unsafe { core::ptr::write_volatile(&mut i, i.wrapping_add(1)) };
            }
        }
        if contains(input, b"trap") {
            if wasm32::memory_grow(0, 8) == usize::MAX {
                return ((OOM.as_ptr() as u64) << 32) | (OOM.len() as u64) | (1u64 << 31);
            }
            wasm32::unreachable();
        }
        let out = alloc(len);
        unsafe { core::ptr::copy_nonoverlapping(input.as_ptr(), out, len as usize) };
        ((out as u64) << 32) | len as u64
    }
}
