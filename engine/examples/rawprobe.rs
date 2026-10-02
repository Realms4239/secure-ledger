// Raw WinAPI mapped-view probe: bypasses memmap2 entirely.
#![windows_subsystem = "console"]

use windows_sys::Win32::{
    Foundation::{GENERIC_READ, GENERIC_WRITE},
    Storage::FileSystem::{CreateFileW, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING},
    System::Memory::{CreateFileMappingW, MapViewOfFile, FILE_MAP_READ, PAGE_READONLY},
};

fn main() {
    unsafe {
        let path: Vec<u16> = std::env::args()
            .nth(1)
            .expect("path")
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect();
        let h = CreateFileW(
            path.as_ptr(),
            (GENERIC_READ | GENERIC_WRITE) as u32,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            std::ptr::null(),
            OPEN_EXISTING,
            0,
            std::ptr::null_mut(),
        );
        if h.is_null() || h as isize == -1 {
            println!("CreateFileW failed");
            return;
        }
        let section =
            CreateFileMappingW(h, std::ptr::null(), PAGE_READONLY, 0, 0, std::ptr::null());
        if section.is_null() {
            println!("CreateFileMappingW failed");
            return;
        }
        let view = MapViewOfFile(section, FILE_MAP_READ, 0, 0, 0);
        if view.Value.is_null() {
            println!("MapViewOfFile failed");
            return;
        }
        let bytes = std::slice::from_raw_parts(view.Value as *const u8, 144);
        println!(
            "raw-view head={:02x?} tail={:02x?} magic={:02x?}",
            &bytes[0..8],
            &bytes[64..72],
            &bytes[136..140]
        );
    }
}
