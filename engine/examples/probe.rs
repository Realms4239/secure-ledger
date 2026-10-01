use secureledger_engine::ring::RingReader;
use std::path::PathBuf;

fn main() {
    let path = PathBuf::from(std::env::args().nth(1).expect("ring path"));
    // raw file read first (disk truth)
    match std::fs::read(&path) {
        Ok(bytes) => println!(
            "disk: len={} head={:02x?} magic={:02x?}",
            bytes.len(),
            &bytes[0..8],
            &bytes[136..140]
        ),
        Err(e) => println!("disk read err {e}"),
    }
    let mut reader = match RingReader::open(&path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("open failed: {e:?}");
            std::process::exit(1);
        }
    };
    for i in 0..3 {
        match reader.next() {
            Ok(Some(slot)) => {
                println!("poll {i}: GOT type={:#x}", slot.typ);
                return;
            }
            Ok(None) => println!("poll {i}: empty"),
            Err(e) => println!("poll {i}: err {e}"),
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
}
