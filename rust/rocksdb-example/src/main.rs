use rocksdb::{DB, SliceTransform};
use std::io;
use rocksdb::IteratorMode::Start;

fn main() {
    let path = "./db";
    let mut options = rocksdb::Options::default();
    options.optimize_for_point_lookup(16);
    options.create_if_missing(true);
    options.set_prefix_extractor(SliceTransform::create_fixed_prefix(1));
    let db = DB::open(&options, path).unwrap();

    loop {
        println!("Enter command (put, get, getall, search, quit):");

        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        let input = input.trim();

        let mut words = input.split_whitespace();
        let command = words.next().unwrap_or("");

        match command {
            "put" => {
                let key = words.next().unwrap_or("");
                let value = words.next().unwrap_or("");
                db.put(key.as_bytes(), value.as_bytes()).unwrap();
                println!("Data inserted: ({}, {})", key, value);
            }
            "getall" => {
                let iter = db.iterator(Start);
                println!("All data:");
                for elem in iter {
                    if elem.is_ok() {
                        let (key, value) = elem.unwrap();
                        println!("({}, {})",
                                 std::str::from_utf8(key.as_ref()).unwrap(),
                                 std::str::from_utf8(value.as_ref()).unwrap());
                    }
                }
            }
            "get" => {
                let key = words.next().unwrap_or("");
                match db.get(key.as_bytes()) {
                    Ok(Some(value)) => println!("Value for '{}': {}", key, std::str::from_utf8(value.as_ref()).unwrap()),
                    Ok(None) => println!("Key '{}' not found.", key),
                    Err(e) => println!("Database error: {:?}", e)
                }
            }
            "search" => {
                let prefix = words.next().unwrap_or("");
                // prefix_iterator searches for the entries with the same prefix as a requested key
                // options.set_prefix_extractor(SliceTransform::create_fixed_prefix(1)) in the beginning of the file
                // sets the prefix to 1. Only records with the same first byte are returned.
                // If prefix extractor is not set, this iterator will return the first record with
                // the longest same prefix as the requested key and all subsequent records.
                let iter = db.prefix_iterator(prefix.as_bytes());
                println!("Searching by prefix '{}':", prefix);
                for elem in iter {
                    if elem.is_ok() {
                        let (key, value) = elem.unwrap();
                        println!("({}, {})",
                                 std::str::from_utf8(key.as_ref()).unwrap(),
                                 std::str::from_utf8(value.as_ref()).unwrap());
                    }
                }
            }
            "quit" => break,
            _ => println!("Invalid command."),
        }
    }
}
