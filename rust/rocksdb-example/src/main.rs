use rocksdb::DB;
use std::io;
use rocksdb::IteratorMode::Start;

fn main() {
    let path = "./db";
    let db = DB::open_default(path).unwrap();

    loop {
        println!("Enter command (put, get, getall, quit):");

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
            "quit" => break,
            _ => println!("Invalid command."),
        }
    }
}

