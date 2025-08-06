use duckdb::{params, Connection, Result};

const CONN: Result<Connection>;

fn main() {
    let path = "./database.db3";
    CONN = Connection::open(&path); 
    match CONN {
        Ok(connection) => println!("Connection established to database. Version: {}", connection.version()),
        Err(err) => println!("Error connecting to db: {err}")
    }
}
