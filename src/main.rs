use duckdb::{Connection};
use duckdb::types::ValueRef;

fn main() {
    let path = "./database.db3";
    let conn = Connection::open(&path);
    match conn {
        Ok(connection) => {
            println!("Connection established to database. Version: {}", connection.version().unwrap());
            loop {
                let mut eingabe = String::new();
                println!("Enter SQL statement: ");
                std::io::stdin().read_line(&mut eingabe).unwrap_or_else(|err| {
                    println!("Error reading line: {}", err);
                    0
                });
                let result = query_db(&eingabe, &connection);
                for row in result {
                    println!("{row}");
                }
            }
        },
        Err(err) => {
                println!("Error connecting to db: {:?}", err)
        }
    }
}

fn query_db(statement: &str, connection: &Connection) -> Vec<String> {
    let mut result = Vec::new();
    let stmt = connection.prepare(statement);
    match stmt {
        Ok(mut statement) => {
            let rows = statement.query_map([], |row| {
                let mut row_string = String::new();
                let mut i = 0;
                loop {
                    let current_row: Result<ValueRef, duckdb::Error> = row.get_ref(i);
                    match current_row {
                        Ok(value) => {
                            let value_str = match value {
                                ValueRef::Null => "NULL".to_string(),
                                ValueRef::Boolean(v) => v.to_string(),
                                ValueRef::TinyInt(v) => v.to_string(),
                                ValueRef::SmallInt(v) => v.to_string(),
                                ValueRef::Int(v) => v.to_string(),
                                ValueRef::BigInt(v) => v.to_string(),
                                ValueRef::HugeInt(v) => v.to_string(),
                                ValueRef::UTinyInt(v) => v.to_string(),
                                ValueRef::USmallInt(v) => v.to_string(),
                                ValueRef::UInt(v) => v.to_string(),
                                ValueRef::UBigInt(v) => v.to_string(),
                                ValueRef::Float(v) => v.to_string(),
                                ValueRef::Double(v) => v.to_string(),
                                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                                _ => "[unsupported]".to_string(), // fallback
                            };
                            row_string.push_str(&value_str);
                            row_string.push_str(", ");
                        },
                        Err(_) => {
                            row_string.remove(row_string.len() - 2);
                            break;
                        }
                    }
                    i += 1;
                };
                Ok(row_string)
            });
            match rows {
                Ok(rows) => {
                    for row in rows {
                        match row {
                            Ok(row) => {
                                result.push(row);
                            },
                            Err(err) => {
                                result.push(err.to_string());
                            }
                        }
                    }
                }
                Err(err) => {
                    result.push(err.to_string());
                }
            }
        },
        Err(err) => {
            result.push(err.to_string());
        }
    }
    result
}
