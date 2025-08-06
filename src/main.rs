use duckdb::{Connection};
use duckdb::types::ValueRef;
use axum::{
    routing::{post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Serialize)]
struct ResponseData {
    result: Vec<String>,
}

#[derive(Deserialize)]
struct InputData {
    query: String,
}

#[tokio::main]
async fn main() {

    let app = Router::new()
        .route("/query", post(query));

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

fn query_db(statement: String, connection: Connection) -> Vec<String> {
    let mut result = Vec::new();
    let stmt = connection.prepare(statement.as_str());
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
                            if row_string.ends_with(", ") {
                                row_string.truncate(row_string.len() - 2);
                            }
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

async fn query(Json(payload): Json<InputData>) -> Json<ResponseData> {
    match get_db_connection() {
        Ok(conn) => {
            let result = query_db(payload.query, conn);
            Json(ResponseData { result })
        }
        Err(err) => Json(ResponseData { result: vec![err.to_string()] }),
    }
}

fn get_db_connection<'a>() -> Result<Connection, duckdb::Error> {
    Connection::open("./database.db3")
}