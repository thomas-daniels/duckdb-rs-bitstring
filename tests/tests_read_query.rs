use duckdb::{self, Connection};
use duckdb_bitstring::Bitstring;

fn setup() -> Connection {
    let db = duckdb::Connection::open_in_memory().unwrap();
    db.execute_batch(
        "create table t1 (id int, d bit);
        insert into t1 values (1, '10110'::bit);
        insert into t1 values (2, '001011010110001011001011010110'::bit);
        insert into t1 values (3, '1111111111'::bit);",
    )
    .unwrap();
    db
}

fn setup_same_lengths() -> Connection {
    let db = duckdb::Connection::open_in_memory().unwrap();
    db.execute_batch(
        "create table t1 (id int, d bit);
        insert into t1 values (1, '10110'::bit);
        insert into t1 values (2, '01111'::bit);
        insert into t1 values (3, '10011'::bit);",
    )
    .unwrap();
    db
}

#[test]
fn test_read() {
    let db = setup();
    let rows: Vec<String> = db
        .prepare("select * from t1 order by id")
        .unwrap()
        .query_and_then([], |row| row.get::<_, Bitstring>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .map(|r| format!("{}", r.as_bitvec()))
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "10110");
    assert_eq!(rows[1], "001011010110001011001011010110");
    assert_eq!(rows[2], "1111111111");
}

#[test]
fn test_read_option() {
    let db = setup();
    db.execute("insert into t1 values (4, NULL)", []).unwrap();
    let rows: Vec<String> = db
        .prepare("select * from t1 order by id")
        .unwrap()
        .query_and_then([], |row| row.get::<_, Option<Bitstring>>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .map(|r| {
            r.map(|v| format!("{}", v.as_bitvec()))
                .unwrap_or("NULL".to_owned())
        })
        .collect();

    assert_eq!(rows.len(), 4);
    assert_eq!(rows[0], "10110");
    assert_eq!(rows[1], "001011010110001011001011010110");
    assert_eq!(rows[2], "1111111111");
    assert_eq!(rows[3], "NULL");
}

#[test]
fn test_read_after_operation() {
    let db = setup_same_lengths();
    let rows: Vec<String> = db
        .prepare("select id, d & '10101'::bit from t1 order by id")
        .unwrap()
        .query_and_then([], |row| row.get::<_, Bitstring>(1))
        .unwrap()
        .map(|r| r.unwrap())
        .map(|r| format!("{}", r.as_bitvec()))
        .collect();

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], "10100");
    assert_eq!(rows[1], "00101");
    assert_eq!(rows[2], "10001");
}
