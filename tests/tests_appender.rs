use bit_vec::BitVec;
use duckdb::{self, params, Connection};
use duckdb_bitstring::Bitstring;

fn setup() -> Connection {
    let db = duckdb::Connection::open_in_memory().unwrap();
    db.execute_batch("create table t1 (id int, d bit);")
        .unwrap();
    db
}

fn get_bitstring(split: usize) -> Bitstring<'static> {
    let mut bv = BitVec::from_bytes(&[0b01101100, 0b10000011, 0b10101011]);
    Bitstring::from(bv.split_off(split))
}

fn check_row(db: &Connection, id: i32, b: &str) {
    let id_returned: i32 = db
        .query_row("select id from t1 where d = ?::bit", [b], |row| {
            row.get::<_, i32>(0)
        })
        .unwrap();
    assert_eq!(id_returned, id);
}

#[test]
fn test_appender() {
    let db = setup();

    {
        let mut appender = db.appender("t1").unwrap();
        appender
            .append_rows([
                params![1, get_bitstring(4)],
                params![2, get_bitstring(5)],
                params![3, get_bitstring(6)],
            ])
            .unwrap();
    } // appender needs to be dropped

    check_row(&db, 1, "11001000001110101011");
    check_row(&db, 2, "1001000001110101011");
    check_row(&db, 3, "001000001110101011");
}
