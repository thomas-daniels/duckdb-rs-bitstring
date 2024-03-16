use bit_vec::BitVec;
use duckdb::{self, params, Connection};
use duckdb_bitstring::{Bitstring, BitstringError};

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

fn get_bitvec() -> BitVec {
    let mut bv = BitVec::from_bytes(&[0b01101100, 0b10000011, 0b10101011]);
    bv.split_off(4)
}

fn check_new_row(db: Connection) {
    let id: i32 = db
        .query_row(
            "select id from t1 where d = '11001000001110101011'::bit",
            [],
            |row| row.get::<_, i32>(0),
        )
        .unwrap();
    assert_eq!(id, 4);
}

#[test]
fn test_param_owned() {
    let db = setup();
    let owned_bv = get_bitvec();

    db.execute(
        "insert into t1 values (?, ?::bit)",
        params![4, Bitstring::from(owned_bv)],
    )
    .unwrap();

    check_new_row(db);
}

#[test]
fn test_param_ref() {
    let db = setup();
    let bv = get_bitvec();

    test_param_ref_inner(&bv, db);
}

fn test_param_ref_inner(bv: &BitVec, db: Connection) {
    db.execute(
        "insert into t1 values (?, ?::bit)",
        params![4, Bitstring::from(bv)],
    )
    .unwrap();

    check_new_row(db);
}

#[test]
fn test_param_ref2() {
    let db = setup();
    let bv = get_bitvec();

    test_param_ref2_inner1(&bv, db);
}

fn test_param_ref2_inner1(bv: &BitVec, db: Connection) {
    let bs = Bitstring::from(bv);
    test_param_ref2_inner2(&bs, db);
}

fn test_param_ref2_inner2(bs: &Bitstring, db: Connection) {
    db.execute("insert into t1 values (?, ?::bit)", params![4, bs])
        .unwrap();

    check_new_row(db);
}

#[test]
fn test_param_option() {
    let db = setup();
    let bs = Some(Bitstring::from(get_bitvec()));

    db.execute("insert into t1 values (?, ?::bit)", params![4, bs])
        .unwrap();

    check_new_row(db);
}

#[test]
fn test_param_option_none() {
    let db = setup();
    let bs: Option<Bitstring> = None;

    db.execute("insert into t1 values (?, ?::bit)", params![4, bs])
        .unwrap();

    let id: i32 = db
        .query_row("select id from t1 where d is null", [], |row| {
            row.get::<_, i32>(0)
        })
        .unwrap();
    assert_eq!(id, 4);
}

#[test]
fn test_error_empty() {
    let db = setup();
    let bv = BitVec::new();

    let result = db.execute(
        "insert into t1 values (?, ?::bit)",
        params![4, Bitstring::from(bv)],
    );
    assert!(matches!(
        result,
        Err(duckdb::Error::ToSqlConversionFailure(_))
    ));

    if let Err(duckdb::Error::ToSqlConversionFailure(err)) = result {
        matches!(err.downcast_ref().unwrap(), BitstringError::EmptyBitstring);
    } else {
        unreachable!(); // The assert! before the if branch already confirmed it matches.
    }
}
