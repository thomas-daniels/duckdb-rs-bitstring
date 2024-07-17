[![crates.io](https://img.shields.io/crates/v/duckdb-bitstring.svg)](https://crates.io/crates/duckdb-bitstring)
[![docs.rs](https://docs.rs/duckdb-bitstring/badge.svg)](https://docs.rs/duckdb-bitstring)

# duckdb-rs-bitstring

An extension for [duckdb-rs](https://github.com/duckdb/duckdb-rs) providing support for the DuckDB BIT/BITSTRING type. The corresponding Rust type is `BitVec` from the [`bit-vec` crate](https://crates.io/crates/bit-vec). (`duckdb-bitstring` provides the `Bitstring` type as wrapper around `BitVec` in order to make this work.)

## Compatibility table

| `duckdb-bitstring`  | DuckDB        | `bit-vec`     |
| -------------       | ------------- | ------------- |
| 0.3                 | 1.0.X         | 0.6.3         |
| 0.2                 | 0.10.X        | 0.6.3         |

## Querying BITs from DuckDB

Similar to the example in `duckdb-rust` - a `Bitstring` can be obtained from `.get` on a row. A `Bitstring` can be consumed and turned into the underlying `BitVec` using `.into_bitvec()`, or `.as_bitvec()` can be used to get a reference to the underlying `BitVec` without consuming the `Bitstring`.

```rust
use bit_vec::BitVec;
use duckdb::{Connection, Result};
use duckdb_bitstring::Bitstring;

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        "CREATE TABLE t1 (d BIT);
        INSERT INTO t1 VALUES ('10110'::BIT);
        INSERT INTO t1 VALUES ('01101100010101101'::BIT);
        INSERT INTO t1 VALUES ('11111111111'::BIT);"
    )?;

    let bitvecs: Vec<Result<BitVec>> = conn.prepare("SELECT d FROM t1")?.query_map([], |row| {
        let value: Bitstring = row.get(0)?;
        Ok(value.into_bitvec())
    })?.collect();

    for bv in bitvecs {
        println!("{:?}", bv?);
    }

    // 10110
    // 01101100010101101
    // 11111111111

    Ok(())
}
```

## Providing a Bitstring as query parameter

Use `Bitstring::from(...)` to wrap an owned or borrowed `BitVec` into a `Bitstring`. The `Bitstring` can then be passed as SQL parameter as usual in `duckdb-rs`. In the SQL query, it's still recommended to cast the parameter to BIT using `::BIT`. That is because the `BitVec` gets necessarily converted to a string under the hood, and while DuckDB will be able to automatically recognize it as bitstring in many cases, it won't in all cases - hence the explicit cast.

```rust
use bit_vec::BitVec;
use duckdb::{params, Connection, Result};
use duckdb_bitstring::Bitstring;

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE t1 (id INT, d BIT);",
        [],
    )?;

    let bv = BitVec::from_iter(vec![true, true, false, false, true]);
    let bs = Bitstring::from(bv);

    conn.execute(
        "INSERT INTO t1 VALUES (?, ?::BIT)",
        params![1, bs],
    )?;


    Ok(())
}
```

Important: a `Bitstring` (or rather its inner `BitVec`) can be empty (i.e. length of zero bits), but empty BITs are not supported in DuckDB. `duckdb-bitstring` will error if you try to pass an empty `Bitstring` as query parameter.

## Usage in Appender

`Bitstring`s can also be used as parameter for an `Appender`:

```rust
use bit_vec::BitVec;
use duckdb::{params, Connection, Result};
use duckdb_bitstring::Bitstring;

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute(
        "CREATE TABLE t1 (id INT, d BIT);",
        [],
    )?;

    let mut appender = conn.appender("t1")?;

    let bv = BitVec::from_iter(vec![true, true, false, false, true]);
    let bs = Bitstring::from(bv);

    appender.append_row(params![1, bs])?;

    // note: if you want to try querying the `t1` table now,
    // you need to drop `appender` first!

    Ok(())
}
```