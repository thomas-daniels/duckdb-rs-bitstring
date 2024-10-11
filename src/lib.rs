use bit_vec::BitVec;
use duckdb::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, Value, ValueRef};
use std::borrow::Cow;
use std::fmt;

#[derive(Debug, Clone)]
/// Type representing a bitstring that can be converted to a DuckDB BIT type (or the other way around).
/// Under the hood this is just a wrapper for [`bit_vec::BitVec`] with the necessary traits ([`FromSql`]/[`ToSql`]) implemented.
/// Use [`Bitstring::from`] to obtain a [`Bitstring`] from an owned or borrowed [`bit_vec::BitVec`].
pub struct Bitstring<'a>(Cow<'a, BitVec>);

impl<'a> ToSql for Bitstring<'a> {
    fn to_sql(&self) -> duckdb::Result<ToSqlOutput<'_>> {
        if self.as_bitvec().is_empty() {
            Err(duckdb::Error::ToSqlConversionFailure(Box::new(
                BitstringError::EmptyBitstring,
            )))
        } else {
            Ok(ToSqlOutput::Owned(Value::Text(format!("{}", self))))
        }
    }
}

impl fmt::Display for Bitstring<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_bitvec())
    }
}

#[derive(Debug, Clone)]
pub enum BitstringError {
    /// Occurs when trying to convert an empty [`Bitstring`] to a DuckDB BIT type (as that is not supported by DuckDB).
    EmptyBitstring,
    /// Occurs when DuckDB returns an invalid representation of a BIT type.
    /// This should not happen in practice so please let me know if you run into this error.
    RawDataBadPadding(u8),
    /// Occurs when DuckDB returns an invalid representation of a BIT type.
    /// This should not happen in practice so please let me know if you run into this error.
    RawDataTooShort(usize),
}

impl fmt::Display for BitstringError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            BitstringError::RawDataBadPadding(pad) => write!(f, "raw data padding byte should be 0-7, was {pad}"),
            BitstringError::RawDataTooShort(len) => write!(f, "raw data too short (should be at least 2 bytes, was {len} bytes long)"),
            BitstringError::EmptyBitstring => write!(f, "DuckDB does not support empty bit strings, consider using a nullable column and Option<Bitstring>")
        }
    }
}

impl std::error::Error for BitstringError {}

impl<'a> Bitstring<'a> {
    #[must_use]
    pub fn into_bitvec(self) -> BitVec {
        self.0.into_owned()
    }

    #[must_use]
    pub fn as_bitvec(&'a self) -> &'a BitVec {
        self.0.as_ref()
    }

    fn from_raw<'b>(bytes: &[u8]) -> Result<Bitstring<'b>, BitstringError> {
        if bytes.len() < 2 {
            Err(BitstringError::RawDataTooShort(bytes.len()))
        } else if bytes[0] > 7 {
            Err(BitstringError::RawDataBadPadding(bytes[0]))
        } else {
            let mut raw_vec = BitVec::from_bytes(&bytes[1..]);
            if bytes[0] == 0 {
                Ok(Bitstring::from(raw_vec))
            } else {
                Ok(Bitstring::from(raw_vec.split_off(bytes[0].into())))
            }
        }
    }
}

impl From<BitVec> for Bitstring<'_> {
    fn from(v: BitVec) -> Bitstring<'static> {
        Bitstring(Cow::Owned(v))
    }
}

impl<'a> From<&'a BitVec> for Bitstring<'a> {
    fn from(v: &'a BitVec) -> Bitstring<'a> {
        Bitstring(Cow::Borrowed(v))
    }
}

impl FromSql for Bitstring<'_> {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(bytes) => Ok(Bitstring::from_raw(bytes)?),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

impl From<BitstringError> for FromSqlError {
    fn from(value: BitstringError) -> Self {
        FromSqlError::Other(Box::new(value))
    }
}

#[doc = include_str!("../README.md")]
#[cfg(doctest)]
pub struct _ReadmeDoctests;

#[cfg(test)]
mod tests {
    use crate::{Bitstring, BitstringError};
    use bit_vec::BitVec;
    use duckdb::{
        types::{ToSqlOutput, Value},
        ToSql,
    };

    #[test]
    fn from_raw_1() {
        let bytes = vec![0, 0b01100101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "011001011110010100000101");
    }

    #[test]
    fn from_raw_2() {
        let bytes = vec![1, 0b11100101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "11001011110010100000101");
    }

    #[test]
    fn from_raw_3() {
        let bytes = vec![2, 0b11100101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "1001011110010100000101");
    }

    #[test]
    fn from_raw_4() {
        let bytes = vec![3, 0b11100101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "001011110010100000101");
    }

    #[test]
    fn from_raw_5() {
        let bytes = vec![4, 0b11110101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "01011110010100000101");
    }

    #[test]
    fn from_raw_6() {
        let bytes = vec![5, 0b11111101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "1011110010100000101");
    }

    #[test]
    fn from_raw_7() {
        let bytes = vec![6, 0b11111101, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "011110010100000101");
    }

    #[test]
    fn from_raw_8() {
        let bytes = vec![7, 0b11111111, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "11110010100000101");
    }

    #[test]
    fn from_raw_error_1() {
        let bytes = vec![8, 0b11111111, 0b11100101, 0b00000101];
        let bv = Bitstring::from_raw(&bytes);
        assert!(matches!(bv, Err(BitstringError::RawDataBadPadding(8))));
    }

    #[test]
    fn from_raw_error_2() {
        let bytes = vec![7];
        let bv = Bitstring::from_raw(&bytes);
        assert!(matches!(bv, Err(BitstringError::RawDataTooShort(1))));
    }

    #[test]
    fn from_raw_error_3() {
        let bytes = vec![];
        let bv = Bitstring::from_raw(&bytes);
        assert!(matches!(bv, Err(BitstringError::RawDataTooShort(0))));
    }

    #[test]
    fn test_raw_minimal() {
        let bytes = vec![7, 0b11111111];
        let bv = Bitstring::from_raw(&bytes).unwrap().into_bitvec();
        let s = format!("{}", bv);
        assert_eq!(s, "1");
    }

    #[test]
    fn test_tosql() {
        let bv = Bitstring::from(BitVec::from_bytes(&[0b11100101, 0b11100101, 0b00000101]));
        let s = bv.to_sql().unwrap();
        assert_eq!(
            s,
            ToSqlOutput::Owned(Value::Text(String::from("111001011110010100000101")))
        );
    }

    #[test]
    fn test_display() {
        let bv = Bitstring::from(BitVec::from_bytes(&[0b11100101, 0b11100101, 0b00000101]));
        let s = format!("{}", bv);
        assert_eq!(s, "111001011110010100000101");
    }
}
