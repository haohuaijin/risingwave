use std::io::Write;

use bytes::{Bytes, BytesMut};
use chrono::{TimeZone, Utc};
use postgres_types::ToSql;
use serde::{Deserialize, Serialize};

use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::DataType;
use crate::array::ArrayResult;
use crate::error::Result;

#[derive(
    Default,
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    parse_display::Display,
    Serialize,
    Deserialize,
)]
#[repr(transparent)]
pub struct Timestamptz(pub i64);

impl ToBinary for Timestamptz {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        assert!(matches!(ty, DataType::Timestamptz));
        let secs = self.0.div_euclid(1_000_000);
        let nsecs = self.0.rem_euclid(1_000_000) * 1000;
        let instant = Utc.timestamp_opt(secs, nsecs as u32).unwrap();
        let mut out = BytesMut::new();
        // postgres_types::Type::ANY is only used as a placeholder.
        instant
            .to_sql(&postgres_types::Type::ANY, &mut out)
            .unwrap();
        Ok(Some(out.freeze()))
    }
}

impl ToText for Timestamptz {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        // Just a meaningful representation as placeholder. The real implementation depends
        // on TimeZone from session. See #3552.
        let secs = self.0.div_euclid(1_000_000);
        let nsecs = self.0.rem_euclid(1_000_000) * 1000;
        let instant = Utc.timestamp_opt(secs, nsecs as u32).unwrap();
        // PostgreSQL uses a space rather than `T` to separate the date and time.
        // https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-DATETIME-OUTPUT
        // same as `instant.format("%Y-%m-%d %H:%M:%S%.f%:z")` but faster
        write!(f, "{}+00:00", instant.naive_local())
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Timestamptz => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl Timestamptz {
    pub const MIN: Self = Self(i64::MIN);

    pub fn from_protobuf(timestamp_micros: i64) -> ArrayResult<Self> {
        Ok(Self(timestamp_micros))
    }

    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        output.write(&self.0.to_be_bytes()).map_err(Into::into)
    }
}