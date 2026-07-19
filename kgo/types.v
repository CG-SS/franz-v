// Re-exports of the protocol-level record types from krec, so client
// users need only `import kgo`.
module kgo

import krec

// Record is a producible/consumable Kafka record; see krec.Record.
pub type Record = krec.Record

// RecordHeader is one record header; see krec.RecordHeader.
pub type RecordHeader = krec.RecordHeader

// Codec selects record-batch compression; see krec.Codec.
pub type Codec = krec.Codec
