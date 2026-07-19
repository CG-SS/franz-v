// Compression codecs for record batches. The enum values equal the Kafka
// attribute bits (0-2) for each codec.
module krec

import compress.gzip
import compress.snappy
import compress.zstd

// Codec selects the compression for produced record batches.
pub enum Codec {
	uncompressed = 0
	gzip         = 1
	snappy       = 2
	lz4          = 3
	zstd         = 4
}

// UnsupportedCodecError is returned for codecs franz-v cannot produce.
pub struct UnsupportedCodecError {
	Error
pub:
	codec Codec
}

// msg implements IError.
pub fn (e UnsupportedCodecError) msg() string {
	return 'codec ${e.codec} is not supported: vlib has no interoperable LZ4 frame implementation'
}

// Why lz4 is rejected rather than wired to vlib's compress.lz — verified
// empirically against Apache Kafka 4.3.1: vlib's compress_lz4 emits a
// custom container with magic bytes 'VLZ1' (56 4c 5a 31), not the LZ4
// frame magic (04 22 4d 18). A batch produced with it round-trips locally
// but the broker rejects it during log validation with
// `Lz4Compression.wrapForInput: java.io.IOException: Stream unsupported
// (invalid magic bytes)`, surfaced to the client as UNKNOWN_SERVER_ERROR.
// Supporting lz4 requires a real LZ4 frame codec (vlib or in-project).

// compress_payload compresses a record-batch payload with the codec.
fn compress_payload(codec Codec, payload []u8) ![]u8 {
	return match codec {
		.uncompressed {
			payload
		}
		.gzip {
			gzip.compress(payload)!
		}
		.snappy {
			snappy.compress(payload)
		}
		.lz4 {
			UnsupportedCodecError{
				codec: .lz4
			}
		}
		.zstd {
			zstd.compress(payload)!
		}
	}
}

// decompress_payload reverses compress_payload; used by the consumer path
// and by kfake to verify produced batches.
fn decompress_payload(codec Codec, payload []u8) ![]u8 {
	return match codec {
		.uncompressed {
			payload
		}
		.gzip {
			gzip.decompress(payload)!
		}
		.snappy {
			snappy.decompress(payload)!
		}
		.lz4 {
			UnsupportedCodecError{
				codec: .lz4
			}
		}
		.zstd {
			zstd.decompress(payload)!
		}
	}
}
