//! BLE advertising payload assembly for tines-bt.
//!
//! The advertised payload carries just enough for a peer to (a) recognise a
//! tines device and (b) learn our identity *without* opening a connection: a
//! standard LE flags record followed by a "Service Data — 16-bit UUID" record
//! that embeds a private 16-bit UUID and our 16-byte [`NodeId`].
//!
//! A 16-bit UUID fits the legacy 31-byte extended-in-data (EIR) advertising
//! limit; a 128-bit UUID would not (39 bytes). Decoding the [`NodeId`] back out
//! of the service-data record is exactly what [`decode_node_id`] does — the
//! invariant the round-trip test pins.

use tines_core::observation::NodeId;

/// The LE flags byte carried in the flags AD record: LE General Available and
/// BR/EDR not supported (`0x06`).
pub const ADV_FLAGS: u8 = 0x06;

/// AD type: Flags (`0x01`).
const AD_TYPE_FLAGS: u8 = 0x01;

/// AD type: Service Data — 16-bit UUID (`0x16`).
const AD_TYPE_SERVICE_DATA_16: u8 = 0x16;

/// Private-use 16-bit service UUID advertised in the service-data record.
///
/// Not a SIG-assigned UUID; any private value works because tines peers agree
/// on it out of band. Chosen to avoid common Nordic/nRF service UUIDs.
pub const SERVICE_UUID: u16 = 0xFE7E;

/// Number of bytes a tines advertising buffer holds.
///
/// Matches the legacy BLE extended-in-data maximum so this crate advertises
/// within the 31-byte legacy advertising limit rather than requiring
/// LE Advertising (V2) extended advertising.
pub const ADVERTISING_BUFFER_LEN: usize = 31;

/// Length, in bytes, of a [`NodeId`].
pub const NODE_ID_LEN: usize = 16;

/// Assemble the legacy 31-byte BLE advertising buffer that advertises `node`
/// (this device's [`NodeId`]).
///
/// The buffer is the standard EIR layout:
///
/// ```text
/// [0x02 0x01 0x06]                              flags: LE available, no BR/EDR
/// [0x13 0x16 <uuid_le> <node[16>              service data, 16-bit UUID + NodeId
/// <zero padding to 31 bytes>
/// ```
///
/// The meaningful bytes occupy the first 23 bytes; the remainder is zero
/// padding. The returned buffer is always exactly
/// [`ADVERTISING_BUFFER_LEN`] bytes and always fits the legacy advertising
/// limit.
pub fn advertise(node: NodeId) -> [u8; ADVERTISING_BUFFER_LEN] {
    let mut buf = [0u8; ADVERTISING_BUFFER_LEN];

    // Flags AD record: length (2 bytes follow), type (0x01), data (0x06).
    buf[0] = 2;
    buf[1] = AD_TYPE_FLAGS;
    buf[2] = ADV_FLAGS;

    // Service Data — 16-bit UUID record. The payload after the length byte is
    // type (0x16) + 2-byte UUID + 16-byte NodeId = 19 bytes; the length byte is
    // the count of the bytes that follow it.
    let uuid_le = SERVICE_UUID.to_le_bytes();
    buf[3] = 1 + 2 + NODE_ID_LEN as u8; // 19
    buf[4] = AD_TYPE_SERVICE_DATA_16; // 0x16
    buf[5] = uuid_le[0];
    buf[6] = uuid_le[1];
    buf[7..7 + NODE_ID_LEN].copy_from_slice(&node);

    buf
}

/// Decode the [`NodeId`] advertised in a raw EIR buffer.
///
/// Walks the AD records looking for a "Service Data — 16-bit UUID" record
/// (`0x16`), skips its 2-byte UUID, and returns the following 16 bytes as the
/// [`NodeId`]. Returns `None` if no such record is present or it is malformed.
///
/// This is generic over the total buffer length — it works whether the
/// meaningful data is 23 bytes or any other length `<= ADVERTISING_BUFFER_LEN`
/// — which is what makes the round-trip test robust to the exact padding.
///
/// [`NodeId`]: tines_core::observation::NodeId
pub fn decode_node_id(payload: &[u8]) -> Option<NodeId> {
    let mut i = 0;
    while i < payload.len() {
        let len = payload[i] as usize;
        if len == 0 {
            break;
        }
        // record = [len][type][data...len]; guard against overrun.
        let end = i.checked_add(1 + len)?;
        if end > payload.len() {
            break;
        }
        let ad_type = payload[i + 1];
        let data = &payload[i + 2..i + 1 + len];
        if ad_type == AD_TYPE_SERVICE_DATA_16 && data.len() >= 2 + NODE_ID_LEN {
            let mut node = [0u8; NODE_ID_LEN];
            node.copy_from_slice(&data[2..2 + NODE_ID_LEN]);
            return Some(node);
        }
        i = end;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advertises_flags_then_service_data() {
        let p = advertise([3u8; 16]);
        assert_eq!(p.len(), ADVERTISING_BUFFER_LEN);
        assert_eq!((p[0], p[1], p[2]), (2, AD_TYPE_FLAGS, ADV_FLAGS));
        assert_eq!(
            (p[3], p[4]),
            (1 + 2 + NODE_ID_LEN as u8, AD_TYPE_SERVICE_DATA_16)
        );
    }

    #[test]
    fn meaningful_bytes_fit_legacy_limit() {
        let p = advertise([9u8; 16]);
        let last = p.iter().rposition(|&b| b != 0).expect("payload has data");
        assert_eq!(last, 22);
        assert_eq!(&p[23..], &[0u8; 8]);
    }

    #[test]
    fn service_uuid_is_little_endian() {
        let p = advertise([0u8; 16]);
        let uuid_le = [p[5], p[6]];
        assert_eq!(u16::from_le_bytes(uuid_le), SERVICE_UUID);
    }

    #[test]
    fn roundtrips_node_id() {
        let node: NodeId = [
            0xde, 0xad, 0xbe, 0xef, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99,
            0xaa, 0xbb,
        ];
        assert_eq!(decode_node_id(&advertise(node)), Some(node));
    }

    #[test]
    fn missing_service_data_returns_none() {
        let mut payload = [0u8; ADVERTISING_BUFFER_LEN];
        // A lone flags record, no service-data record.
        payload[0] = 2;
        payload[1] = AD_TYPE_FLAGS;
        payload[2] = ADV_FLAGS;
        assert_eq!(decode_node_id(&payload), None);
    }

    #[test]
    fn node_id_zero_is_roundtripped() {
        // All-zero node id must still round-trip (must not be read as empty).
        assert_eq!(decode_node_id(&advertise([0u8; 16])), Some([0u8; 16]));
    }
}
