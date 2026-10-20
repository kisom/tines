# tines-bt

The BLE transport for tines: a [`Transport`] implementation whose address is
the peer's advertised [`NodeId`], so identity survives BLE address rotation.

Real radio I/O sits behind the small [`Radio`] trait (implemented over
`bluer`/BlueZ by [`BluerRadio`]); everything else — the `from`/`Addr` check, the
central/peripheral role decision, and the address→`NodeId` binding — is pure and
unit-testable without hardware.

## Framing

Connections are `bluer` [`l2cap::SeqPacket`] channels (LE L2CAP
Connection-Capable Channel): each received SDU is exactly one frame. The MTU is
clamped to [`MAX_FRAME`], so a single SDU is a single wire envelope — no
length-prefix framing is needed.

## Roles

Every node advertises **and** scans. To keep two nodes from both dialing, the
numerically-smaller [`NodeId`] acts as the BLE central (initiator) and the
larger accepts on the [`SeqPacketListener`] bound to PSM `0x008D`. The listener
runs for the whole session.

## Binding

The BLE address is only a routing key (it rotates), so a connection is bound to
the peer's advertised [`NodeId`] instead:

- the scanner decodes the peer's [`NodeId`] from a tines advertisement — the
  `Service Data — 16-bit UUID` record must carry the tines service UUID
  (`0xFE7E`), which is checked by [`adv::decode_ad_node_id`];
- the decoded `bt address → NodeId` pair is recorded in the scan map;
- an **inbound** connection is bound by looking its peer address up in that map
  — an inbound from an unknown address is rejected immediately;
- an **outbound** connection dials using the discovered address *type*, not a
  hard-coded one.

A frame's declared sender is then checked against the bound [`NodeId`]: the
transport peeks the `from` field with [`wire::frame_sender`] and drops any frame
whose declared sender differs from the bound peer (an address-spoofing
violation).

## Lifecycle

- connection up → `Event::Contact(addr)`;
- connection end (EOF, error, or adapter loss) → `Event::Lost(addr)`;
- scanning and listening continue after a loss.

## Multi-peer

Up to [`MAX_PEERS`] connections may be live at once, one per bound [`NodeId`];
a second connection to the same [`NodeId`] replaces the first.

## Manual verification

The SeqPacket framing can't be exercised in CI (it needs two real radios), so
verify it by hand with two devices running the `tines` service:

1. start `tines` on both devices with distinct `NodeId`s;
2. confirm the smaller-`NodeId` device dials and the larger accepts (check the
   `tines-bt: connected to … (central)` / `accepted connection from … (bound)`
   trace lines);
3. send a burst of observations on one device and confirm the other receives
   each as a separate, intact frame (no merged or truncated frames).

[`Transport`]: https://docs.rs/tines-core/latest/tines_core/transport/trait.Transport.html
[`NodeId`]: https://docs.rs/tines-core/latest/tines_core/observation/struct.NodeId.html
[`Radio`]: crate::Radio
[`BluerRadio`]: crate::BluerRadio
[`adv::decode_ad_node_id`]: crate::adv::decode_ad_node_id
[`wire::frame_sender`]: https://docs.rs/tines-core/latest/tines_core/wire/fn.frame_sender.html
[`MAX_FRAME`]: https://docs.rs/tines-core/latest/tines_core/consts/constant.MAX_FRAME.html
[`MAX_PEERS`]: https://docs.rs/tines-core/latest/tines_core/consts/constant.MAX_PEERS.html
[`l2cap::SeqPacket`]: https://docs.rs/bluer/latest/bluer/l2cap/struct.SeqPacket.html
[`SeqPacketListener`]: https://docs.rs/bluer/latest/bluer/l2cap/struct.SeqPacketListener.html
