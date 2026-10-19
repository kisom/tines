# tines host hardening — before a robot leaves the lab

Date: 2026-10-19
Status: draft v1. Not yet reviewed.
Fixes: findings F10–F12 in `THREAT_MODEL.md` (assessed at `3b1a644`)
Depends on: `2026-10-19-tines-fleet-fixes.md`, which adds the poll loop,
the trim/flush hooks and the log format this spec builds on

## Intent

The hosts that run tines on a robot (`tines`, `tines-ros2`, and the
`tines-bt` and `tines-store` crates under them) currently trust three
things they shouldn't:
- that storage never fills,
- that every local process is friendly,
- that every participant on the ROS2 domain is friendly.

This spec bounds every resource a peer, a local process or a ROS2
publisher can grow, and limits who can act as the robot. It's the bar a
robot must meet before it operates anywhere other than a trusted lab.

## Why separately from 1.0

Everything here is in host crates, which stay 0.x with their own versions
(1.0 spec, D4). None of it changes the wire or core's API. Tying it to the
core 1.0 release would make one wait on the other for no benefit. The bar
is deployment readiness, not a version number.

## Goals

1. **Nothing grows without bound** (F10): store size, log size, channels,
   and per-request allocations.
2. **Only authorised local users can act as the robot** through the
   service's IPC (F11), and no client can block the others.
3. **The ROS2 bridge doesn't feed on itself** (F12): it can be restricted
   to a trusted DDS scope, can run read-only, and rate-limits what it
   accepts.
4. **Hosts run with least privilege**, under a provided systemd unit.

## Non-goals

- Authenticating peers or signing observations (1.1). These limits make
  floods *bounded*; they don't make them *impossible*.
- Confidentiality of the radio link (threat model F15).
- Core changes. Per-peer behaviour in the exchange (eviction, summary rate
  limits) is in the 1.0 spec §10.

## Constraints

- No changes to tines-core's API or wire format.
- Defaults must be safe without configuration: a robot that gets an
  untuned config must still be bounded.
- Every limit is configurable, has a documented default, and is reported
  (counter or log line) when it bites. A silent limit looks like a bug.

## The how

### 1. F10 — bounded growth

1. **Default trim policy.** Both hosts call `trim` on the poll loop's
   schedule (fleet fixes §2), every `trim_interval_ms` (default 10 s), with
   a config `TrimPolicy` whose defaults are always on:
   - `max_count`: default 50,000 observations (about 50 MB worst case at
     the 1 KiB payload cap; size it per robot).
   - `max_age_ms`: default 7 days.
   - `keep_per_topic`: unset by default.

   The fleet spec's sizing guidance applies: custody traffic must outlive
   its lifetime. The host docs say how to size `max_count` for that.
2. **Store full stops being silent.** `LogStore` gains an optional hard cap
   (`max_records`, defaulting to 2 × `max_count`). Above it, `insert`
   returns `StoreError::Full`. With the fleet fix for F1, a full store no
   longer acks, so custody stays with the sender. That's the safe
   direction, and it's the point of having a cap.
3. **Log compaction.** When dead records (trimmed or superseded) exceed
   `compact_ratio` of the file (default 0.5), and at most once per
   `compact_interval_ms`, `LogStore` writes the live records and current
   release flags to a new file. It fsyncs the file, renames it over the
   old one, and fsyncs the directory. A crash during compaction leaves the
   old log intact. Releases written as records (fleet fixes §4) are
   carried over.
4. **Bounded channels.**
   - tines-bt inbound: a bounded `mpsc` of `BT_INBOUND_CAP` frames (default
     256). When full, the oldest is dropped and counted. A frame is
     droppable; the protocol tolerates loss.
   - tines-bt outbound: a bounded channel. When full, `send` returns
     `SendError::Busy`, which core already retries via `on_send_error`.
   - tines-ros2 inbound: bounded (default 256), dropping the oldest with a
     counter.
5. **Per-request allocation.** The IPC frame cap drops from 1 MiB to
   16 KiB. The largest valid request is a hex-encoded 64-byte topic plus a
   1 KiB payload in JSON, about 2.3 KiB.
6. **Load.** `LogStore::open` reads the log in a stream instead of
   `fs::read` of the whole file, so memory use at open is bounded by the
   index, not the file size.

### 2. F11 — local IPC

1. **Socket location and mode.**
   - Default path `$RUNTIME_DIRECTORY/tines.sock`, provided by systemd's
     `RuntimeDirectory=tines` with mode `0750`.
   - The socket is created `0660`, owned by the service user and a
     configurable `ipc_group` (default `tines`).
   - Only members of that group can connect.
2. **Peer credentials.** On accept, read `SO_PEERCRED`. Allow if the uid is
   the service uid, or the gid or any supplementary group is `ipc_group`,
   or the uid is in the config `ipc_allow_uids`. Anything else is closed
   with a logged refusal. This is defence in depth on top of the file
   mode.
3. **Read-only clients.** Config `ipc_readonly_uids` and
   `ipc_readonly_gid` get `Belief`, `Status` and `Events` only. `Observe`
   requires the full grant. Most consumers only read, and shouldn't be
   able to speak as the robot.
4. **Don't unlink arbitrary paths.** Remove the configured path only if
   it's a socket (`symlink_metadata` → file type socket). Otherwise fail
   with an error.
5. **Concurrency and timeouts.**
   - Serve each client on its own thread, up to `ipc_max_clients`
     (default 8); beyond that, accept and close.
   - Each connection has read and write timeouts (`ipc_timeout_ms`,
     default 5 s).
   - An idle client is closed. No client can block another.
6. **Framing errors close the connection.** An oversized length is
   rejected and the connection is closed, so the stream can't be left
   desynchronised. Today it returns an error and keeps reading.
7. **`accept` errors** (for example `EMFILE`) are logged and retried with
   backoff; they no longer end the daemon.
8. **Events.** `Events` stays a destructive drain (one consumer), and the
   docs say so. Fan-out to several consumers is a later feature if it's
   needed.

### 3. F12 — the ROS2 bridge

1. **No self-echo.** The bridge must not re-observe its own publications.
   - Prefer the middleware option that ignores local publications, if
     rclrs exposes it.
   - Otherwise, filter in the callback by publisher GID if available, or
     else drop an inbound message whose payload equals the last payload
     the bridge published on that topic within `echo_window_ms` (default
     1 s).
   - **Test:** publish once, poll several times; the store gains exactly
     one observation.
2. **Read-only mode.** Config `bridge_inbound` (default `false`). When
   false, the bridge creates publishers only. ROS2 consumers can read
   tines state, but nothing on the domain can make the robot say anything.
   Enabling inbound is an explicit decision per deployment.
3. **Rate limits.** When inbound is enabled, a per-topic token bucket
   applies (`inbound_rate_per_s`, default 2; burst 4). Excess messages are
   dropped and counted. A bridged topic should carry facts, not telemetry
   streams.
4. **DDS scope.**
   - The docs and the sample unit set a dedicated `ROS_DOMAIN_ID` and
     `ROS_AUTOMATIC_DISCOVERY_RANGE=LOCALHOST` unless the deployment needs
     the ROS graph off-box.
   - For off-box graphs, the docs describe enabling SROS2: an enclave for
     the `tines` node, with governance that allows only listed
     participants to publish on `/tines/**`.
   - This spec documents SROS2 setup; it doesn't automate it.
5. **Receipts topic.** `/tines/receipts` reveals peer NodeIds and
   observation ids. It stays on, but can be disabled
   (`publish_receipts`, default `true`) for deployments that bridge
   off-box without SROS2.

### 4. Least privilege and the systemd unit

Provide `packaging/systemd/tines.service` (standalone) and a ROS2 launch
note. The unit:

```ini
[Service]
User=tines
Group=tines
SupplementaryGroups=bluetooth
StateDirectory=tines          # log lives here, 0700
RuntimeDirectory=tines        # socket lives here
RuntimeDirectoryMode=0750
Restart=on-failure
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
RestrictAddressFamilies=AF_UNIX AF_BLUETOOTH
CapabilityBoundingSet=
```

- The service never runs as root.
- BlueZ access goes through D-Bus policy for the `bluetooth` group. The
  docs list the bluetoothd configuration the target images need.
- A panic exits non-zero, and `Restart=on-failure` brings the service back
  (fleet fixes §2).

## Acceptance

- Each limit has a test that drives it past its cap: the counter or log
  fires, and memory or disk stays bounded:
  - trim,
  - the store cap,
  - compaction (including a crash mid-compaction),
  - both BT channels,
  - the ROS2 inbound channel and rate limit,
  - the IPC frame cap.
- **IPC tests:**
  - wrong uid refused;
  - read-only uid can't `Observe`;
  - an idle client doesn't block a second client;
  - an oversized frame closes the connection;
  - a non-socket path isn't unlinked.
- **ROS2 tests:** the self-echo test, and that read-only mode creates no
  subscriptions. These run in the existing ROS2 Docker job (`Dockerfile.ros2`).
- The systemd unit passes `systemd-analyze verify` and runs the service on
  the Pi image.
- `THREAT_MODEL.md` marks F10–F12 mitigated and updates §5's
  "Bounded memory" row.

## Left to the builder

- Exact defaults where marked; they get tuned once real robots are
  measured.
- Whether rclrs exposes local-publication filtering (and so which echo
  filter is used).
- Compaction triggers beyond the ratio, for example on open.

## Sequencing

Follows the fleet fixes. Within this spec, the order is:
1. channels and the IPC cap (small, independent);
2. trim defaults and the store cap;
3. compaction;
4. IPC permissions, credentials and concurrency;
5. ROS2;
6. the systemd unit.
