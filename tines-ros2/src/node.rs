//! rclrs-backed ROS2 node for tines (feature `ros2`).
//!
//! [`TinesNode`] is a self-contained ROS2 node that owns a tines
//! [`tines_core::Node`] over a [`tines_store::LogStore`] and a [`tines_bt::BtTransport`].
//! It bridges the two stacks in both directions:
//!
//! * **tines → ROS2**: each distinct live tines topic is published to its
//!   hex-escaped ROS2 topic (see [`crate::topics::topic_name`]) carrying the
//!   raw observation payload as a `UInt8MultiArray` (`data: Vec<u8>`), and every
//!   [`tines_core::ConfidenceEvent`] the exchange emits is published to
//!   [`RECEIPTS_TOPIC`] as a 32-byte `UInt8MultiArray` (`peer[..16] ++
//!   id[..16]`).
//! * **ROS2 → tines**: inbound messages on a topic are observed back into the
//!   tines node under the original (decoded) topic, the payload being the
//!   `UInt8MultiArray.data` bytes verbatim.
//!
//! Topic creation is lazy: a ROS2 publisher/subscriber pair is created the
//! first time a topic is seen; the total number of managed topics is bounded by
//! [`TinesNode::new`]'s `topic_cap` and by the number of topics actually live
//! in the store.
//!
//! This module is compiled only with `--features ros2`; it requires a ROS2
//! toolchain (and `rclrs`) to build. The default, ROS2-free build of the crate
//! never touches this code, so the crate is testable without ROS2 installed.
//!
//! The rclrs calls below follow the rclrs 0.8 API:
//! * the global ROS2 context is created via [`rclrs::Context::default_from_env`],
//!   then an [`rclrs::Executor`] is created from it and the node is created with
//!   [`rclrs::NodeState::create_node`];
//! * publishers are created with [`rclrs::NodeState::create_publisher`] and
//!   subscriptions with [`rclrs::NodeState::create_subscription`], whose
//!   callback argument's type is inferred (no generic, no QoS arg); the topic
//!   is passed as the options argument, which defaults to `QOS_PROFILE_DEFAULT`.
//! * messages live in the `ros_env` crate (see the `ros_env` dependency), not
//!   under `rclrs`.
//!
//! The `Context`/`Executor` are created per [`TinesNode::new`]; the node is
//! retained by [`TinesNode`] and owns the shared context, so no caller-side
//! [`rclrs::init`] is required (the pre-0.8 `rclrs::init` global is gone).

use std::collections::{BTreeSet, HashMap};
use std::sync::mpsc::channel;

use rclrs::{Context, CreateBasicExecutor, Node as RclNode, Publisher, RclrsError, Subscription};
use ros_env::std_msgs::msg::{MultiArrayLayout, UInt8MultiArray};

use tines_bt::{BtTransport, Radio};
use tines_core::{Node as TinesNodeCore, NodeId, Observation, ObservationId, Store, Tick};
use tines_store::LogStore;

use crate::topics::topic_name;

/// Inbound (topic bytes, payload) delivered from a subscription callback on the
/// rclrs dispatch thread to the node's single owner task.
type InboundMsg = (Vec<u8>, Vec<u8>);

/// ROS2 topic carrying confidence events, the literal `/tines/receipts`
/// (hex-escaping is a no-op here since the bytes are already valid ROS2 name
/// characters). This is a `&str` because ROS2 publisher/subscription creation
/// takes a `&str` topic name, unlike the arbitrary-byte [`crate::RECEIPTS_TOPIC`].
const RECEIPTS_TOPIC: &str = "/tines/receipts";

/// A managed ROS2 topic: the tines bytes it maps back to, its publisher, its
/// subscription, and the id of the observation last published (to avoid
/// re-publishing an unchanged value every poll).
struct ManagedTopic {
    tines_topic: Vec<u8>,
    publisher: Option<Publisher<UInt8MultiArray>>,
    // The subscription handle must stay alive for the node's lifetime: rclrs 0.8
    // `Subscription` owns the underlying rcl handle, so dropping it would tear the
    // subscription down. We never read its fields, hence the allow.
    #[allow(dead_code)]
    subscriber: Option<Subscription<UInt8MultiArray>>,
    last_pub: Option<ObservationId>,
}

/// Errors constructing a [`TinesNode`].
#[derive(Debug)]
pub enum TinesNodeError {
    /// The underlying rclrs call failed (ROS2 context, topic creation).
    Rcl(RclrsError),
    /// The [`LogStore`] backend rejected the open/initialization.
    Store(tines_core::StoreError),
}

impl From<RclrsError> for TinesNodeError {
    fn from(err: RclrsError) -> Self {
        TinesNodeError::Rcl(err)
    }
}

impl From<tines_core::StoreError> for TinesNodeError {
    fn from(err: tines_core::StoreError) -> Self {
        TinesNodeError::Store(err)
    }
}

/// A self-contained ROS2 node bridging tines and ROS2.
///
/// Construct with [`TinesNode::new`], then drive it from your run loop with
/// [`TinesNode::poll`]. Inbound ROS2 observations arrive on an internal
/// channel and are consumed on the next [`TinesNode::poll`], so the node is not
/// thread-safe by itself — drive it from a single task.
pub struct TinesNode {
    node: RclNode,
    tines: TinesNodeCore<LogStore, BtTransport>,
    /// ROS2 topics currently managed (ROS2 topic name → bookkeeping).
    managed: HashMap<String, ManagedTopic>,
    /// Receipt publisher for confidence events.
    receipt_pub: Publisher<UInt8MultiArray>,
    /// Channel from subscription callbacks back to this node.
    inbound_rx: std::sync::mpsc::Receiver<InboundMsg>,
    /// Cloned per subscription so callbacks can route inbound bytes.
    inbound_tx: std::sync::mpsc::Sender<InboundMsg>,
    /// Upper bound on the number of managed ROS2 topics.
    topic_cap: usize,
}

impl TinesNode {
    /// Initialize a node for local id `me` over a [`LogStore`] and a
    /// [`BtTransport`].
    ///
    /// * `log_path` — where the backing [`LogStore`] lives (created if missing).
    /// * `peer` / `radio` — the BLE connection this node exchanges over.
    /// * `first_seq` — the next tines observation sequence to use.
    /// * `topic_cap` — the maximum number of ROS2 topics to manage at once.
    ///
    /// The ROS2 context is created internally via [`rclrs::Context::default_from_env`];
    /// the store's anchor tick is `0`; ages then advance under the `Tick`
    /// values passed to [`TinesNode::poll`].
    pub fn new(
        me: NodeId,
        first_seq: u64,
        log_path: &std::path::Path,
        peer: NodeId,
        radio: impl Radio + 'static,
        topic_cap: usize,
    ) -> Result<Self, TinesNodeError> {
        // rclrs 0.8: create the global context, then a basic executor, then the
        // node through that executor (the pre-0.8 `rclrs::init` +
        // `Node::new(name, ns)` shape is gone). The node owns the shared
        // context, so no caller-side init is required.
        let context = Context::default_from_env()?;
        let executor = context.create_basic_executor();
        let node = executor.create_node("tines")?;

        let store = LogStore::new(log_path, 0, first_seq)?;
        let transport = BtTransport::new(me, peer, radio);
        let tines = TinesNodeCore::new(me, first_seq, store, transport);

        let receipt_pub = node.create_publisher::<UInt8MultiArray>(RECEIPTS_TOPIC)?;
        let (inbound_tx, inbound_rx) = channel::<InboundMsg>();

        Ok(Self {
            node,
            tines,
            managed: HashMap::new(),
            receipt_pub,
            inbound_rx,
            inbound_tx,
            topic_cap,
        })
    }

    /// The local tines node id this node was built with.
    pub fn id(&self) -> NodeId {
        self.tines.id()
    }

    /// The current in-memory count of managed ROS2 topics.
    pub fn managed_topic_count(&self) -> usize {
        self.managed.len()
    }

    /// One bridge tick: consume inbound ROS2 observations, drive the tines
    /// exchange, publish live topics, and publish any confidence events.
    ///
    /// `now` is a monotonic millisecond [`Tick`]: milliseconds since the host
    /// process started (a single `Instant`-based clock, `0` at startup), never
    /// wall-clock time. Pass the same clock's value every call so ages and
    /// addressed lifetimes advance in real milliseconds.
    ///
    /// Returns the number of *new* observations stored this round (from
    /// [`TinesNodeCore::poll`]).
    pub fn poll(&mut self, now: Tick) -> Result<usize, TinesNodeError> {
        self.drain_inbound(now);
        let stored = self.tines.poll(now);
        self.publish_live_topics(now)?;
        self.publish_receipts();
        Ok(stored)
    }

    /// Consume inbound ROS2 observations into the tines node, skipping anything
    /// oversized for tines to reject.
    fn drain_inbound(&mut self, now: Tick) {
        while let Ok((topic, payload)) = self.inbound_rx.try_recv() {
            if payload.len() > tines_core::consts::MAX_PAYLOAD
                || topic.len() > tines_core::consts::MAX_TOPIC
            {
                continue; // oversized; tines would reject it anyway
            }
            let _ = self.tines.observe(&topic, &payload, None, now);
        }
    }

    /// Lazily create a ROS2 publisher/subscriber pair for `tines_topic`, bounded
    /// by [`TinesNode::new`]'s `topic_cap`.
    ///
    /// The subscription callback carries a cloned channel sender and a copy of
    /// the original topic bytes (not `self`) so it can outlive this borrow and
    /// run on the rclrs dispatch thread.
    fn ensure(&mut self, tines_topic: &[u8]) -> Result<(), TinesNodeError> {
        let name = topic_name(tines_topic);
        if self.managed.contains_key(&name) {
            return Ok(());
        }
        if self.managed.len() >= self.topic_cap {
            return Ok(()); // cap reached; bound live-topic creation
        }

        // The subscription callback owns a channel sender and a copy of the
        // original topic bytes — not `self` — so it can outlive this borrow.
        let inbound_tx = self.inbound_tx.clone();
        let tines_topic_cb = tines_topic.to_vec();
        let subscriber = self
            .node
            .create_subscription(&name, move |msg: UInt8MultiArray| {
                // `msg.data` is the raw payload bytes verbatim.
                let _ = inbound_tx.send((tines_topic_cb.clone(), msg.data.clone()));
            })?;
        let publisher = self.node.create_publisher::<UInt8MultiArray>(&name)?;

        self.managed.insert(
            name,
            ManagedTopic {
                tines_topic: tines_topic.to_vec(),
                publisher: Some(publisher),
                subscriber: Some(subscriber),
                last_pub: None,
            },
        );
        Ok(())
    }

    /// Publish every distinct topic currently live in the store.
    fn publish_live_topics(&mut self, now: Tick) -> Result<(), TinesNodeError> {
        // Distinct live topics, via the store's topic iterator.
        let mut topics: BTreeSet<Vec<u8>> = BTreeSet::new();
        self.tines.store().for_each_topic(&[], now, &mut |obs| {
            topics.insert(obs.topic.as_slice().to_vec());
        });
        // Create publishers for any topic we don't yet manage (cap-bounded in
        // [`Self::ensure`]).
        for tines_topic in topics {
            let name = topic_name(&tines_topic);
            if self.managed.contains_key(&name) {
                continue;
            }
            self.ensure(&tines_topic)?;
        }

        // Publish the latest observation for every managed topic. We only need
        // each topic's `tines_topic` bytes to look up its latest observation,
        // so clone them (and the name) into owned locals *before* calling
        // `publish_one`. That drops the borrow of `self.managed` up front —
        // holding `&mut self.managed` across the `&mut self` call is the
        // sibling of the E0502 the reviewer flagged.
        let managed: Vec<(String, Vec<u8>)> = self
            .managed
            .iter()
            .map(|(name, t)| (name.clone(), t.tines_topic.clone()))
            .collect();
        for (name, tines_topic) in managed {
            self.publish_one(&tines_topic, name, now)?;
        }
        Ok(())
    }

    /// Publish the latest observation for a single managed topic, unless its
    /// value is unchanged since the last publish (to avoid a busy loop).
    fn publish_one(
        &mut self,
        tines_topic: &[u8],
        name: String,
        now: Tick,
    ) -> Result<(), TinesNodeError> {
        // `latest_obs` takes `&self`, but `topic` below would hold a live
        // `&mut self.managed` — the E0502 the reviewer flagged. Collect the
        // latest observation on an immutable borrow that ends here, then take
        // `&mut self.managed` for only the publish + bookkeeping that follows.
        let Some(obs) = self.latest_obs(tines_topic, now) else {
            return Ok(());
        };
        if let Some(topic) = self.managed.get_mut(&name) {
            if topic.last_pub == Some(obs.id()) {
                return Ok(());
            }
            // Publish the raw payload bytes as a `UInt8MultiArray`.
            let data = obs.payload.to_vec();
            let msg = UInt8MultiArray {
                data,
                layout: MultiArrayLayout {
                    dim: vec![],
                    data_offset: 0,
                },
            };
            if let Some(publisher) = topic.publisher.as_mut() {
                let _ = publisher.publish(&msg);
                topic.last_pub = Some(obs.id());
            }
        }
        Ok(())
    }

    /// The youngest observation for `tines_topic`, if any.
    fn latest_obs(&self, tines_topic: &[u8], now: Tick) -> Option<Observation> {
        let mut best: Option<Observation> = None;
        self.tines
            .store()
            .for_each_topic(tines_topic, now, &mut |obs| match best {
                Some(ref b) if obs.age_ms >= b.age_ms => {}
                _ => best = Some(obs.clone()),
            });
        best
    }

    /// Publish every pending confidence event to [`RECEIPTS_TOPIC`].
    ///
    /// Each receipt is a 32-byte `UInt8MultiArray` = `peer[..16] ++ id[..16]`,
    /// the raw node ids (no hex encoding).
    fn publish_receipts(&mut self) {
        let events = self.tines.drain_events();
        for ev in events.iter() {
            let mut data = [0u8; 32];
            data[..16].copy_from_slice(&ev.peer);
            data[16..32].copy_from_slice(&ev.id);
            let msg = UInt8MultiArray {
                data: data.to_vec(),
                layout: MultiArrayLayout {
                    dim: vec![],
                    data_offset: 0,
                },
            };
            let _ = self.receipt_pub.publish(&msg);
        }
    }
}
