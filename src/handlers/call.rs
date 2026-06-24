use std::sync::Arc;

use async_trait::async_trait;
use log::{debug, warn};
#[cfg(feature = "voip")]
use wacore::stanza::call::{TerminateParams, build_terminate};
use wacore::stanza::call::{build_offer_ack_receipt, parse_call_stanza};
use wacore::types::call::{CallAction, IncomingCall, MissedCall, MissedReason};
use wacore::types::events::Event;
#[cfg(feature = "voip")]
use wacore_binary::Jid;
use wacore_binary::{OwnedNodeRef, Server};

use crate::client::Client;

use super::traits::StanzaHandler;

/// Router sends the generic `<ack>` via `should_ack`, so this handler only
/// parses and dispatches. On `Offer` it also emits the `<receipt><offer/></receipt>`
/// ack-of-offer so the caller's signaling layer knows the device received the ring.
#[derive(Default)]
pub struct CallHandler;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl StanzaHandler for CallHandler {
    fn tag(&self) -> &'static str {
        "call"
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(name = "wa.recv.call", level = "debug", skip_all)
    )]
    async fn handle(
        &self,
        client: Arc<Client>,
        node: Arc<OwnedNodeRef>,
        _cancelled: &mut bool,
    ) -> bool {
        let nr = node.get();
        match parse_call_stanza(nr) {
            Ok(Some(call)) => {
                // Diagnostic: every recognized <call> action we receive (offer/accept/reject/
                // terminate/transport/relaylatency...). Lets us see whether the caller actually gets a
                // peer device's <accept> (which drives the sibling dismiss).
                debug!(
                    "call: received {} for {} from {}",
                    call.action.action_kind(),
                    call.action.call_id(),
                    call.from.observe()
                );
                let is_offer = matches!(call.action, CallAction::Offer { .. });
                if is_offer && call.offline {
                    // Offline-queue replay: the call is long dead (no relay, not connectable). Don't
                    // ack or ring it -- surface a non-ringing missed-call so a consumer can't auto-
                    // accept it (WA Web's cancel_call + missed_call for offerReceivedWhileOffline).
                    client
                        .core
                        .event_bus
                        .dispatch(Event::MissedCall(MissedCall::new(
                            call.from.clone(),
                            call.action.call_id().to_string(),
                            call.timestamp,
                            MissedReason::Offline,
                        )));
                } else {
                    if is_offer && let Err(e) = send_offer_ack_receipt(&client, &call).await {
                        warn!("call: failed to send offer ack receipt: {e}");
                    }
                    // Caller-side: key our recv path to the device that actually answered. We dial the
                    // base callee LID, but a companion answers from `:N` and encrypts under its own
                    // device id; without this every inbound frame decrypts to garbage. One-shot, and a
                    // no-op for an incoming call or a call we aren't the caller of (no sender registered).
                    #[cfg(feature = "voip")]
                    if let CallAction::Accept { .. } = &call.action {
                        client
                            .call_registry()
                            .send_rekey(call.action.call_id(), call.from.to_string());
                    }
                    // Caller-side multi-device dismiss: when one of the callee's devices accepts or
                    // rejects an outbound call of ours, tell the rest to stop ringing.
                    #[cfg(feature = "voip")]
                    dismiss_outgoing_siblings(&client, &call).await;
                    // A peer <reject>/<terminate> ends the call: tear down the media task and any
                    // dormant pending-outgoing entry so CallHandle::wait_ended() resolves, instead of
                    // leaking the relay/mic task until an unrelated relay timeout. Runs after dismiss
                    // (which reads the registry entry) and before the move into dispatch.
                    // A <terminate> for a call with no active or pending registry entry is an
                    // unanswered incoming call the peer gave up on: surface a missed call (WA Web's
                    // "missed" call-log outcome) alongside the generic event.
                    #[cfg(feature = "voip")]
                    if let CallAction::Terminate { .. } = &call.action
                        && client
                            .call_registry()
                            .generation_of(call.action.call_id())
                            .is_none()
                    {
                        client
                            .core
                            .event_bus
                            .dispatch(Event::MissedCall(MissedCall::new(
                                call.from.clone(),
                                call.action.call_id().to_string(),
                                call.timestamp,
                                MissedReason::Remote,
                            )));
                    }
                    #[cfg(feature = "voip")]
                    if matches!(
                        &call.action,
                        CallAction::Reject { .. } | CallAction::Terminate { .. }
                    ) {
                        crate::voip::facade::terminate_call(&client, call.action.call_id());
                    }
                    client.core.event_bus.dispatch(Event::IncomingCall(call));
                }
            }
            Ok(None) => {
                debug!("call: ignoring unrecognized action (forward-compat)");
            }
            Err(e) => {
                warn!("call: failed to parse stanza: {e}");
            }
        }
        true
    }
}

#[cfg_attr(feature = "tracing", tracing::instrument(name = "wa.recv.call_offer_ack", level = "debug", skip_all, fields(peer = %call.from.observe()), err(Debug)))]
async fn send_offer_ack_receipt(client: &Client, call: &IncomingCall) -> anyhow::Result<()> {
    let own_from = match call.from.server {
        Server::Lid => client.get_lid(),
        _ => client.get_pn(),
    };

    let Some(receipt) = build_offer_ack_receipt(call, own_from.as_ref()) else {
        return Ok(());
    };

    client.send_node(receipt).await.map_err(anyhow::Error::from)
}

/// Caller-side sibling dismiss. When a callee device accepts/rejects one of OUR outbound calls, the
/// caller (us) tells the callee's OTHER devices to stop ringing via `<terminate reason=...>` -- the
/// dismiss is caller-driven (the callee never dismisses its own siblings; verified vs WA Web + APK).
/// The rung device set lives on the registry session (`take_dismiss_targets`), consumed one-shot so a
/// duplicate accept/reject can't re-dismiss. No-op for any other action, or a call we aren't the
/// caller of (inbound call, single-device callee, or one already dismissed). A `Terminate` needs no
/// handling here: the call ends, its registry entry (and the device set with it) goes away.
#[cfg(feature = "voip")]
async fn dismiss_outgoing_siblings(client: &Client, call: &IncomingCall) {
    let reason = match &call.action {
        CallAction::Accept { .. } => "accepted_elsewhere",
        CallAction::Reject { .. } => "rejected_elsewhere",
        _ => return,
    };
    let call_id = call.action.call_id();

    let Some((call_creator, devices)) = client.call_registry().take_dismiss_targets(call_id) else {
        // Either not our outgoing call, the call already deregistered, the rung set was already
        // consumed (a duplicate accept), or it rang a single device. If a multi-device callee's
        // sibling is still ringing and we land here, the rung set wasn't there to dismiss from.
        debug!("call: {reason} for {call_id}: no sibling-dismiss targets tracked");
        return;
    };

    // Send ONE <terminate> per sibling device, addressed to that DEVICE JID with a generated wrapper
    // id -- the WA Web/APK form. (A single stanza with a <destination> block to the bare peer is NOT
    // it: WA Web gates the destination fan-out to offer/enc_rekey, and the server routes call
    // signaling per device.) Skip the device that accepted/rejected: compare on device identity
    // (user + server + device), not full Jid equality, since the usync device-list and the accept's
    // `from` can carry a different `agent` for the same physical device.
    let others: Vec<Jid> = devices
        .into_iter()
        .filter(|d| !same_device(d, &call.from))
        .collect();
    debug!(
        "call: {reason} from {} for {call_id}: dismissing {} sibling device(s)",
        call.from.observe(),
        others.len()
    );
    for dev in &others {
        let id = client.generate_request_id();
        let node = build_terminate(&TerminateParams {
            call_id,
            to: dev,
            id: Some(&id),
            call_creator: &call_creator,
            reason: Some(reason),
        });
        match client.send_node(node).await {
            Ok(()) => debug!(
                "call: dismissed sibling device {} ({reason}) for {call_id}",
                dev.observe()
            ),
            Err(e) => warn!(
                "call: failed to dismiss sibling device {}: {e}",
                dev.observe()
            ),
        }
    }
}

/// Whether two JIDs name the same device: user + server + device id. Excludes `agent`/`integrator`,
/// representation details that can differ between the usync device-list and a stanza's `from`.
#[cfg(feature = "voip")]
fn same_device(a: &Jid, b: &Jid) -> bool {
    a.user == b.user && a.server == b.server && a.device == b.device
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{MockHttpClient, create_test_backend, node_to_owned_ref};
    use std::sync::Arc;
    use wacore::types::events::{ChannelEventHandler, Event};
    use wacore_binary::builder::NodeBuilder;
    use wacore_binary::{Jid, Server};

    fn fake_caller_lid() -> Jid {
        Jid::new("111111111111111", Server::Lid)
    }

    fn offer_stanza() -> wacore_binary::Node {
        NodeBuilder::new("call")
            .attr("from", fake_caller_lid())
            .attr("id", "STANZA-ID-0001")
            .attr("t", "1766847151")
            .children([NodeBuilder::new("offer")
                .attr("call-creator", fake_caller_lid())
                .attr("call-id", "CALL-ID-0001")
                .children([NodeBuilder::new("audio")
                    .attr("enc", "opus")
                    .attr("rate", "16000")
                    .build()])
                .build()])
            .build()
    }

    async fn make_client() -> Arc<Client> {
        use crate::store::persistence_manager::PersistenceManager;
        let backend = create_test_backend().await;
        let pm = PersistenceManager::new(backend)
            .await
            .expect("persistence manager should initialize");
        let transport = Arc::new(crate::transport::mock::MockTransportFactory::new());
        let http_client = Arc::new(MockHttpClient);
        let (client, _rx) = Client::new(
            Arc::new(crate::runtime_impl::TokioRuntime),
            Arc::new(pm),
            transport,
            http_client,
            None,
        )
        .await;
        client
    }

    #[tokio::test]
    async fn offer_dispatches_event() {
        let client = make_client().await;
        let (handler, rx) = ChannelEventHandler::new();
        client.register_handler(handler);

        let node = node_to_owned_ref(&offer_stanza());
        let mut cancelled = false;
        assert!(CallHandler.handle(client, node, &mut cancelled).await);

        let mut seen = false;
        while let Ok(ev) = rx.try_recv() {
            if matches!(&*ev, Event::IncomingCall(call) if call.action.call_id() == "CALL-ID-0001")
            {
                seen = true;
                break;
            }
        }
        assert!(seen, "IncomingCall event must be dispatched");
    }

    #[tokio::test]
    async fn unrecognized_action_does_not_dispatch() {
        let client = make_client().await;
        let (handler, rx) = ChannelEventHandler::new();
        client.register_handler(handler);

        let node = node_to_owned_ref(
            &NodeBuilder::new("call")
                .attr("from", fake_caller_lid())
                .attr("id", "S")
                .attr("t", "1766847151")
                .children([NodeBuilder::new("surprise").build()])
                .build(),
        );
        let mut cancelled = false;
        assert!(CallHandler.handle(client, node, &mut cancelled).await);

        while let Ok(ev) = rx.try_recv() {
            assert!(
                !matches!(&*ev, Event::IncomingCall(_)),
                "must not dispatch IncomingCall for unknown action"
            );
        }
    }

    /// Drives the handler end-to-end with a real `NoiseSocket` wired to a
    /// counting transport so the offer-ack send path is exercised. Without
    /// this, a regression that removes `send_offer_ack_receipt` from the
    /// handler would go unnoticed by the event-dispatch test alone.
    #[tokio::test]
    async fn offer_triggers_outbound_send() {
        use async_trait::async_trait;
        use bytes::Bytes;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use wacore::handshake::NoiseCipher;

        struct CountingTransport {
            count: Arc<AtomicUsize>,
        }

        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        impl crate::transport::Transport for CountingTransport {
            async fn send(&self, _data: Bytes) -> Result<(), anyhow::Error> {
                self.count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
            async fn disconnect(&self) {}
        }

        let client = make_client().await;
        let count = Arc::new(AtomicUsize::new(0));
        let transport: Arc<dyn crate::transport::Transport> = Arc::new(CountingTransport {
            count: count.clone(),
        });
        let key = [0u8; 32];
        let noise_socket = crate::socket::NoiseSocket::new(
            Arc::new(crate::runtime_impl::TokioRuntime),
            transport,
            NoiseCipher::new(&key).expect("valid key"),
            NoiseCipher::new(&key).expect("valid key"),
        );
        *client.noise_socket.lock().await = Some(Arc::new(noise_socket));

        let node = node_to_owned_ref(&offer_stanza());
        let mut cancelled = false;
        assert!(CallHandler.handle(client, node, &mut cancelled).await);

        assert!(
            count.load(Ordering::SeqCst) >= 1,
            "handler must invoke the outbound send path for offer ack receipts"
        );
    }

    #[tokio::test]
    async fn malformed_stanza_does_not_error_or_dispatch() {
        let client = make_client().await;
        let (handler, rx) = ChannelEventHandler::new();
        client.register_handler(handler);

        let node = node_to_owned_ref(
            &NodeBuilder::new("call")
                .attr("from", fake_caller_lid())
                .attr("id", "S")
                .children([NodeBuilder::new("offer")
                    .attr("call-creator", fake_caller_lid())
                    .attr("call-id", "X")
                    .build()])
                .build(),
        );
        let mut cancelled = false;
        assert!(CallHandler.handle(client, node, &mut cancelled).await);
        while let Ok(ev) = rx.try_recv() {
            assert!(!matches!(&*ev, Event::IncomingCall(_)));
        }
    }

    // Caller-side sibling dismiss: when one callee device accepts our outbound call, the OTHER rung
    // device gets a per-device `<call to=DEVICE_JID id=..><terminate reason="accepted_elsewhere">`
    // (no <destination> block), and the rung set is consumed one-shot. A two-device callee keeps the
    // assertion to a single dismiss stanza the waiter can capture in full.
    #[cfg(feature = "voip")]
    #[tokio::test]
    async fn accept_dismisses_other_callee_device() {
        let client = make_client().await;
        let peer = Jid::new("222222222222222", Server::Lid);
        let creator = Jid::new("111111111111111", Server::Lid);
        let (sibling, accepting) = (peer.with_device(1), peer.with_device(2));

        // Register the outbound call with its rung device set on the session (as place_call does).
        let mut session =
            wacore::voip::CallSession::new_outgoing("CALL-ID-0001", peer.clone(), creator.clone());
        session.ring_devices = vec![sibling.clone(), accepting.clone()];
        client.call_registry().insert(session);

        // The `accepting` device accepts.
        let accept = NodeBuilder::new("call")
            .attr("from", accepting.clone())
            .attr("id", "STANZA-ACCEPT")
            .attr("t", "1766847151")
            .children([NodeBuilder::new("accept")
                .attr("call-creator", creator.clone())
                .attr("call-id", "CALL-ID-0001")
                .build()])
            .build();

        let waiter = client.wait_for_sent_node(crate::client::NodeFilter::tag("call"));
        let mut cancelled = false;
        assert!(
            CallHandler
                .handle(client.clone(), node_to_owned_ref(&accept), &mut cancelled)
                .await
        );

        let sent = waiter.await.expect("a dismiss <terminate> must be sent");
        let r = sent.as_node_ref();
        // Addressed to the SIBLING device JID (not the bare peer, not the accepting device), with an id.
        assert_eq!(
            r.attrs().optional_string("to").as_deref(),
            Some(sibling.to_string().as_str())
        );
        assert!(
            r.attrs().optional_string("id").is_some(),
            "wrapper needs an id"
        );
        let term = &r.children().unwrap()[0];
        assert_eq!(term.tag, "terminate");
        assert_eq!(
            term.attrs().optional_string("reason").as_deref(),
            Some("accepted_elsewhere")
        );
        assert!(
            term.get_optional_child("destination").is_none(),
            "terminate must not use a <destination> block"
        );
        assert!(
            client
                .call_registry()
                .take_dismiss_targets("CALL-ID-0001")
                .is_none(),
            "the rung device set must be consumed one-shot"
        );
    }

    // A peer <terminate> for our call tears it down: the registry entry (and with it the media task)
    // is removed so CallHandle::wait_ended() resolves, instead of leaking until a relay timeout.
    #[cfg(feature = "voip")]
    #[tokio::test]
    async fn terminate_tears_down_the_call() {
        let client = make_client().await;
        let peer = Jid::new("222222222222222", Server::Lid);
        let creator = Jid::new("111111111111111", Server::Lid);
        let session =
            wacore::voip::CallSession::new_outgoing("CALL-ID-0001", peer.clone(), creator.clone());
        client.call_registry().insert(session);
        assert!(
            client
                .call_registry()
                .generation_of("CALL-ID-0001")
                .is_some(),
            "precondition: the call is registered"
        );

        let terminate = NodeBuilder::new("call")
            .attr("from", peer.with_device(1))
            .attr("id", "STANZA-TERM")
            .attr("t", "1766847151")
            .children([NodeBuilder::new("terminate")
                .attr("call-creator", creator.clone())
                .attr("call-id", "CALL-ID-0001")
                .build()])
            .build();

        let mut cancelled = false;
        assert!(
            CallHandler
                .handle(
                    client.clone(),
                    node_to_owned_ref(&terminate),
                    &mut cancelled
                )
                .await
        );

        assert!(
            client
                .call_registry()
                .generation_of("CALL-ID-0001")
                .is_none(),
            "a peer <terminate> must remove the call from the registry"
        );
    }

    // A <terminate> for a call we never answered (no registry entry) surfaces a MissedCall(Remote),
    // mirroring WA Web's missed-call outcome for an unanswered incoming call.
    #[cfg(feature = "voip")]
    #[tokio::test]
    async fn unanswered_terminate_surfaces_missed_call() {
        let client = make_client().await;
        let (handler, rx) = ChannelEventHandler::new();
        client.register_handler(handler);

        let terminate = NodeBuilder::new("call")
            .attr("from", fake_caller_lid())
            .attr("id", "STANZA-TERM")
            .attr("t", "1766847151")
            .children([NodeBuilder::new("terminate")
                .attr("call-creator", fake_caller_lid())
                .attr("call-id", "CALL-ID-0001")
                .build()])
            .build();

        let mut cancelled = false;
        assert!(
            CallHandler
                .handle(
                    client.clone(),
                    node_to_owned_ref(&terminate),
                    &mut cancelled
                )
                .await
        );

        let mut seen = false;
        while let Ok(ev) = rx.try_recv() {
            if let Event::MissedCall(m) = &*ev
                && m.call_id == "CALL-ID-0001"
                && matches!(m.reason, MissedReason::Remote)
            {
                seen = true;
            }
        }
        assert!(
            seen,
            "an unanswered <terminate> must surface a MissedCall(Remote)"
        );
    }
}
