use alloc::string::String;
use alloc::vec::Vec;
use alloc::boxed::Box;;
pub struct ShardSend<'a, M> {
    sender: &'a ShardSender<M>,
    message: Option<M>,
}

// Safety: the message field is accessed by value (take/replace), never pinned.
impl<M> Unpin for ShardSend<'_, M> {}

impl<M: Send + 'static> Future for ShardSend<'_, M> {
    type Output = Result<(), ShardSendError<M>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.sender.shared.poll_send(&mut this.message, context)
    }
}

impl<M> Drop for ShardSend<'_, M> {
    fn drop(&mut self) {
        if self.message.is_some() {
            self.sender.shared.wake_one_sender();
        }
    }
}

impl<M> fmt::Debug for ShardSend<'_, M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardSend")
            .field("target_shard", &self.sender.shared.shard_id)
            .field("pending", &self.message.is_some())
            .finish()
    }
}

struct SharedMailbox<M> {
    shard_id: ShardId,
    state: Mutex<MailboxState<M>>,
    sent: AtomicU64,
    received: AtomicU64,
    full_rejections: AtomicU64,
    closed_rejections: AtomicU64,
}

struct MailboxState<M> {
    queue: VecDeque<M>,
    capacity: usize,
    sender_count: usize,
    sender_factory_open: bool,
    receiver_taken: bool,
    receiver_closed: bool,
    recv_waker: Option<Waker>,
    send_wakers: VecDeque<Waker>,
}

impl<M> SharedMailbox<M> {
    fn try_send(&self, message: M) -> Result<(), ShardSendError<M>> {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if state.receiver_closed {
                self.closed_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(ShardSendError::Closed(message));
            }
            if state.queue.len() == state.capacity {
                self.full_rejections.fetch_add(1, Ordering::AcqRel);
                return Err(ShardSendError::Full(message));
            }

            state.queue.push_back(message);
            self.sent.fetch_add(1, Ordering::AcqRel);
            state.recv_waker.take()
        };

        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(())
    }

    fn try_recv(&self) -> Result<M, ShardRecvError> {
        let send_wake;
        let result;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if let Some(message) = state.queue.pop_front() {
                self.received.fetch_add(1, Ordering::AcqRel);
                send_wake = state.send_wakers.pop_front();
                result = Ok(message);
            } else if state.receiver_closed
                || (!state.sender_factory_open && state.sender_count == 0)
            {
                send_wake = None;
                result = Err(ShardRecvError::Closed);
            } else {
                send_wake = None;
                result = Err(ShardRecvError::Empty);
            }
        }
        if let Some(waker) = send_wake {
            waker.wake();
        }
        result
    }

    fn poll_recv(&self, context: &mut Context<'_>) -> Poll<Result<M, ShardRecvError>> {
        let send_wake;
        let result;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if let Some(message) = state.queue.pop_front() {
                self.received.fetch_add(1, Ordering::AcqRel);
                send_wake = state.send_wakers.pop_front();
                result = Poll::Ready(Ok(message));
            } else if state.receiver_closed
                || (!state.sender_factory_open && state.sender_count == 0)
            {
                send_wake = None;
                result = Poll::Ready(Err(ShardRecvError::Closed));
            } else {
                if !state
                    .recv_waker
                    .as_ref()
                    .is_some_and(|waker| waker.will_wake(context.waker()))
                {
                    state.recv_waker = Some(context.waker().clone());
                }
                send_wake = None;
                result = Poll::Pending;
            }
        }
        if let Some(waker) = send_wake {
            waker.wake();
        }
        result
    }

    fn close_receiver(&self) {
        let recv_wake;
        let send_wakes;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if state.receiver_closed {
                return;
            }
            state.receiver_closed = true;
            recv_wake = state.recv_waker.take();
            send_wakes = state.send_wakers.drain(..).collect::<Vec<_>>();
        }

        if let Some(waker) = recv_wake {
            waker.wake();
        }
        for waker in send_wakes {
            waker.wake();
        }
    }

    fn drop_sender(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            state.sender_count = state.sender_count.saturating_sub(1);
            if !state.sender_factory_open && state.sender_count == 0 && state.queue.is_empty() {
                state.recv_waker.take()
            } else {
                None
            }
        };

        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn close_sender_factory(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            if !state.sender_factory_open {
                None
            } else {
                state.sender_factory_open = false;
                if state.sender_count == 0 && state.queue.is_empty() {
                    state.recv_waker.take()
                } else {
                    None
                }
            }
        };

        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn poll_send(
        &self,
        message: &mut Option<M>,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), ShardSendError<M>>> {
        let recv_wake;
        {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            let msg = message.take().expect("poll_send called after completion");
            if state.receiver_closed {
                self.closed_rejections.fetch_add(1, Ordering::AcqRel);
                return Poll::Ready(Err(ShardSendError::Closed(msg)));
            }
            if state.queue.len() < state.capacity {
                state.queue.push_back(msg);
                self.sent.fetch_add(1, Ordering::AcqRel);
                recv_wake = state.recv_waker.take();
            } else {
                *message = Some(msg);
                state.send_wakers.push_back(context.waker().clone());
                return Poll::Pending;
            }
        }
        if let Some(waker) = recv_wake {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }

    fn wake_one_sender(&self) {
        let wake = {
            let mut state = self.state.lock().expect("mailbox mutex poisoned");
            state.send_wakers.pop_front()
        };
        if let Some(waker) = wake {
            waker.wake();
        }
    }

    fn snapshot(&self) -> ShardMailboxSnapshot {
        let state = self.state.lock().expect("mailbox mutex poisoned");
        ShardMailboxSnapshot {
            shard_id: self.shard_id,
            capacity: state.capacity,
            len: state.queue.len(),
            sender_count: state.sender_count,
            receiver_taken: state.receiver_taken,
            receiver_closed: state.receiver_closed,
            sent: self.sent.load(Ordering::Acquire),
            received: self.received.load(Ordering::Acquire),
            full_rejections: self.full_rejections.load(Ordering::Acquire),
            closed_rejections: self.closed_rejections.load(Ordering::Acquire),
            send_waiter_count: state.send_wakers.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use core::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use crate::executor::{block_on, yield_now};
    use crate::{ShardedExecutor, current_executor_shard};

    use super::*;

    #[derive(Debug)]
    struct DropCounter {
        drops: Arc<AtomicUsize>,
    }

    impl Drop for DropCounter {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn construction_rejects_zero_values() {
        assert_eq!(
            ShardMailboxSet::<usize>::with_shard_count(0, ShardMailboxConfig::new(1)).unwrap_err(),
            ShardMailboxCreateError::ZeroShards
        );
        assert_eq!(
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(0)).unwrap_err(),
            ShardMailboxCreateError::ZeroCapacity
        );
    }

    #[test]
    fn uniform_router_maps_keys_to_physical_shards() {
        assert_eq!(
            UniformShardRouter::new(0).unwrap_err(),
            KeyRouterCreateError::ZeroTargets
        );

        let router = UniformShardRouter::new(4).unwrap();
        let target = router.route("alpha");

        assert!(target.0 < router.shard_count());
        assert_eq!(target, shard_for_hash("alpha", 4));
    }

    #[test]
    fn work_unit_router_maps_keys_to_logical_names() {
        assert_eq!(
            WorkUnitRouter::<String>::new(Vec::new()).unwrap_err(),
            KeyRouterCreateError::ZeroTargets
        );

        let router = WorkUnitRouter::new([
            String::from("assembler-a"),
            String::from("assembler-b"),
            String::from("assembler-c"),
        ])
        .unwrap();
        let target = router.route("alpha");

        assert_eq!(router.work_unit_count(), 3);
        assert!(router.work_units().contains(&target));
        assert_eq!(target, router.work_units()[shard_for_hash("alpha", 3).0]);
    }

    #[test]
    fn try_send_and_try_recv_transfer_owned_messages() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(String::from("hello")).unwrap();

        assert_eq!(receiver.try_recv().unwrap(), "hello");
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Empty));
    }

    #[test]
    fn messages_preserve_fifo_order() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(3)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        sender.try_send(3).unwrap();

        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Ok(3));
    }

    #[test]
    fn full_mailbox_returns_original_message() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();

        sender.try_send(String::from("first")).unwrap();
        let error = sender.try_send(String::from("second")).unwrap_err();

        assert!(matches!(error, ShardSendError::Full(_)));
        assert_eq!(error.into_message(), "second");
        assert_eq!(mailboxes.snapshot(ShardId(0)).unwrap().full_rejections, 1);
    }

    #[test]
    fn receiver_close_rejects_future_sends() {
        let mailboxes =
            ShardMailboxSet::<String>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        receiver.close();
        let error = sender.try_send(String::from("closed")).unwrap_err();

        assert!(matches!(error, ShardSendError::Closed(_)));
        assert_eq!(error.into_message(), "closed");
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Closed));
        assert_eq!(mailboxes.snapshot(ShardId(0)).unwrap().closed_rejections, 1);
    }

    #[test]
    fn dropping_all_senders_closes_after_draining() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        sender.try_send(7).unwrap();
        drop(sender);

        assert_eq!(receiver.try_recv(), Ok(7));
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Empty));
        drop(mailboxes);
        assert_eq!(receiver.try_recv(), Err(ShardRecvError::Closed));
    }

    #[test]
    fn taking_two_receivers_for_same_shard_fails() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(1)).unwrap();
        let _receiver = mailboxes.receiver_for(ShardId(0)).unwrap();

        assert_eq!(
            mailboxes.receiver_for(ShardId(0)).unwrap_err(),
            ShardMailboxAddressError::ReceiverAlreadyTaken {
                shard_id: ShardId(0)
            }
        );
    }

    #[test]
    fn queued_messages_drop_exactly_once() {
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let mailboxes = ShardMailboxSet::with_shard_count(1, ShardMailboxConfig::new(2))
                .expect("mailbox set starts");
            let sender = mailboxes.sender_to(ShardId(0)).unwrap();
            sender
                .try_send(DropCounter {
                    drops: Arc::clone(&drops),
                })
                .unwrap();
            sender
                .try_send(DropCounter {
                    drops: Arc::clone(&drops),
                })
                .unwrap();
        }

        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn snapshots_report_state_and_counters() {
        let mailboxes =
            ShardMailboxSet::<usize>::with_shard_count(1, ShardMailboxConfig::new(2)).unwrap();
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        let mut receiver = mailboxes.receiver_for(ShardId(0)).unwrap();
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert!(matches!(sender.try_send(3), Err(ShardSendError::Full(3))));
        assert_eq!(receiver.try_recv(), Ok(1));

        assert_eq!(
            mailboxes.snapshot(ShardId(0)).unwrap(),
            ShardMailboxSnapshot {
                shard_id: ShardId(0),
                capacity: 2,
                len: 1,
                sender_count: 1,
                receiver_taken: true,
                receiver_closed: false,
                sent: 2,
                received: 1,
                full_rejections: 1,
                closed_rejections: 0,
                send_waiter_count: 0,
            }
        );
    }

    #[test]
    fn work_unit_mailboxes_reject_invalid_definitions() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();

        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                Vec::new(),
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::NoWorkUnits
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("a"), ShardId(0))],
                ShardMailboxConfig::new(0),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::ZeroCapacity
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("a"), ShardId(9))],
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::InvalidShard {
                name: String::from("a"),
                shard_id: ShardId(9),
            }
        );
        assert_eq!(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [
                    WorkUnitSpec::new(String::from("a"), ShardId(0)),
                    WorkUnitSpec::new(String::from("a"), ShardId(1)),
                ],
                ShardMailboxConfig::new(1),
            )
            .unwrap_err(),
            WorkUnitMailboxCreateError::DuplicateName {
                name: String::from("a"),
            }
        );

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn work_unit_mailboxes_route_by_logical_name() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let mailboxes = WorkUnitMailboxSet::new(
            &submitter,
            [
                WorkUnitSpec::new(String::from("parse"), ShardId(0)),
                WorkUnitSpec::new(String::from("merge-a"), ShardId(1)),
                WorkUnitSpec::new(String::from("merge-b"), ShardId(1)),
            ],
            ShardMailboxConfig::new(2),
        )
        .unwrap();

        assert_eq!(
            mailboxes.assigned_shard(&String::from("parse")),
            Ok(ShardId(0))
        );
        assert_eq!(
            mailboxes.assigned_shard(&String::from("merge-b")),
            Ok(ShardId(1))
        );

        let sender_a = mailboxes.sender_to(&String::from("merge-a")).unwrap();
        let sender_b = mailboxes.sender_to(&String::from("merge-b")).unwrap();
        let mut receiver_a = mailboxes.receiver_for(&String::from("merge-a")).unwrap();
        let mut receiver_b = mailboxes.receiver_for(&String::from("merge-b")).unwrap();

        sender_a.try_send(10).unwrap();
        sender_b.try_send(20).unwrap();

        assert_eq!(receiver_a.try_recv(), Ok(10));
        assert_eq!(receiver_b.try_recv(), Ok(20));
        assert_eq!(
            mailboxes
                .snapshot(&String::from("merge-a"))
                .unwrap()
                .mailbox
                .received,
            1
        );

        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn work_unit_receiver_for_current_shard_rejects_wrong_shard() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let submitter = runtime.submitter();
        let mailboxes = Arc::new(
            WorkUnitMailboxSet::<String, usize>::new(
                &submitter,
                [WorkUnitSpec::new(String::from("owned-by-one"), ShardId(1))],
                ShardMailboxConfig::new(1),
            )
            .unwrap(),
        );

        let task_mailboxes = Arc::clone(&mailboxes);
        let handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                task_mailboxes.receiver_for_current_shard(&String::from("owned-by-one"))
            })
            .unwrap();

        assert_eq!(
            block_on(handle).unwrap().unwrap_err(),
            WorkUnitMailboxAddressError::WrongShard {
                name: String::from("owned-by-one"),
                assigned_shard: ShardId(1),
                current_shard: ShardId(0),
            }
        );

        drop(mailboxes);
        drop(submitter);
        runtime.stop().unwrap();
    }

    #[test]
    fn recv_await_wakes_when_another_shard_sends() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(2)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        let receiver_mailboxes = Arc::clone(&mailboxes);

        let receiver = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                let before = current_executor_shard();
                let value = receiver.recv().await.unwrap();
                let after = current_executor_shard();
                (before, after, value)
            })
            .unwrap();

        let sender_task = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                yield_now().await;
                sender.try_send(42).unwrap();
            })
            .unwrap();

        block_on(sender_task).unwrap();
        let (before, after, value) = block_on(receiver).unwrap();

        assert_eq!(before, Some(ShardId(1)));
        assert_eq!(after, Some(ShardId(1)));
        assert_eq!(value, 42);

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn all_shards_can_send_to_all_shards() {
        let runtime = ShardedExecutor::start(3).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<(ShardId, usize)>::with_shard_count(3, ShardMailboxConfig::new(16))
                .unwrap(),
        );
        let mut receiver_handles = Vec::new();

        for shard_idx in 0..3 {
            let receiver_mailboxes = Arc::clone(&mailboxes);
            receiver_handles.push(
                runtime
                    .spawn_with_handle_on(ShardId(shard_idx), async move {
                        let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                        let mut seen = Vec::new();
                        for _ in 0..3 {
                            seen.push(receiver.recv().await.unwrap());
                        }
                        seen.sort_by_key(|(from, value)| (from.0, *value));
                        (current_executor_shard().unwrap(), seen)
                    })
                    .unwrap(),
            );
        }

        let mut sender_handles = Vec::new();
        for from_idx in 0..3 {
            let sender_mailboxes = Arc::clone(&mailboxes);
            sender_handles.push(
                runtime
                    .spawn_with_handle_on(ShardId(from_idx), async move {
                        for target_idx in 0..3 {
                            let sender = sender_mailboxes.sender_to(ShardId(target_idx)).unwrap();
                            sender.try_send((ShardId(from_idx), target_idx)).unwrap();
                        }
                    })
                    .unwrap(),
            );
        }

        for handle in sender_handles {
            block_on(handle).unwrap();
        }
        for handle in receiver_handles {
            let (shard_id, seen) = block_on(handle).unwrap();
            assert_eq!(seen.len(), 3);
            assert!(seen.iter().all(|(_, target)| *target == shard_id.0));
        }

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_waits_for_capacity() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        sender.try_send(1).unwrap();
        assert!(matches!(sender.try_send(2), Err(ShardSendError::Full(2))));

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let receiver = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                yield_now().await;
                let a = receiver.recv().await.unwrap();
                let b = receiver.recv().await.unwrap();
                (a, b)
            })
            .unwrap();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                sender.send(2).await.unwrap();
            })
            .unwrap();

        block_on(send_handle).unwrap();
        let (a, b) = block_on(receiver).unwrap();

        assert_eq!(a, 1);
        assert_eq!(b, 2);

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_returns_closed_when_receiver_drops() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(0)).unwrap();
        sender.try_send(1).unwrap();

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let closer = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                let receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                yield_now().await;
                drop(receiver);
            })
            .unwrap();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(1), async move { sender.send(2).await })
            .unwrap();

        block_on(closer).unwrap();
        let result = block_on(send_handle).unwrap();
        assert!(matches!(result, Err(ShardSendError::Closed(2))));

        drop(mailboxes);
        runtime.stop().unwrap();
    }

    #[test]
    fn send_await_snapshot_tracks_waiters() {
        let runtime = ShardedExecutor::start(2).unwrap();
        let mailboxes = Arc::new(
            ShardMailboxSet::<usize>::with_shard_count(2, ShardMailboxConfig::new(1)).unwrap(),
        );
        let sender = mailboxes.sender_to(ShardId(1)).unwrap();
        sender.try_send(1).unwrap();

        let receiver_mailboxes = Arc::clone(&mailboxes);
        let (pending_sender, pending_receiver) = mpsc::channel();

        let send_handle = runtime
            .spawn_with_handle_on(ShardId(0), async move {
                let mut pending_sender = Some(pending_sender);
                let mut send = Box::pin(sender.send(2));
                poll_fn(|context| match send.as_mut().poll(context) {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        if let Some(pending_sender) = pending_sender.take() {
                            pending_sender.send(()).unwrap();
                        }
                        Poll::Pending
                    }
                })
                .await
                .unwrap();
            })
            .unwrap();

        pending_receiver.recv().unwrap();
        let waiters_before_drain = mailboxes.snapshot(ShardId(1)).unwrap().send_waiter_count;

        let snap_handle = runtime
            .spawn_with_handle_on(ShardId(1), async move {
                let mut receiver = receiver_mailboxes.receiver_for_current_shard().unwrap();
                receiver.recv().await.unwrap();
                receiver.recv().await.unwrap();
            })
            .unwrap();

        block_on(send_handle).unwrap();
        block_on(snap_handle).unwrap();

        assert!(waiters_before_drain >= 1);

        drop(mailboxes);
        runtime.stop().unwrap();
    }
}
