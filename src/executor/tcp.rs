#![cfg(unix)]

use std::future::Future;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::{
    JoinError, JoinHandle, Notify, RaceOutput, SchedulingGroup, SpawnError, Spawner, StopToken,
    TaskScope, TaskScopeError, TimeoutError, accept_async, accept_timeout_async, race, timeout,
};

/// Accepts `connection_count` TCP connections and spawns one handler task for
/// each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Handler
/// futures run concurrently on `spawner`; this future waits for all spawned
/// handlers before returning.
pub async fn serve_tcp_n<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    connection_count: usize,
    handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_n_with(
        listener,
        spawner,
        None,
        connection_count,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts `connection_count` TCP connections, then gives handler tasks up to
/// `shutdown_timeout` to finish.
///
/// If the shutdown timeout elapses, still-running handler tasks are aborted and
/// this future returns `io::ErrorKind::TimedOut`.
pub async fn serve_tcp_n_timeout<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    connection_count: usize,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_n_with(
        listener,
        spawner,
        None,
        connection_count,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts `connection_count` TCP connections and spawns handlers into
/// `group`.
pub async fn serve_tcp_n_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    connection_count: usize,
    handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_n_with(
        listener,
        spawner,
        Some(group),
        connection_count,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts `connection_count` TCP connections, spawning handlers into `group`,
/// then gives those handlers up to `shutdown_timeout` to finish.
pub async fn serve_tcp_n_timeout_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    connection_count: usize,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_n_with(
        listener,
        spawner,
        Some(group),
        connection_count,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts TCP connections until `idle_timeout` elapses and spawns handlers
/// into `group`.
pub async fn serve_tcp_until_idle_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    idle_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_idle_with(
        listener,
        spawner,
        Some(group),
        idle_timeout,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `idle_timeout` elapses, spawning handlers
/// into `group`, then gives those handlers up to `shutdown_timeout` to finish.
pub async fn serve_tcp_until_idle_timeout_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_idle_with(
        listener,
        spawner,
        Some(group),
        idle_timeout,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts TCP connections until `idle_timeout` elapses without a new
/// connection, spawning one handler task for each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Handler
/// futures run concurrently on `spawner`; this future waits for all spawned
/// handlers before returning the number of accepted connections.
pub async fn serve_tcp_until_idle<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    idle_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_idle_with(
        listener,
        spawner,
        None,
        idle_timeout,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `idle_timeout` elapses without a new
/// connection, then gives handler tasks up to `shutdown_timeout` to finish.
///
/// If the shutdown timeout elapses, still-running handler tasks are aborted and
/// this future returns `io::ErrorKind::TimedOut`.
pub async fn serve_tcp_until_idle_timeout<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    idle_timeout: Duration,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_idle_with(
        listener,
        spawner,
        None,
        idle_timeout,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, spawning one handler task
/// for each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Handler
/// futures run concurrently on `spawner`; this future waits for all spawned
/// handlers before returning the number of accepted connections.
pub async fn serve_tcp_until_stopped<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    stop: StopToken,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_with(
        listener,
        spawner,
        None,
        stop,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, then gives handler tasks up
/// to `shutdown_timeout` to finish.
///
/// If the shutdown timeout elapses, still-running handler tasks are aborted and
/// this future returns `io::ErrorKind::TimedOut`. Unlike
/// [`serve_tcp_until_stopped_scoped_timeout`], this helper does not pass a stop
/// token to handlers; it only bounds how long shutdown can wait after the
/// accept loop has stopped.
pub async fn serve_tcp_until_stopped_timeout<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    stop: StopToken,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_with(
        listener,
        spawner,
        None,
        stop,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes and spawns handlers into
/// `group`.
pub async fn serve_tcp_until_stopped_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    stop: StopToken,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_with(
        listener,
        spawner,
        Some(group),
        stop,
        TcpHandlerShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, spawning handlers into
/// `group`, then gives those handlers up to `shutdown_timeout` to finish.
pub async fn serve_tcp_until_stopped_timeout_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    stop: StopToken,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_with(
        listener,
        spawner,
        Some(group),
        stop,
        TcpHandlerShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum TcpHandlerShutdown {
    Wait,
    Timeout(Duration),
}

async fn serve_tcp_n_with<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: Option<&SchedulingGroup>,
    connection_count: usize,
    shutdown: TcpHandlerShutdown,
    mut handler: H,
) -> io::Result<()>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::with_capacity(connection_count);

    for _ in 0..connection_count {
        let (stream, peer) = accept_async(&listener).await?;
        handlers.push(spawn_tcp_handler(&spawner, group, handler(stream, peer))?);
    }

    join_tcp_handlers_with(handlers, shutdown).await
}

async fn serve_tcp_until_idle_with<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: Option<&SchedulingGroup>,
    idle_timeout: Duration,
    shutdown: TcpHandlerShutdown,
    mut handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::new();

    loop {
        match accept_timeout_async(&listener, idle_timeout).await {
            Ok((stream, peer)) => {
                handlers.push(spawn_tcp_handler(&spawner, group, handler(stream, peer))?);
            }
            Err(error) if error.kind() == io::ErrorKind::TimedOut => break,
            Err(error) => return Err(error),
        }
    }

    let accepted = handlers.len();
    join_tcp_handlers_with(handlers, shutdown).await?;

    Ok(accepted)
}

async fn serve_tcp_until_stopped_with<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: Option<&SchedulingGroup>,
    stop: StopToken,
    shutdown: TcpHandlerShutdown,
    mut handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = Vec::new();

    loop {
        match race(accept_async(&listener), stop.clone()).await {
            RaceOutput::First(Ok((stream, peer))) => {
                handlers.push(spawn_tcp_handler(&spawner, group, handler(stream, peer))?);
            }
            RaceOutput::First(Err(error)) => return Err(error),
            RaceOutput::Second(()) => break,
        }
    }

    let accepted = handlers.len();
    join_tcp_handlers_with(handlers, shutdown).await?;

    Ok(accepted)
}

/// Accepts TCP connections until `stop` completes, spawning one stop-aware
/// handler task for each accepted stream.
///
/// The listener is placed in non-blocking mode before serving starts. Once
/// `stop` completes, the accept loop stops, all handler tasks receive a shared
/// scope stop token, and this future waits for them before returning the number
/// of accepted connections.
pub async fn serve_tcp_until_stopped_scoped<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    stop: StopToken,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr, StopToken) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_scoped_with(
        listener,
        spawner,
        None,
        stop,
        ScopedTcpShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, then gives handler tasks up
/// to `shutdown_timeout` to finish after receiving their shared stop token.
///
/// If the shutdown timeout elapses, still-running handler tasks are aborted and
/// this future returns `io::ErrorKind::TimedOut`.
pub async fn serve_tcp_until_stopped_scoped_timeout<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    stop: StopToken,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr, StopToken) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_scoped_with(
        listener,
        spawner,
        None,
        stop,
        ScopedTcpShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, spawning stop-aware handler
/// tasks into `group`.
pub async fn serve_tcp_until_stopped_scoped_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    stop: StopToken,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr, StopToken) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_scoped_with(
        listener,
        spawner,
        Some(group),
        stop,
        ScopedTcpShutdown::Wait,
        handler,
    )
    .await
}

/// Accepts TCP connections until `stop` completes, spawning stop-aware handler
/// tasks into `group`, then gives those handlers up to `shutdown_timeout` to
/// finish after receiving their shared stop token.
pub async fn serve_tcp_until_stopped_scoped_timeout_in_group<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: &SchedulingGroup,
    stop: StopToken,
    shutdown_timeout: Duration,
    handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr, StopToken) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    serve_tcp_until_stopped_scoped_with(
        listener,
        spawner,
        Some(group),
        stop,
        ScopedTcpShutdown::Timeout(shutdown_timeout),
        handler,
    )
    .await
}

#[derive(Debug, Clone, Copy)]
enum ScopedTcpShutdown {
    Wait,
    Timeout(Duration),
}

async fn serve_tcp_until_stopped_scoped_with<H, F>(
    listener: TcpListener,
    spawner: Spawner,
    group: Option<&SchedulingGroup>,
    stop: StopToken,
    shutdown: ScopedTcpShutdown,
    mut handler: H,
) -> io::Result<usize>
where
    H: FnMut(TcpStream, SocketAddr, StopToken) -> F,
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    listener.set_nonblocking(true)?;
    let mut handlers = TaskScope::new(spawner);
    let handler_error = Arc::new(Mutex::new(None));
    let handler_error_notify = Notify::new();
    let mut accepted = 0usize;

    loop {
        match race(
            accept_async(&listener),
            race(stop.clone(), handler_error_notify.notified()),
        )
        .await
        {
            RaceOutput::First(Ok((stream, peer))) => {
                let handler_error = Arc::clone(&handler_error);
                let handler_error_notify = handler_error_notify.clone();
                let future = {
                    let future = handler(stream, peer, handlers.stop_token());
                    async move {
                        if let Err(error) = future.await {
                            let mut stored = handler_error
                                .lock()
                                .expect("TCP handler error mutex poisoned");
                            if stored.is_none() {
                                *stored = Some(error);
                            }
                            handler_error_notify.notify_waiters();
                        }
                    }
                };

                match group {
                    Some(group) => handlers.spawn_in_group(group, future),
                    None => handlers.spawn(future),
                }
                .map_err(spawn_error_to_io)?;
                accepted += 1;
            }
            RaceOutput::First(Err(error)) => return Err(error),
            RaceOutput::Second(_) => break,
        }
    }

    let shutdown_result = match shutdown {
        ScopedTcpShutdown::Wait => handlers.shutdown().await.map_err(join_error_to_io),
        ScopedTcpShutdown::Timeout(duration) => handlers
            .shutdown_timeout(duration)
            .await
            .map_err(task_scope_error_to_io),
    };
    let handler_error = handler_error
        .lock()
        .expect("TCP handler error mutex poisoned")
        .take();

    match (shutdown, shutdown_result, handler_error) {
        (ScopedTcpShutdown::Timeout(_), _, Some(error)) => Err(error),
        (_, Err(error), _) => Err(error),
        (_, Ok(()), Some(error)) => Err(error),
        (_, Ok(()), None) => Ok(accepted),
    }
}

fn spawn_tcp_handler<F>(
    spawner: &Spawner,
    group: Option<&SchedulingGroup>,
    future: F,
) -> io::Result<JoinHandle<io::Result<()>>>
where
    F: Future<Output = io::Result<()>> + Send + 'static,
{
    match group {
        Some(group) => spawner.spawn_with_handle_in_group(group, future),
        None => spawner.spawn_with_handle(future),
    }
    .map_err(spawn_error_to_io)
}

async fn join_tcp_handlers_with(
    handlers: Vec<JoinHandle<io::Result<()>>>,
    shutdown: TcpHandlerShutdown,
) -> io::Result<()> {
    match shutdown {
        TcpHandlerShutdown::Wait => join_tcp_handlers_unbounded(handlers).await,
        TcpHandlerShutdown::Timeout(duration) => {
            join_tcp_handlers_timeout(handlers, duration).await
        }
    }
}

async fn join_tcp_handlers_unbounded(handlers: Vec<JoinHandle<io::Result<()>>>) -> io::Result<()> {
    for handler in handlers {
        handler.await.map_err(join_error_to_io)??;
    }

    Ok(())
}

async fn join_tcp_handlers_timeout(
    mut handlers: Vec<JoinHandle<io::Result<()>>>,
    duration: Duration,
) -> io::Result<()> {
    let deadline = Instant::now() + duration;

    while let Some(mut handler) = handlers.pop() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            handler.abort();
            abort_tcp_handlers(handlers);
            return Err(tcp_shutdown_timeout_io());
        }

        match timeout(remaining, &mut handler).await {
            Ok(Ok(result)) => result?,
            Ok(Err(error)) => return Err(join_error_to_io(error)),
            Err(TimeoutError) => {
                handler.abort();
                abort_tcp_handlers(handlers);
                return Err(tcp_shutdown_timeout_io());
            }
        }
    }

    Ok(())
}

fn abort_tcp_handlers(handlers: Vec<JoinHandle<io::Result<()>>>) {
    for handler in handlers {
        handler.abort();
    }
}

fn spawn_error_to_io(error: SpawnError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error)
}

fn join_error_to_io(error: JoinError) -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, error.to_string())
}

fn tcp_shutdown_timeout_io() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "TCP handler shutdown timed out")
}

fn task_scope_error_to_io(error: TaskScopeError) -> io::Error {
    match error {
        TaskScopeError::Join(error) => join_error_to_io(error),
        TaskScopeError::TimedOut => io::Error::new(io::ErrorKind::TimedOut, error.to_string()),
    }
}
