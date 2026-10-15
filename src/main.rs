use std::{env, io, net::SocketAddr, sync::Arc};
use streamforge::{api, broker::Broker};

#[tokio::main]
async fn main() -> io::Result<()> {
    let address: SocketAddr = env::var("STREAMFORGE_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
        .parse()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid STREAMFORGE_ADDR: {error}"),
            )
        })?;
    let data = env::var("STREAMFORGE_DATA").unwrap_or_else(|_| "./data".to_owned());
    let partitions = env::var("STREAMFORGE_PARTITIONS")
        .unwrap_or_else(|_| "4".to_owned())
        .parse()
        .map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid STREAMFORGE_PARTITIONS: {error}"),
            )
        })?;
    let listener = tokio::net::TcpListener::bind(address).await?;
    eprintln!("streamforge listening on {address}");
    axum::serve(
        listener,
        api::router(Arc::new(Broker::open(data, partitions)?)),
    )
    .with_graceful_shutdown(shutdown())
    .await
}

async fn shutdown() {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler")
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! { _ = interrupt => {}, _ = terminate => {} }
}
