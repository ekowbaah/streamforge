use crate::broker::{AppendedRecord, Broker, BrokerRecord};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use serde::{Deserialize, Serialize};
use std::{io, sync::Arc};

pub fn router(broker: Arc<Broker>) -> Router {
    Router::new()
        .route("/healthz", get(health))
        .route("/readyz", get(health))
        .route("/v1/topics/{topic}/records", post(append))
        .route(
            "/v1/topics/{topic}/partitions/{partition}/records",
            get(read),
        )
        .route(
            "/v1/groups/{group}/partitions/{partition}/offset",
            put(commit).get(committed),
        )
        .with_state(broker)
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
}

async fn health() -> Json<Health> {
    Json(Health { status: "ok" })
}

#[derive(Deserialize)]
struct AppendRequest {
    key: String,
    payload: String,
}

async fn append(
    State(broker): State<Arc<Broker>>,
    Path(topic): Path<String>,
    Json(input): Json<AppendRequest>,
) -> Result<(StatusCode, Json<AppendedRecord>), ApiError> {
    let appended = broker.append(&topic, input.key.as_bytes(), input.payload.as_bytes())?;
    Ok((StatusCode::CREATED, Json(appended)))
}

#[derive(Deserialize)]
struct ReadQuery {
    offset: Option<u64>,
    limit: Option<usize>,
}

#[derive(Serialize)]
struct RecordResponse {
    offset: u64,
    payload: String,
}

async fn read(
    State(broker): State<Arc<Broker>>,
    Path((topic, partition)): Path<(String, u32)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<Vec<RecordResponse>>, ApiError> {
    let records = broker.read(
        &topic,
        partition,
        query.offset.unwrap_or(0),
        query.limit.unwrap_or(100),
    )?;
    Ok(Json(records.into_iter().map(record_response).collect()))
}

fn record_response(record: BrokerRecord) -> RecordResponse {
    RecordResponse {
        offset: record.offset,
        payload: String::from_utf8_lossy(&record.payload).into_owned(),
    }
}

#[derive(Deserialize)]
struct CommitRequest {
    next_offset: u64,
}

#[derive(Serialize)]
struct OffsetResponse {
    next_offset: u64,
}

async fn commit(
    State(broker): State<Arc<Broker>>,
    Path((group, partition)): Path<(String, u32)>,
    Json(input): Json<CommitRequest>,
) -> Result<Json<OffsetResponse>, ApiError> {
    broker.commit(&group, partition, input.next_offset)?;
    Ok(Json(OffsetResponse {
        next_offset: input.next_offset,
    }))
}

async fn committed(
    State(broker): State<Arc<Broker>>,
    Path((group, partition)): Path<(String, u32)>,
) -> Result<Json<OffsetResponse>, ApiError> {
    Ok(Json(OffsetResponse {
        next_offset: broker.committed(&group, partition)?,
    }))
}

struct ApiError(io::Error);

impl From<io::Error> for ApiError {
    fn from(value: io::Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self.0.kind() {
            io::ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
            io::ErrorKind::WouldBlock => StatusCode::TOO_MANY_REQUESTS,
            io::ErrorKind::NotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(serde_json::json!({"error": {"code": self.0.kind().to_string(), "message": self.0.to_string()}}))).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    async fn http_api_appends_reads_and_commits_offsets() {
        let root = std::env::temp_dir().join(format!("streamforge-api-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let app = router(Arc::new(Broker::open(&root, 2).unwrap()));
        let append = app
            .clone()
            .oneshot(
                Request::post("/v1/topics/orders/records")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"key":"customer-1","payload":"created"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(append.status(), StatusCode::CREATED);
        let body: Value =
            serde_json::from_slice(&to_bytes(append.into_body(), 16_384).await.unwrap()).unwrap();
        let partition = body["partition"].as_u64().unwrap();

        let read = app
            .clone()
            .oneshot(
                Request::get(format!(
                    "/v1/topics/orders/partitions/{partition}/records?offset=0&limit=10"
                ))
                .body(Body::empty())
                .unwrap(),
            )
            .await
            .unwrap();
        let records: Value =
            serde_json::from_slice(&to_bytes(read.into_body(), 16_384).await.unwrap()).unwrap();
        assert_eq!(records[0]["payload"], "created");

        let commit = app
            .oneshot(
                Request::put(format!("/v1/groups/billing/partitions/{partition}/offset"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"next_offset":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(commit.status(), StatusCode::OK);
        std::fs::remove_dir_all(root).unwrap();
    }
}
