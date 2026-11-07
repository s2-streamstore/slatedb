//! Tests for [`ManifestRefresh::OnDemand`].

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use rstest::rstest;
use slatedb_common::metrics::{DefaultMetricsRecorder, MetricValue};

use crate::config::{ManifestRefresh, Settings};
use crate::instrumented_object_store::stats::REQUEST_COUNT;
use crate::{CloseReason, Db, ErrorKind, IsolationLevel};

fn settings(refresh: ManifestRefresh, poll_interval: Duration) -> Settings {
    Settings {
        manifest_poll_interval: poll_interval,
        manifest_refresh: refresh,
        compactor_options: None,
        garbage_collector_options: None,
        ..Settings::default()
    }
}

/// Returns the number of object store requests of every kind.
fn requests(recorder: &DefaultMetricsRecorder) -> u64 {
    recorder
        .snapshot()
        .by_name(REQUEST_COUNT)
        .iter()
        .map(|metric| match metric.value {
            MetricValue::Counter(value) => value,
            _ => panic!("expected an object store request counter"),
        })
        .sum()
}

/// Lets background tasks process ready work.
async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}

async fn open(
    path: &str,
    store: Arc<InMemory>,
    refresh: ManifestRefresh,
    poll_interval: Duration,
    recorder: Arc<DefaultMetricsRecorder>,
) -> Db {
    Db::builder(path, store)
        .with_settings(settings(refresh, poll_interval))
        .with_metrics_recorder(recorder)
        .build()
        .await
        .unwrap()
}

#[rstest]
#[case::interval(ManifestRefresh::Interval)]
#[case::on_demand(ManifestRefresh::OnDemand)]
#[tokio::test(start_paused = true)]
async fn idle_writer_polls_only_in_interval_mode(#[case] refresh: ManifestRefresh) {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let db = open(
        "idle-writer",
        Arc::new(InMemory::new()),
        refresh,
        Duration::from_secs(1),
        recorder.clone(),
    )
    .await;
    settle().await;
    let before = requests(&recorder);

    for _ in 0..600 {
        tokio::time::advance(Duration::from_millis(100)).await;
        settle().await;
    }

    let idle_requests = requests(&recorder) - before;
    match refresh {
        ManifestRefresh::Interval => assert!(idle_requests > 0),
        ManifestRefresh::OnDemand => assert_eq!(idle_requests, 0),
    }
    db.close().await.unwrap();
}

/// The read kinds that must refresh a stale manifest.
#[derive(Clone, Copy, Debug)]
enum Read {
    Get,
    Scan,
    SnapshotGet,
    TransactionGet,
}

#[rstest]
#[case::get(Read::Get)]
#[case::scan(Read::Scan)]
#[case::snapshot_get(Read::SnapshotGet)]
#[case::transaction_get(Read::TransactionGet)]
#[tokio::test(start_paused = true)]
async fn stale_read_refreshes_manifest_and_detects_fencing(#[case] read: Read) {
    let store = Arc::new(InMemory::new());
    let path = "stale-read";
    let poll_interval = Duration::from_secs(60);
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let writer = open(
        path,
        store.clone(),
        ManifestRefresh::OnDemand,
        poll_interval,
        recorder,
    )
    .await;
    writer.put(b"key", b"value").await.unwrap();
    let snapshot = writer.snapshot().unwrap();
    let transaction = writer.begin(IsolationLevel::Snapshot).unwrap();

    // A newer writer fences this writer. This writer does not know yet.
    let replacement = Db::builder(path, store)
        .with_settings(settings(ManifestRefresh::Interval, poll_interval))
        .build()
        .await
        .unwrap();

    let read_once = || async {
        match read {
            Read::Get => writer.get(b"key").await,
            Read::Scan => {
                let mut iter = writer.scan(..).await?;
                iter.next().await.map(|kv| kv.map(|kv| kv.value))
            }
            Read::SnapshotGet => snapshot.get(b"key").await,
            Read::TransactionGet => transaction.get(b"key").await,
        }
    };

    // The manifest is recent, so the read uses it without a refresh.
    assert_eq!(
        read_once().await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );

    // The manifest is stale, so the read refreshes it and finds the newer writer.
    tokio::time::advance(poll_interval).await;
    let error = read_once().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Closed(CloseReason::Fenced));

    replacement.close().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn concurrent_stale_reads_share_one_refresh() {
    let recorder = Arc::new(DefaultMetricsRecorder::new());
    let poll_interval = Duration::from_secs(60);
    let db = open(
        "shared-refresh",
        Arc::new(InMemory::new()),
        ManifestRefresh::OnDemand,
        poll_interval,
        recorder.clone(),
    )
    .await;
    db.put(b"key", b"value").await.unwrap();
    settle().await;

    // Measure the requests of one refresh.
    let before = requests(&recorder);
    db.refresh_manifest().await.unwrap();
    let refresh_requests = requests(&recorder) - before;
    assert!(refresh_requests > 0);

    tokio::time::advance(poll_interval).await;
    settle().await;
    let before = requests(&recorder);
    let reads = futures::future::join_all((0..8).map(|_| db.get(b"key"))).await;
    for value in reads {
        assert_eq!(value.unwrap(), Some(Bytes::from_static(b"value")));
    }
    assert_eq!(requests(&recorder) - before, refresh_requests);

    // The manifest is recent again, so the next read makes no requests.
    let before = requests(&recorder);
    db.get(b"key").await.unwrap();
    assert_eq!(requests(&recorder) - before, 0);

    db.close().await.unwrap();
}
