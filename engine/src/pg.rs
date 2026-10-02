//! Postgres sink: the queryable source-of-truth behind the WAL.
//!
//! The WAL stays the durability path (replay on crash); PG mirrors saga
//! states for queries and reporting. `NoTls` fits the local compose demo
//! only — any real deployment terminates TLS first (see docs/limitations.md).
//! When PG is unreachable the engine keeps serving from WAL/file truth and
//! the flush task retries; `pg_degraded` surfaces in logs.

use crate::saga::Saga;
use tokio_postgres::{Client, NoTls};

pub struct PgSink {
    client: Client,
}

/// Verdict string stored per saga: honest about what the matcher decided.
pub fn verdict_of(s: &Saga) -> &'static str {
    if s.steps.iter().any(|x| x.starts_with("matched:exact")) {
        return "settled";
    }
    if s.steps.iter().any(|x| x.starts_with("matched:tolerance")) {
        return "tolerated";
    }
    if s.steps.iter().any(|x| x.starts_with("discrepancy:")) {
        return "discrepancy";
    }
    "pending"
}

impl PgSink {
    pub async fn connect(
        url: &str,
    ) -> Result<(Self, tokio::task::JoinHandle<()>), tokio_postgres::Error> {
        let (client, connection) = tokio_postgres::connect(url, NoTls).await?;
        let handle = tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("engine: pg connection error: {e}");
            }
        });
        Ok((Self { client }, handle))
    }

    pub async fn ensure_schema(&self) -> Result<(), tokio_postgres::Error> {
        self.client
            .batch_execute(
                "CREATE TABLE IF NOT EXISTS sagas (
                    saga_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    operator TEXT NOT NULL DEFAULT '',
                    txid TEXT NOT NULL DEFAULT '',
                    amount BIGINT NOT NULL DEFAULT 0,
                    verdict TEXT NOT NULL DEFAULT 'pending',
                    updated_at_ns BIGINT NOT NULL DEFAULT 0
                )",
            )
            .await
    }

    pub async fn upsert_saga(&self, s: &Saga) -> Result<(), tokio_postgres::Error> {
        self.client
            .execute(
                "INSERT INTO sagas (saga_id, state, operator, txid, amount, verdict, updated_at_ns)
                 VALUES ($1,$2,$3,$4,$5,$6,$7)
                 ON CONFLICT (saga_id) DO UPDATE SET
                   state=EXCLUDED.state, operator=EXCLUDED.operator, txid=EXCLUDED.txid,
                   amount=EXCLUDED.amount, verdict=EXCLUDED.verdict,
                   updated_at_ns=EXCLUDED.updated_at_ns",
                &[
                    &s.saga_id,
                    &s.state.to_string(),
                    &s.operator,
                    &s.txid,
                    &s.amount,
                    &verdict_of(s),
                    &(s.updated_at_ns as i64),
                ],
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn pg_round_trip() {
        let url = match std::env::var("DATABASE_URL") {
            Ok(u) if !u.is_empty() => u,
            _ => {
                eprintln!("pg test skipped: DATABASE_URL unset");
                return;
            }
        };
        let (sink, _h) = PgSink::connect(&url).await.expect("connect");
        sink.ensure_schema().await.expect("schema");
        let s = Saga {
            saga_id: "test-saga-1".into(),
            state: crate::saga::SagaState::InProgress,
            steps: vec!["matched:exact t1".into()],
            idempotency_key: "k".into(),
            updated_at_ns: 1,
            operator: "mvola".into(),
            txid: "t1".into(),
            to: "bob".into(),
            amount: 100,
        };
        sink.upsert_saga(&s).await.expect("upsert");
        let row = sink
            .client
            .query_one(
                "SELECT verdict, amount FROM sagas WHERE saga_id='test-saga-1'",
                &[],
            )
            .await
            .expect("select");
        let verdict: String = row.get(0);
        let amount: i64 = row.get(1);
        assert_eq!(verdict, "settled");
        assert_eq!(amount, 100);
        sink.client
            .execute("DELETE FROM sagas WHERE saga_id='test-saga-1'", &[])
            .await
            .unwrap();
    }
}
