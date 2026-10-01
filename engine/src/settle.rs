//! Settlement matching: join operator CSV rows back to gateway sagas.
//!
//! Invariant: the generator (`gateway/cmd/seed`, Task 8) owns the CSV shape —
//! no quoted commas — so parsing is a plain split, not a csv crate. Any row
//! that breaks the shape counts `csv_corrupt` and never a match.

use serde::Serialize;
use std::collections::{HashMap, HashSet};

/// Fee tolerance in minor units: |row.amount + row.fee - saga.amount| <= TOL.
pub const TOL_CENTS: i64 = 100;
/// Timestamp skew window in nanoseconds (5 minutes).
pub const SKEW_NS: u64 = 5 * 60 * 1_000_000_000;

#[derive(Debug, Clone)]
pub struct SagaSnap {
    pub saga_id: String,
    pub txid: String,
    pub amount: i64,
    pub updated_at_ns: u64,
}

#[derive(Debug, Clone)]
pub struct SettlementRow {
    // operator/from/to are row identity from the CSV shape; matching keys on
    // txid+amount, but reports and future tolerance rules read these.
    #[allow(dead_code)]
    pub operator: String,
    pub txid: String,
    #[allow(dead_code)]
    pub from: String,
    #[allow(dead_code)]
    pub to: String,
    pub amount: i64,
    pub fee: i64,
    pub ts: u64,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct MatchReport {
    pub settled: u64,
    pub tolerated: u64,
    pub missing: Vec<String>,
    pub orphan: Vec<String>,
    pub mismatch: Vec<String>,
    pub csv_corrupt: u64,
}

/// Outcome of one settlement sweep: counts plus per-saga status notes
/// (`(saga_id, note)`) the caller pushes into saga steps.
pub struct MatchOutcome {
    pub report: MatchReport,
    pub notes: Vec<(String, String)>,
}

/// Parse settlement CSV: `operator,txid,from,to,amount,fee,ts` per line.
/// A header line starting with `operator` and blank lines are skipped;
/// anything else malformed counts corrupt.
pub fn parse_csv(text: &str) -> (Vec<SettlementRow>, u64) {
    let mut rows = Vec::new();
    let mut corrupt = 0u64;
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        if i == 0 && line.starts_with("operator") {
            continue;
        }
        let f: Vec<&str> = line.split(',').collect();
        if f.len() != 7 {
            corrupt += 1;
            continue;
        }
        let row = (|| -> Option<SettlementRow> {
            Some(SettlementRow {
                operator: f[0].to_string(),
                txid: f[1].to_string(),
                from: f[2].to_string(),
                to: f[3].to_string(),
                amount: f[4].parse().ok()?,
                fee: f[5].parse().ok()?,
                ts: f[6].parse().ok()?,
            })
        })();
        match row {
            Some(r) if !r.txid.is_empty() => rows.push(r),
            _ => corrupt += 1,
        }
    }
    (rows, corrupt)
}

pub fn match_all(snaps: &[SagaSnap], rows: &[SettlementRow]) -> MatchOutcome {
    let by_txid: HashMap<&str, &SagaSnap> = snaps
        .iter()
        .filter(|s| !s.txid.is_empty())
        .map(|s| (s.txid.as_str(), s))
        .collect();
    let mut matched: HashSet<&str> = HashSet::new(); // saga_ids
    let mut seen_rows: HashSet<&str> = HashSet::new();
    let mut report = MatchReport::default();
    let mut notes = Vec::new();

    for row in rows {
        if !seen_rows.insert(row.txid.as_str()) {
            report.mismatch.push(row.txid.clone());
            continue; // duplicate row for an already-judged txid
        }
        match by_txid.get(row.txid.as_str()) {
            None => report.orphan.push(row.txid.clone()),
            Some(s) => {
                matched.insert(s.saga_id.as_str());
                if row.amount == s.amount {
                    report.settled += 1;
                    notes.push((s.saga_id.clone(), format!("matched:exact {}", row.txid)));
                } else if (row.amount + row.fee - s.amount).abs() <= TOL_CENTS
                    && row.ts.abs_diff(s.updated_at_ns) <= SKEW_NS
                {
                    report.tolerated += 1;
                    notes.push((
                        s.saga_id.clone(),
                        format!("matched:tolerance fee-skew {}", row.txid),
                    ));
                } else {
                    report.mismatch.push(row.txid.clone());
                    notes.push((
                        s.saga_id.clone(),
                        format!("discrepancy:mismatch {}", row.txid),
                    ));
                }
            }
        }
    }
    for s in snaps {
        if s.txid.is_empty() || matched.contains(s.saga_id.as_str()) {
            continue;
        }
        report.missing.push(s.saga_id.clone());
        notes.push((s.saga_id.clone(), format!("discrepancy:missing {}", s.txid)));
    }
    MatchOutcome { report, notes }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(id: &str, txid: &str, amount: i64) -> SagaSnap {
        SagaSnap {
            saga_id: id.into(),
            txid: txid.into(),
            amount,
            updated_at_ns: 1_700_000_000_000_000_000,
        }
    }

    fn row(txid: &str, amount: i64, fee: i64) -> SettlementRow {
        SettlementRow {
            operator: "mvola".into(),
            txid: txid.into(),
            from: "alice".into(),
            to: "bob".into(),
            amount,
            fee,
            ts: 1_700_000_000_000_000_000,
        }
    }

    #[test]
    fn exact_tolerance_and_classes() {
        let snaps = vec![
            snap("s1", "t1", 10_000),
            snap("s2", "t2", 5_000),
            snap("s3", "t3", 7_000),
            snap("s4", "t4", 9_000),
        ];
        let rows = vec![
            row("t1", 10_000, 0),   // exact
            row("t2", 4_950, 50),   // fee-shifted within tol
            row("nope", 1, 0),      // orphan
            row("t3", 1, 0),        // mismatch
        ];
        let out = match_all(&snaps, &rows);
        assert_eq!(out.report.settled, 1);
        assert_eq!(out.report.tolerated, 1);
        assert_eq!(out.report.orphan, vec!["nope".to_string()]);
        assert_eq!(out.report.mismatch, vec!["t3".to_string()]);
        assert_eq!(out.report.missing, vec!["s4".to_string()]);
        assert_eq!(out.notes.len(), 4); // exact, tolerance, mismatch, missing
    }

    #[test]
    fn csv_shape_and_corrupt_count() {
        let text = "operator,txid,from,to,amount,fee,ts\nmvola,t1,alice,bob,100,0,1700000000000000000\nbroken,line\nmvola,,x,y,1,0,2\n";
        let (rows, corrupt) = parse_csv(text);
        assert_eq!(rows.len(), 1);
        assert_eq!(corrupt, 2);
    }
}
