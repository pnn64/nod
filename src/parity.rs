use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use rssp::{AnalysisOptions, analyze};
use serde::Deserialize;

use crate::cli::ParityCmd;
use crate::compat::slot_abbreviation;
use crate::fs_scan::{baseline_rel_for_md5, discover_simfiles, md5_hex, rel_path};
use crate::model::{ParityCase, ParityReport};

pub fn run(args: &ParityCmd) -> Result<ParityReport, String> {
    let simfiles = discover_simfiles(&args.root_path)?;
    let mut cases = Vec::with_capacity(simfiles.len());
    for simfile in simfiles {
        cases.push(check_one(&simfile, &args.root_path, &args.baseline_path));
    }
    Ok(build_report(&args.root_path, &args.baseline_path, cases))
}

fn check_one(path: &Path, root: &Path, baseline_root: &Path) -> ParityCase {
    let simfile_rel = rel_path(root, path);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => return case_read_error(simfile_rel, err.to_string()),
    };
    let simfile_md5 = md5_hex(&bytes);
    let baseline_rel = baseline_rel_for_md5(&simfile_md5);
    let Some((baseline_path, baseline_rel_out)) =
        find_baseline_path(baseline_root, &simfile_md5, &baseline_rel)
    else {
        return ParityCase {
            simfile_rel,
            simfile_md5,
            baseline_rel: Some(baseline_rel),
            status: "missing_baseline".to_string(),
            error: None,
            mismatch_count: 0,
            mismatches: Vec::new(),
        };
    };

    let baseline = match read_baseline(&baseline_path).and_then(parse_baseline) {
        Ok(baseline) => baseline,
        Err(err) => {
            return ParityCase {
                simfile_rel,
                simfile_md5,
                baseline_rel: Some(baseline_rel_out),
                status: "invalid_baseline".to_string(),
                error: Some(err),
                mismatch_count: 0,
                mismatches: Vec::new(),
            };
        }
    };

    let extension = simfile_ext(path);
    let summary = match analyze(&bytes, &extension, &AnalysisOptions::default()) {
        Ok(summary) => summary,
        Err(err) => {
            return ParityCase {
                simfile_rel,
                simfile_md5,
                baseline_rel: Some(baseline_rel_out),
                status: "analyze_error".to_string(),
                error: Some(err),
                mismatch_count: 0,
                mismatches: Vec::new(),
            };
        }
    };

    let mismatches = compare_fixture(&baseline, &summary);
    let mismatch_count = mismatches.len();
    let status = if mismatch_count == 0 {
        "matched"
    } else {
        "mismatch"
    };
    ParityCase {
        simfile_rel,
        simfile_md5,
        baseline_rel: Some(baseline_rel_out),
        status: status.to_string(),
        error: None,
        mismatch_count,
        mismatches,
    }
}

fn find_baseline_path(root: &Path, md5: &str, base_rel: &str) -> Option<(PathBuf, String)> {
    let (candidate_json, candidate_zst) = baseline_candidates(root, md5);
    if candidate_json.exists() {
        Some((candidate_json, base_rel.to_string()))
    } else if candidate_zst.exists() {
        Some((candidate_zst, format!("{base_rel}.zst")))
    } else {
        None
    }
}

fn baseline_candidates(root: &Path, md5: &str) -> (PathBuf, PathBuf) {
    let prefix = md5.get(0..2).unwrap_or("00");
    let shard = root.join(prefix);
    (
        shard.join(format!("{md5}.json")),
        shard.join(format!("{md5}.json.zst")),
    )
}

fn read_baseline(path: &Path) -> Result<Vec<u8>, String> {
    let raw =
        fs::read(path).map_err(|e| format!("read baseline {} failed: {e}", path.display()))?;
    let is_zst = path
        .extension()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("zst"));
    if is_zst {
        zstd::stream::decode_all(Cursor::new(raw))
            .map_err(|e| format!("zstd decode {} failed: {e}", path.display()))
    } else {
        Ok(raw)
    }
}

fn parse_baseline(bytes: Vec<u8>) -> Result<BaselineFixture, String> {
    serde_json::from_slice::<BaselineFixture>(&bytes)
        .map_err(|e| format!("baseline json parse failed: {e}"))
}

fn compare_fixture(baseline: &BaselineFixture, summary: &rssp::SimfileSummary) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_text(
        "title",
        &baseline.title,
        &summary.title_str,
        &mut mismatches,
    );
    compare_text(
        "subtitle",
        &baseline.subtitle,
        &summary.subtitle_str,
        &mut mismatches,
    );
    compare_text(
        "artist",
        &baseline.artist,
        &summary.artist_str,
        &mut mismatches,
    );
    compare_text(
        "music",
        &baseline.music,
        &summary.music_path,
        &mut mismatches,
    );
    compare_float_opt(
        "offset",
        baseline.offset,
        summary.offset,
        1e-6,
        &mut mismatches,
    );
    compare_chart_rows(&baseline.charts, &expected_charts(summary), &mut mismatches);
    mismatches
}

fn compare_chart_rows(
    baseline_rows: &[BaselineChart],
    expected_rows: &[ExpectedChart],
    mismatches: &mut Vec<String>,
) {
    if baseline_rows.len() != expected_rows.len() {
        mismatches.push(format!(
            "charts.count mismatch: baseline={} expected={}",
            baseline_rows.len(),
            expected_rows.len()
        ));
    }

    let base_map = build_baseline_map(baseline_rows, mismatches);
    let expected_map = build_expected_map(expected_rows, mismatches);

    for (chart_index, expected) in &expected_map {
        let Some(baseline) = base_map.get(chart_index) else {
            mismatches.push(format!(
                "chart[{chart_index:?}] missing in baseline (expected row exists)"
            ));
            continue;
        };
        compare_chart_row(*chart_index, baseline, expected, mismatches);
    }

    for chart_index in base_map.keys() {
        if !expected_map.contains_key(chart_index) {
            mismatches.push(format!(
                "chart[{chart_index:?}] is extra in baseline (no expected row)"
            ));
        }
    }
}

fn build_baseline_map<'a>(
    rows: &'a [BaselineChart],
    mismatches: &mut Vec<String>,
) -> BTreeMap<Option<usize>, &'a BaselineChart> {
    let mut map = BTreeMap::new();
    for row in rows {
        if map.insert(row.chart_index, row).is_some() {
            mismatches.push(format!(
                "duplicate baseline chart_index entry: {:?}",
                row.chart_index
            ));
        }
    }
    map
}

fn build_expected_map<'a>(
    rows: &'a [ExpectedChart],
    mismatches: &mut Vec<String>,
) -> BTreeMap<Option<usize>, &'a ExpectedChart> {
    let mut map = BTreeMap::new();
    for row in rows {
        if map.insert(row.chart_index, row).is_some() {
            mismatches.push(format!(
                "duplicate expected chart_index entry: {:?}",
                row.chart_index
            ));
        }
    }
    map
}

fn compare_chart_row(
    chart_index: Option<usize>,
    baseline: &BaselineChart,
    expected: &ExpectedChart,
    mismatches: &mut Vec<String>,
) {
    if baseline.slot_null != expected.slot_null {
        mismatches.push(format!(
            "chart[{chart_index:?}].slot_null mismatch: baseline={} expected={}",
            baseline.slot_null, expected.slot_null
        ));
    }
    if baseline.slot_p9ms != expected.slot_p9ms {
        mismatches.push(format!(
            "chart[{chart_index:?}].slot_p9ms mismatch: baseline={} expected={}",
            baseline.slot_p9ms, expected.slot_p9ms
        ));
    }
    if baseline.slot != expected.slot_null && baseline.slot != expected.slot_p9ms {
        mismatches.push(format!(
            "chart[{chart_index:?}].slot mismatch: baseline={} expected one of [{}, {}]",
            baseline.slot, expected.slot_null, expected.slot_p9ms
        ));
    }
    compare_opt_text(
        &format!("chart[{chart_index:?}].steps_type"),
        baseline.steps_type.as_deref(),
        expected.steps_type.as_deref(),
        mismatches,
    );
    compare_opt_text(
        &format!("chart[{chart_index:?}].difficulty"),
        baseline.difficulty.as_deref(),
        expected.difficulty.as_deref(),
        mismatches,
    );
    compare_opt_text(
        &format!("chart[{chart_index:?}].description"),
        baseline.description.as_deref(),
        expected.description.as_deref(),
        mismatches,
    );
    if baseline.chart_has_own_timing != expected.chart_has_own_timing {
        mismatches.push(format!(
            "chart[{chart_index:?}].chart_has_own_timing mismatch: baseline={} expected={}",
            baseline.chart_has_own_timing, expected.chart_has_own_timing
        ));
    }
}

fn expected_charts(summary: &rssp::SimfileSummary) -> Vec<ExpectedChart> {
    let mut rows = Vec::with_capacity(1 + summary.charts.len());
    rows.push(ExpectedChart {
        chart_index: None,
        slot_null: "*".to_string(),
        slot_p9ms: "*".to_string(),
        steps_type: None,
        difficulty: None,
        description: None,
        chart_has_own_timing: false,
    });
    for (chart_index, chart) in summary.charts.iter().enumerate() {
        if !chart.chart_has_own_timing {
            continue;
        }
        rows.push(ExpectedChart {
            chart_index: Some(chart_index),
            slot_null: slot_abbreviation(
                &chart.step_type_str,
                &chart.difficulty_str,
                chart_index,
                "null",
            ),
            slot_p9ms: slot_abbreviation(
                &chart.step_type_str,
                &chart.difficulty_str,
                chart_index,
                "+9ms",
            ),
            steps_type: Some(chart.step_type_str.clone()),
            difficulty: Some(chart.difficulty_str.clone()),
            description: Some(chart.description_str.clone()),
            chart_has_own_timing: true,
        });
    }
    rows.sort_by_key(|row| (row.chart_index.is_some(), row.chart_index.unwrap_or(0)));
    rows
}

fn compare_text(field: &str, baseline: &str, expected: &str, mismatches: &mut Vec<String>) {
    if baseline != expected {
        mismatches.push(format!(
            "{field} mismatch: baseline={:?} expected={:?}",
            baseline, expected
        ));
    }
}

fn compare_opt_text(
    field: &str,
    baseline: Option<&str>,
    expected: Option<&str>,
    mismatches: &mut Vec<String>,
) {
    let base = normalize_opt_text(baseline);
    let exp = normalize_opt_text(expected);
    if base != exp {
        mismatches.push(format!(
            "{field} mismatch: baseline={:?} expected={:?}",
            base, exp
        ));
    }
}

fn normalize_opt_text(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|s| !s.is_empty())
}

fn compare_float_opt(
    field: &str,
    baseline: Option<f64>,
    expected: f64,
    tolerance: f64,
    mismatches: &mut Vec<String>,
) {
    let Some(base) = baseline else {
        mismatches.push(format!("{field} missing in baseline"));
        return;
    };
    if (base - expected).abs() > tolerance {
        mismatches.push(format!(
            "{field} mismatch: baseline={base:.6} expected={expected:.6} tolerance={tolerance:.6}"
        ));
    }
}

fn case_read_error(simfile_rel: String, err: String) -> ParityCase {
    ParityCase {
        simfile_rel,
        simfile_md5: String::new(),
        baseline_rel: None,
        status: "read_error".to_string(),
        error: Some(format!("read simfile failed: {err}")),
        mismatch_count: 0,
        mismatches: Vec::new(),
    }
}

fn build_report(root: &Path, baseline: &Path, cases: Vec<ParityCase>) -> ParityReport {
    let total = cases.len();
    let matched = count_status(&cases, "matched");
    let mismatched = count_status(&cases, "mismatch");
    let missing = count_status(&cases, "missing_baseline");
    let invalid = count_status(&cases, "invalid_baseline");
    let read_errors = count_status(&cases, "read_error");
    let analyze_errors = count_status(&cases, "analyze_error");
    ParityReport {
        tool: "rnon".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        mode: "structural-parity".to_string(),
        root_path: root.display().to_string(),
        baseline_path: baseline.display().to_string(),
        total_simfiles: total,
        matched,
        mismatched,
        missing_baseline: missing,
        invalid_baseline: invalid,
        read_errors,
        analyze_errors,
        cases,
    }
}

fn count_status(cases: &[ParityCase], status: &str) -> usize {
    cases.iter().filter(|c| c.status == status).count()
}

fn simfile_ext(path: &Path) -> String {
    path.extension()
        .and_then(|s| s.to_str())
        .map_or_else(String::new, |s| s.to_ascii_lowercase())
}

#[derive(Debug, Deserialize)]
struct BaselineFixture {
    #[serde(default)]
    title: String,
    #[serde(default)]
    subtitle: String,
    #[serde(default)]
    artist: String,
    #[serde(default)]
    music: String,
    offset: Option<f64>,
    #[serde(default)]
    charts: Vec<BaselineChart>,
}

#[derive(Debug, Deserialize)]
struct BaselineChart {
    chart_index: Option<usize>,
    #[serde(default)]
    slot: String,
    #[serde(default)]
    slot_null: String,
    #[serde(default)]
    slot_p9ms: String,
    #[serde(default)]
    steps_type: Option<String>,
    #[serde(default)]
    difficulty: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    chart_has_own_timing: bool,
}

#[derive(Debug)]
struct ExpectedChart {
    chart_index: Option<usize>,
    slot_null: String,
    slot_p9ms: String,
    steps_type: Option<String>,
    difficulty: Option<String>,
    description: Option<String>,
    chart_has_own_timing: bool,
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use crate::cli::ParityCmd;
    use crate::fs_scan::md5_hex;

    use super::{expected_charts, run};

    #[test]
    fn parity_matches_existing_baseline_file() {
        let temp = temp_root("parity-pass");
        let root = temp.join("packs");
        let song = root.join("PackA").join("SongA");
        fs::create_dir_all(&song).expect("mkdir song");
        let simfile = song.join("chart.sm");
        let bytes = b"#TITLE:Test;\n#ARTIST:Rust;\n#MUSIC:test.ogg;\n#OFFSET:0.000;\n#BPMS:0.000=120.000;\n#NOTES:dance-single:desc:Easy:1:0,0,0,0:0000\n;";
        fs::write(&simfile, bytes).expect("write simfile");

        let md5 = md5_hex(bytes);
        let baseline = temp.join("baseline");
        let shard = baseline.join(&md5[0..2]);
        fs::create_dir_all(&shard).expect("mkdir shard");
        let fixture = matching_fixture(bytes);
        fs::write(
            shard.join(format!("{md5}.json")),
            serde_json::to_vec(&fixture).expect("serialize fixture"),
        )
        .expect("write baseline");

        let args = ParityCmd {
            root_path: PathBuf::from(&root),
            baseline_path: PathBuf::from(&baseline),
            output: None,
            fail_on_missing: true,
            fail_on_mismatch: true,
        };
        let report = run(&args).expect("run parity");
        assert_eq!(report.total_simfiles, 1);
        assert_eq!(report.matched, 1);
        assert_eq!(report.mismatched, 0);
        assert_eq!(report.missing_baseline, 0);
        assert_eq!(report.invalid_baseline, 0);
        assert_eq!(report.analyze_errors, 0);
        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn parity_reports_structural_mismatch() {
        let temp = temp_root("parity-mismatch");
        let root = temp.join("packs");
        let song = root.join("PackA").join("SongA");
        fs::create_dir_all(&song).expect("mkdir song");
        let simfile = song.join("chart.sm");
        let bytes = b"#TITLE:Test;\n#ARTIST:Rust;\n#MUSIC:test.ogg;\n#OFFSET:0.000;\n#BPMS:0.000=120.000;\n#NOTES:dance-single:desc:Easy:1:0,0,0,0:0000\n;";
        fs::write(&simfile, bytes).expect("write simfile");

        let md5 = md5_hex(bytes);
        let baseline = temp.join("baseline");
        let shard = baseline.join(&md5[0..2]);
        fs::create_dir_all(&shard).expect("mkdir shard");
        let mut fixture = matching_fixture(bytes);
        fixture["music"] = json!("wrong.ogg");
        fs::write(
            shard.join(format!("{md5}.json")),
            serde_json::to_vec(&fixture).expect("serialize fixture"),
        )
        .expect("write baseline");

        let args = ParityCmd {
            root_path: root,
            baseline_path: baseline,
            output: None,
            fail_on_missing: false,
            fail_on_mismatch: false,
        };
        let report = run(&args).expect("run parity");
        assert_eq!(report.total_simfiles, 1);
        assert_eq!(report.matched, 0);
        assert_eq!(report.mismatched, 1);
        assert!(report.cases[0].mismatch_count > 0);
        assert!(
            report.cases[0]
                .mismatches
                .iter()
                .any(|m| m.contains("music mismatch"))
        );
        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn parity_reports_missing_baseline() {
        let temp = temp_root("parity-missing");
        let root = temp.join("packs");
        let song = root.join("PackA").join("SongA");
        fs::create_dir_all(&song).expect("mkdir song");
        fs::write(song.join("chart.sm"), "#TITLE:Missing;").expect("write simfile");
        let baseline = temp.join("baseline");
        fs::create_dir_all(&baseline).expect("mkdir baseline");

        let args = ParityCmd {
            root_path: root,
            baseline_path: baseline,
            output: None,
            fail_on_missing: false,
            fail_on_mismatch: false,
        };
        let report = run(&args).expect("run parity");
        assert_eq!(report.total_simfiles, 1);
        assert_eq!(report.matched, 0);
        assert_eq!(report.missing_baseline, 1);
        let _ = fs::remove_dir_all(temp);
    }

    fn matching_fixture(bytes: &[u8]) -> serde_json::Value {
        let summary = rssp::analyze(bytes, "sm", &rssp::AnalysisOptions::default())
            .expect("rssp analyze in fixture builder");
        let charts: Vec<serde_json::Value> = expected_charts(&summary)
            .into_iter()
            .map(|row| {
                let slot = row.slot_null.clone();
                json!({
                    "chart_index": row.chart_index,
                    "slot": slot,
                    "slot_null": row.slot_null,
                    "slot_p9ms": row.slot_p9ms,
                    "steps_type": row.steps_type,
                    "difficulty": row.difficulty,
                    "description": row.description,
                    "chart_has_own_timing": row.chart_has_own_timing,
                })
            })
            .collect();
        json!({
            "title": summary.title_str,
            "subtitle": summary.subtitle_str,
            "artist": summary.artist_str,
            "music": summary.music_path,
            "offset": summary.offset,
            "charts": charts,
        })
    }

    fn temp_root(tag: &str) -> PathBuf {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_millis();
        let path = env::temp_dir().join(format!("rnon-{tag}-{ts}-{}", std::process::id()));
        fs::create_dir_all(&path).expect("mkdir temp root");
        path
    }
}
