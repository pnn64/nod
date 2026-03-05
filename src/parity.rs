use std::collections::BTreeMap;
use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use rssp::{AnalysisOptions, analyze};
use serde::Deserialize;

use crate::audio::decode_ogg_mono_like_python;
use crate::bias::{BiasCfg, estimate_bias};
use crate::cli::ParityCmd;
use crate::compat::{guess_paradigm, slot_abbreviation};
use crate::fs_scan::{baseline_rel_for_md5, discover_simfiles, md5_hex, rel_path};
use crate::model::{BiasKernel, KernelTarget, ParityCase, ParityReport};

const BIAS_MS_TOLERANCE: f64 = 0.25;
const CONFIDENCE_TOLERANCE: f64 = 1e-3;
const CONV_TOLERANCE: f64 = 1e-3;

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

    let bias_rows = compute_expected_bias_rows(path, &summary, &baseline);
    let mismatches = compare_fixture(&baseline, &summary, &bias_rows);
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

fn compare_fixture(
    baseline: &BaselineFixture,
    summary: &rssp::SimfileSummary,
    bias_rows: &BiasRows,
) -> Vec<String> {
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
    compare_chart_rows(
        &baseline.charts,
        &expected_charts(summary),
        bias_rows,
        &mut mismatches,
    );
    mismatches
}

fn compare_chart_rows(
    baseline_rows: &[BaselineChart],
    expected_rows: &[ExpectedChart],
    bias_rows: &BiasRows,
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
        compare_chart_row(*chart_index, baseline, expected, bias_rows, mismatches);
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
    bias_rows: &BiasRows,
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
    compare_chart_bias(chart_index, baseline, bias_rows, mismatches);
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

fn compare_chart_bias(
    chart_index: Option<usize>,
    baseline: &BaselineChart,
    bias_rows: &BiasRows,
    mismatches: &mut Vec<String>,
) {
    if !has_bias_fields(baseline) {
        return;
    }
    if let Some(err) = bias_rows.errors.get(&chart_index) {
        mismatches.push(format!(
            "chart[{chart_index:?}] bias estimation failed: {err}"
        ));
        return;
    }
    let Some(expected) = bias_rows.values.get(&chart_index) else {
        mismatches.push(format!(
            "chart[{chart_index:?}] missing expected bias estimate"
        ));
        return;
    };
    compare_float_if_present(
        &format!("chart[{chart_index:?}].bias_ms"),
        baseline.bias_ms,
        expected.bias_ms,
        BIAS_MS_TOLERANCE,
        mismatches,
    );
    compare_float_if_present(
        &format!("chart[{chart_index:?}].confidence"),
        baseline.confidence,
        expected.confidence,
        CONFIDENCE_TOLERANCE,
        mismatches,
    );
    compare_float_if_present(
        &format!("chart[{chart_index:?}].conv_quint"),
        baseline.conv_quint,
        expected.conv_quint,
        CONV_TOLERANCE,
        mismatches,
    );
    compare_float_if_present(
        &format!("chart[{chart_index:?}].conv_stdev"),
        baseline.conv_stdev,
        expected.conv_stdev,
        CONV_TOLERANCE,
        mismatches,
    );
    if let Some(base) = normalize_opt_text(baseline.paradigm.as_deref()) {
        let exp = normalize_opt_text(Some(expected.paradigm.as_str()));
        if Some(base) != exp {
            mismatches.push(format!(
                "chart[{chart_index:?}].paradigm mismatch: baseline={:?} expected={:?}",
                Some(base),
                exp
            ));
        }
    }
}

fn compare_float_if_present(
    field: &str,
    baseline: Option<f64>,
    expected: f64,
    tolerance: f64,
    mismatches: &mut Vec<String>,
) {
    let Some(base) = baseline else {
        return;
    };
    if (base - expected).abs() > tolerance {
        mismatches.push(format!(
            "{field} mismatch: baseline={base:.6} expected={expected:.6} tolerance={tolerance:.6}"
        ));
    }
}

fn has_bias_fields(row: &BaselineChart) -> bool {
    row.bias_ms.is_some()
        || row.confidence.is_some()
        || row.conv_quint.is_some()
        || row.conv_stdev.is_some()
        || normalize_opt_text(row.paradigm.as_deref()).is_some()
}

fn compute_expected_bias_rows(
    simfile_path: &Path,
    summary: &rssp::SimfileSummary,
    baseline: &BaselineFixture,
) -> BiasRows {
    let mut out = BiasRows::default();
    let requested = baseline
        .charts
        .iter()
        .filter(|row| has_bias_fields(row))
        .map(|row| row.chart_index)
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return out;
    }
    let Some(song_dir) = simfile_path.parent() else {
        assign_bias_error(
            &mut out.errors,
            &requested,
            "simfile has no parent directory",
        );
        return out;
    };
    let Some(audio_path) = rssp::assets::resolve_music_path_like_itg(song_dir, &summary.music_path)
    else {
        assign_bias_error(
            &mut out.errors,
            &requested,
            "could not resolve #MUSIC audio path",
        );
        return out;
    };
    if !is_ogg_path(&audio_path) {
        assign_bias_error(
            &mut out.errors,
            &requested,
            "only OGG audio is currently supported for parity bias checks",
        );
        return out;
    }
    let decoded = match decode_ogg_mono_like_python(&audio_path) {
        Ok(decoded) => decoded,
        Err(err) => {
            assign_bias_error(
                &mut out.errors,
                &requested,
                &format!("audio decode failed: {err}"),
            );
            return out;
        }
    };
    let cfg = match bias_cfg_from_baseline_params(&baseline.params) {
        Ok(cfg) => cfg,
        Err(err) => {
            assign_bias_error(
                &mut out.errors,
                &requested,
                &format!("invalid baseline params: {err}"),
            );
            return out;
        }
    };
    for chart_index in requested {
        let Some(chart) = chart_for_bias(summary, chart_index) else {
            out.errors.insert(
                chart_index,
                "missing chart in rssp summary for baseline row".to_string(),
            );
            continue;
        };
        match estimate_bias(&decoded.mono, decoded.sample_rate_hz, chart, &cfg) {
            Ok(est) => {
                let paradigm = guess_paradigm(
                    est.bias_ms,
                    baseline.params.tolerance,
                    baseline.params.consider_null,
                    baseline.params.consider_p9ms,
                    true,
                )
                .to_string();
                out.values.insert(
                    chart_index,
                    ExpectedBias {
                        bias_ms: est.bias_ms,
                        confidence: est.confidence,
                        conv_quint: est.conv_quint,
                        conv_stdev: est.conv_stdev,
                        paradigm,
                    },
                );
            }
            Err(err) => {
                out.errors.insert(chart_index, err);
            }
        }
    }
    out
}

fn assign_bias_error(
    errors: &mut BTreeMap<Option<usize>, String>,
    keys: &[Option<usize>],
    msg: &str,
) {
    for key in keys {
        errors.insert(*key, msg.to_string());
    }
}

fn chart_for_bias(
    summary: &rssp::SimfileSummary,
    chart_index: Option<usize>,
) -> Option<&rssp::ChartSummary> {
    match chart_index {
        Some(i) => summary.charts.get(i),
        None => summary
            .charts
            .iter()
            .find(|chart| !chart.chart_has_own_timing)
            .or_else(|| summary.charts.first()),
    }
}

fn is_ogg_path(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("ogg"))
}

fn bias_cfg_from_baseline_params(params: &BaselineParams) -> Result<BiasCfg, String> {
    Ok(BiasCfg {
        fingerprint_ms: params.fingerprint_ms,
        window_ms: params.window_ms,
        step_ms: params.step_ms,
        magic_offset_ms: params.magic_offset_ms,
        kernel_target: parse_kernel_target(&params.kernel_target)?,
        kernel_type: parse_kernel_type(&params.kernel_type)?,
        _full_spectrogram: params.full_spectrogram,
    })
}

fn parse_kernel_target(raw: &str) -> Result<KernelTarget, String> {
    match raw.to_ascii_lowercase().as_str() {
        "0" | "digest" => Ok(KernelTarget::Digest),
        "1" | "acc" | "accumulator" => Ok(KernelTarget::Accumulator),
        _ => Err(format!("invalid kernel target: {raw}")),
    }
}

fn parse_kernel_type(raw: &str) -> Result<BiasKernel, String> {
    match raw.to_ascii_lowercase().as_str() {
        "0" | "rising" => Ok(BiasKernel::Rising),
        "1" | "loudest" => Ok(BiasKernel::Loudest),
        _ => Err(format!("invalid kernel type: {raw}")),
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
        mode: "parity".to_string(),
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
    params: BaselineParams,
    #[serde(default)]
    charts: Vec<BaselineChart>,
}

#[derive(Debug, Deserialize)]
struct BaselineParams {
    #[serde(default = "default_true")]
    consider_null: bool,
    #[serde(default = "default_true")]
    consider_p9ms: bool,
    #[serde(default = "default_fingerprint_ms")]
    fingerprint_ms: f64,
    #[serde(default)]
    full_spectrogram: bool,
    #[serde(default = "default_kernel_target")]
    kernel_target: String,
    #[serde(default = "default_kernel_type")]
    kernel_type: String,
    #[serde(default)]
    magic_offset_ms: f64,
    #[serde(default = "default_step_ms")]
    step_ms: f64,
    #[serde(default = "default_tolerance")]
    tolerance: f64,
    #[serde(default = "default_window_ms")]
    window_ms: f64,
}

impl Default for BaselineParams {
    fn default() -> Self {
        Self {
            consider_null: true,
            consider_p9ms: true,
            fingerprint_ms: default_fingerprint_ms(),
            full_spectrogram: false,
            kernel_target: default_kernel_target(),
            kernel_type: default_kernel_type(),
            magic_offset_ms: 0.0,
            step_ms: default_step_ms(),
            tolerance: default_tolerance(),
            window_ms: default_window_ms(),
        }
    }
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
    #[serde(default)]
    bias_ms: Option<f64>,
    #[serde(default)]
    confidence: Option<f64>,
    #[serde(default)]
    conv_quint: Option<f64>,
    #[serde(default)]
    conv_stdev: Option<f64>,
    #[serde(default)]
    paradigm: Option<String>,
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

#[derive(Debug)]
struct ExpectedBias {
    bias_ms: f64,
    confidence: f64,
    conv_quint: f64,
    conv_stdev: f64,
    paradigm: String,
}

#[derive(Debug, Default)]
struct BiasRows {
    values: BTreeMap<Option<usize>, ExpectedBias>,
    errors: BTreeMap<Option<usize>, String>,
}

const fn default_true() -> bool {
    true
}

const fn default_fingerprint_ms() -> f64 {
    50.0
}

const fn default_step_ms() -> f64 {
    0.2
}

const fn default_window_ms() -> f64 {
    10.0
}

const fn default_tolerance() -> f64 {
    4.0
}

fn default_kernel_target() -> String {
    "digest".to_string()
}

fn default_kernel_type() -> String {
    "rising".to_string()
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
