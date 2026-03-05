use std::fs;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::audio::probe_ogg_mono_like_python;
use crate::cli::ParityCmd;
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
        Err(err) => {
            return ParityCase {
                simfile_rel,
                simfile_md5: String::new(),
                baseline_rel: None,
                status: "read_error".to_string(),
                error: Some(format!("read simfile failed: {err}")),
            };
        }
    };
    let digest = md5_hex(&bytes);
    let baseline_rel = baseline_rel_for_md5(&digest);
    let (candidate_json, candidate_zst) = baseline_candidates(baseline_root, &digest);
    if candidate_json.exists() {
        return load_baseline_file(&candidate_json, path, &simfile_rel, &digest, &baseline_rel);
    }
    if candidate_zst.exists() {
        return load_baseline_file(&candidate_zst, path, &simfile_rel, &digest, &baseline_rel);
    }
    ParityCase {
        simfile_rel,
        simfile_md5: digest,
        baseline_rel: Some(baseline_rel),
        status: "missing_baseline".to_string(),
        error: None,
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

fn load_baseline_file(
    baseline_path: &Path,
    simfile_path: &Path,
    simfile_rel: &str,
    digest: &str,
    baseline_rel: &str,
) -> ParityCase {
    match read_baseline(baseline_path)
        .and_then(parse_baseline)
        .and_then(|baseline| validate_baseline_audio(simfile_path, &baseline))
    {
        Ok(()) => ParityCase {
            simfile_rel: simfile_rel.to_string(),
            simfile_md5: digest.to_string(),
            baseline_rel: Some(baseline_rel.to_string()),
            status: "matched".to_string(),
            error: None,
        },
        Err(err) => ParityCase {
            simfile_rel: simfile_rel.to_string(),
            simfile_md5: digest.to_string(),
            baseline_rel: Some(baseline_rel.to_string()),
            status: "invalid_baseline".to_string(),
            error: Some(err),
        },
    }
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

fn validate_baseline_audio(simfile_path: &Path, baseline: &BaselineFixture) -> Result<(), String> {
    let song_dir = simfile_path.parent().ok_or_else(|| {
        format!(
            "simfile has no parent directory: {}",
            simfile_path.display()
        )
    })?;
    let mut cache = Vec::new();
    for row in &baseline.charts {
        let Some(music_tag) = chart_music_tag(row, &baseline.music) else {
            continue;
        };
        let Some(audio_path) = rssp::assets::resolve_music_path_like_itg(song_dir, music_tag)
        else {
            return Err(format!(
                "{} unresolved #MUSIC {:?}",
                chart_label(row),
                music_tag
            ));
        };
        probe_ogg_cached(&audio_path, &mut cache)
            .map_err(|e| format!("{} audio probe failed: {e}", chart_label(row)))?;
    }
    Ok(())
}

fn chart_music_tag<'a>(row: &'a BaselineChart, root_music: &'a str) -> Option<&'a str> {
    row.music
        .as_deref()
        .and_then(non_empty_trim)
        .or_else(|| non_empty_trim(root_music))
}

fn non_empty_trim(s: &str) -> Option<&str> {
    let t = s.trim();
    if t.is_empty() { None } else { Some(t) }
}

fn chart_label(row: &BaselineChart) -> String {
    row.chart_index
        .map_or_else(|| "chart[base]".to_string(), |i| format!("chart[{i}]"))
}

fn probe_ogg_cached(path: &Path, cache: &mut Vec<AudioCacheEntry>) -> Result<(), String> {
    let mut probe = |p: &Path| -> Result<(), String> {
        if !is_ogg_path(p) {
            return Err(format!("unsupported audio format at {}", p.display()));
        }
        probe_ogg_mono_like_python(p).map(|_| ())
    };
    probe_cached_with(path, cache, &mut probe)
}

fn probe_cached_with<F>(
    path: &Path,
    cache: &mut Vec<AudioCacheEntry>,
    probe_fn: &mut F,
) -> Result<(), String>
where
    F: FnMut(&Path) -> Result<(), String>,
{
    for entry in cache.iter() {
        if entry.path == path {
            return entry.result.clone();
        }
    }
    let result = probe_fn(path);
    cache.push(AudioCacheEntry {
        path: path.to_path_buf(),
        result: result.clone(),
    });
    result
}

fn is_ogg_path(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("ogg"))
}

fn build_report(root: &Path, baseline: &Path, cases: Vec<ParityCase>) -> ParityReport {
    let total = cases.len();
    let matched = count_status(&cases, "matched");
    let missing = count_status(&cases, "missing_baseline");
    let invalid = count_status(&cases, "invalid_baseline");
    let read_errors = count_status(&cases, "read_error");
    ParityReport {
        tool: "rnon".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        mode: "baseline-mapping".to_string(),
        root_path: root.display().to_string(),
        baseline_path: baseline.display().to_string(),
        total_simfiles: total,
        matched,
        missing_baseline: missing,
        invalid_baseline: invalid,
        read_errors,
        cases,
    }
}

fn count_status(cases: &[ParityCase], status: &str) -> usize {
    cases.iter().filter(|c| c.status == status).count()
}

#[derive(Debug, Deserialize)]
struct BaselineFixture {
    #[serde(default)]
    music: String,
    #[serde(default)]
    charts: Vec<BaselineChart>,
}

#[derive(Debug, Deserialize)]
struct BaselineChart {
    #[serde(default)]
    chart_index: Option<usize>,
    #[serde(default)]
    music: Option<String>,
}

#[derive(Clone)]
struct AudioCacheEntry {
    path: PathBuf,
    result: Result<(), String>,
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::fs;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::cli::ParityCmd;
    use crate::fs_scan::md5_hex;

    use super::{BaselineChart, chart_music_tag, probe_cached_with, run};

    #[test]
    fn parity_matches_existing_baseline_file() {
        let temp = temp_root("parity-pass");
        let root = temp.join("packs");
        let song = root.join("PackA").join("SongA");
        fs::create_dir_all(&song).expect("mkdir song");
        let simfile = song.join("chart.sm");
        let bytes =
            b"#TITLE:Test;#BPMS:0.000=120.000;#NOTES:dance-single:desc:Easy:1:0,0,0,0:0000\n;";
        fs::write(&simfile, bytes).expect("write simfile");

        let md5 = md5_hex(bytes);
        let baseline = temp.join("baseline");
        let shard = baseline.join(&md5[0..2]);
        fs::create_dir_all(&shard).expect("mkdir shard");
        fs::write(shard.join(format!("{md5}.json")), "{}").expect("write baseline");

        let args = ParityCmd {
            root_path: PathBuf::from(&root),
            baseline_path: PathBuf::from(&baseline),
            output: None,
            fail_on_missing: true,
        };
        let report = run(&args).expect("run parity");
        assert_eq!(report.total_simfiles, 1);
        assert_eq!(report.matched, 1);
        assert_eq!(report.missing_baseline, 0);
        assert_eq!(report.invalid_baseline, 0);
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
        };
        let report = run(&args).expect("run parity");
        assert_eq!(report.total_simfiles, 1);
        assert_eq!(report.matched, 0);
        assert_eq!(report.missing_baseline, 1);
        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn chart_music_prefers_row_override_then_root() {
        let row_with = BaselineChart {
            chart_index: Some(2),
            music: Some("split.ogg".to_string()),
        };
        let row_without = BaselineChart {
            chart_index: Some(3),
            music: None,
        };
        assert_eq!(chart_music_tag(&row_with, "base.ogg"), Some("split.ogg"));
        assert_eq!(chart_music_tag(&row_without, "base.ogg"), Some("base.ogg"));
        assert_eq!(chart_music_tag(&row_without, "   "), None);
    }

    #[test]
    fn probe_cache_hits_same_path_once() {
        let mut cache = Vec::new();
        let mut calls = 0usize;
        let mut fake = |_: &Path| -> Result<(), String> {
            calls += 1;
            Ok(())
        };
        let p = Path::new("/tmp/same.ogg");
        let r1 = probe_cached_with(p, &mut cache, &mut fake);
        let r2 = probe_cached_with(p, &mut cache, &mut fake);
        let r3 = probe_cached_with(Path::new("/tmp/other.ogg"), &mut cache, &mut fake);
        assert!(r1.is_ok());
        assert!(r2.is_ok());
        assert!(r3.is_ok());
        assert_eq!(calls, 2);
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
