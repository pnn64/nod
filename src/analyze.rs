use std::fs;
use std::path::{Path, PathBuf};

use rssp::{AnalysisOptions, analyze};

use crate::audio::{OggDecode, decode_ogg_mono_like_python, duration_seconds, peak_abs};
use crate::bias::{BiasCfg, estimate_bias};
use crate::cli::AnalyzeCmd;
use crate::compat::guess_paradigm;
use crate::compat::slot_abbreviation;
use crate::fs_scan::{discover_simfiles, md5_hex, rel_path};
use crate::model::{
    AnalyzeParams, AnalyzeReport, AudioScan, BiasKernel, ChartScan, KernelTarget, SimfileScan,
};

pub fn run(args: &AnalyzeCmd) -> Result<AnalyzeReport, String> {
    let report_path = resolve_report_path(&args.root_path, args.report_path.as_deref())?;
    fs::create_dir_all(&report_path)
        .map_err(|e| format!("create report dir {} failed: {e}", report_path.display()))?;
    let params = build_params(args, &report_path)?;
    let bias_cfg = bias_cfg_from_params(&params);
    let simfiles = discover_simfiles(&args.root_path)?;
    let scanned = simfiles
        .iter()
        .map(|path| scan_one(path, &args.root_path, &params, &bias_cfg))
        .collect::<Vec<_>>();
    Ok(AnalyzeReport {
        tool: "rnon".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        mode: "scan".to_string(),
        params,
        simfile_count: scanned.len(),
        simfiles: scanned,
    })
}

fn build_params(args: &AnalyzeCmd, report_path: &Path) -> Result<AnalyzeParams, String> {
    let to_paradigm = validate_paradigm(args.to_paradigm.as_deref())?;
    Ok(AnalyzeParams {
        root_path: args.root_path.display().to_string(),
        report_path: report_path.display().to_string(),
        consider_null: args.consider_null,
        consider_p9ms: args.consider_p9ms,
        tolerance: args.tolerance,
        confidence_limit: args.confidence_limit,
        fingerprint_ms: args.fingerprint_ms,
        window_ms: args.window_ms,
        step_ms: args.step_ms,
        magic_offset_ms: args.magic_offset_ms,
        kernel_target: parse_kernel_target(&args.kernel_target)?,
        kernel_type: parse_kernel_type(&args.kernel_type)?,
        full_spectrogram: args.full_spectrogram,
        to_paradigm,
    })
}

fn validate_paradigm(raw: Option<&str>) -> Result<Option<String>, String> {
    let Some(value) = raw else {
        return Ok(None);
    };
    if value == "null" || value == "+9ms" {
        Ok(Some(value.to_string()))
    } else {
        Err(format!("invalid paradigm: {value}"))
    }
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

fn resolve_report_path(root: &Path, explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(path) = explicit {
        return Ok(path.to_path_buf());
    }
    if root.is_file() {
        let parent = root
            .parent()
            .ok_or_else(|| format!("cannot resolve parent dir for {}", root.display()))?;
        Ok(parent.join("__bias-check"))
    } else {
        Ok(root.join("__bias-check"))
    }
}

fn scan_one(path: &Path, root: &Path, params: &AnalyzeParams, bias_cfg: &BiasCfg) -> SimfileScan {
    let rel = rel_path(root, path);
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) => return read_error(path, &rel, format!("read failed: {err}")),
    };
    let ext = simfile_ext(path);
    let digest = md5_hex(&bytes);
    let options = AnalysisOptions::default();
    match analyze(&bytes, &ext, &options) {
        Ok(summary) => {
            let audio_probe = probe_audio(path, &summary.music_path);
            let mut chart_scans = charts_from_summary(&summary.charts);
            apply_bias_estimates(
                &summary.charts,
                &mut chart_scans,
                audio_probe.decoded.as_ref(),
                params,
                bias_cfg,
            );
            SimfileScan {
                simfile_path: path.display().to_string(),
                simfile_rel: rel,
                simfile_md5: digest,
                extension: ext,
                status: "stub".to_string(),
                error: None,
                title: Some(summary.title_str),
                subtitle: Some(summary.subtitle_str),
                artist: Some(summary.artist_str),
                offset_seconds: Some(summary.offset),
                music_tag: Some(summary.music_path),
                audio: audio_probe.scan,
                charts: chart_scans,
            }
        }
        Err(err) => SimfileScan {
            simfile_path: path.display().to_string(),
            simfile_rel: rel,
            simfile_md5: digest,
            extension: ext,
            status: "error".to_string(),
            error: Some(format!("rssp analyze failed: {err}")),
            title: None,
            subtitle: None,
            artist: None,
            offset_seconds: None,
            music_tag: None,
            audio: audio_unavailable("analyze_error", "simfile analysis failed"),
            charts: Vec::new(),
        },
    }
}

fn charts_from_summary(charts: &[rssp::ChartSummary]) -> Vec<ChartScan> {
    charts
        .iter()
        .enumerate()
        .map(|(i, chart)| ChartScan {
            chart_index: i,
            steps_type: chart.step_type_str.clone(),
            difficulty: chart.difficulty_str.clone(),
            description: chart.description_str.clone(),
            slot_null: slot_abbreviation(&chart.step_type_str, &chart.difficulty_str, i, "null"),
            slot_p9ms: slot_abbreviation(&chart.step_type_str, &chart.difficulty_str, i, "+9ms"),
            chart_has_own_timing: chart.chart_has_own_timing,
            status: "stub".to_string(),
            bias_ms: None,
            confidence: None,
            conv_quint: None,
            conv_stdev: None,
            paradigm: None,
        })
        .collect()
}

fn read_error(path: &Path, rel: &str, err: String) -> SimfileScan {
    SimfileScan {
        simfile_path: path.display().to_string(),
        simfile_rel: rel.to_string(),
        simfile_md5: String::new(),
        extension: simfile_ext(path),
        status: "error".to_string(),
        error: Some(err),
        title: None,
        subtitle: None,
        artist: None,
        offset_seconds: None,
        music_tag: None,
        audio: audio_unavailable("read_error", "simfile could not be read"),
        charts: Vec::new(),
    }
}

fn simfile_ext(path: &Path) -> String {
    path.extension()
        .and_then(|s| s.to_str())
        .map_or_else(String::new, |s| s.to_ascii_lowercase())
}

fn probe_audio(simfile_path: &Path, music_tag: &str) -> AudioProbe {
    let Some(song_dir) = simfile_path.parent() else {
        return AudioProbe {
            scan: audio_unavailable("missing_song_dir", "simfile has no parent directory"),
            decoded: None,
        };
    };
    let Some(audio_path) = rssp::assets::resolve_music_path_like_itg(song_dir, music_tag) else {
        return AudioProbe {
            scan: AudioScan {
                status: "missing".to_string(),
                path: None,
                sample_rate_hz: None,
                source_channels: None,
                mono_samples: None,
                duration_seconds: None,
                peak_abs: None,
                error: Some("no OGG file resolved for #MUSIC".to_string()),
            },
            decoded: None,
        };
    };
    if !is_ogg_path(&audio_path) {
        return AudioProbe {
            scan: AudioScan {
                status: "unsupported".to_string(),
                path: Some(audio_path.display().to_string()),
                sample_rate_hz: None,
                source_channels: None,
                mono_samples: None,
                duration_seconds: None,
                peak_abs: None,
                error: Some("only OGG is currently supported".to_string()),
            },
            decoded: None,
        };
    }
    match decode_ogg_mono_like_python(&audio_path) {
        Ok(decoded) => AudioProbe {
            scan: AudioScan {
                status: "decoded".to_string(),
                path: Some(audio_path.display().to_string()),
                sample_rate_hz: Some(decoded.sample_rate_hz),
                source_channels: Some(decoded.source_channels),
                mono_samples: Some(decoded.mono.len()),
                duration_seconds: Some(duration_seconds(
                    decoded.mono.len(),
                    decoded.sample_rate_hz,
                )),
                peak_abs: Some(peak_abs(&decoded.mono)),
                error: None,
            },
            decoded: Some(decoded),
        },
        Err(err) => AudioProbe {
            scan: AudioScan {
                status: "error".to_string(),
                path: Some(audio_path.display().to_string()),
                sample_rate_hz: None,
                source_channels: None,
                mono_samples: None,
                duration_seconds: None,
                peak_abs: None,
                error: Some(err),
            },
            decoded: None,
        },
    }
}

fn audio_unavailable(status: &str, reason: &str) -> AudioScan {
    AudioScan {
        status: status.to_string(),
        path: None,
        sample_rate_hz: None,
        source_channels: None,
        mono_samples: None,
        duration_seconds: None,
        peak_abs: None,
        error: Some(reason.to_string()),
    }
}

fn is_ogg_path(path: &Path) -> bool {
    path.extension()
        .and_then(|s| s.to_str())
        .is_some_and(|s| s.eq_ignore_ascii_case("ogg"))
}

fn apply_bias_estimates(
    summary_charts: &[rssp::ChartSummary],
    chart_scans: &mut [ChartScan],
    audio: Option<&OggDecode>,
    params: &AnalyzeParams,
    bias_cfg: &BiasCfg,
) {
    let Some(audio) = audio else {
        for chart in chart_scans {
            chart.status = "audio_unavailable".to_string();
        }
        return;
    };
    for (i, chart) in summary_charts.iter().enumerate() {
        let result = estimate_bias(&audio.mono, audio.sample_rate_hz, chart, bias_cfg);
        let scan = &mut chart_scans[i];
        match result {
            Ok(est) => {
                scan.status = "computed".to_string();
                scan.bias_ms = Some(est.bias_ms);
                scan.confidence = Some(est.confidence);
                scan.conv_quint = Some(est.conv_quint);
                scan.conv_stdev = Some(est.conv_stdev);
                scan.paradigm = Some(
                    guess_paradigm(
                        est.bias_ms,
                        params.tolerance,
                        params.consider_null,
                        params.consider_p9ms,
                        true,
                    )
                    .to_string(),
                );
            }
            Err(err) => {
                scan.status = "bias_error".to_string();
                scan.paradigm = Some("????".to_string());
                scan.conv_quint = None;
                scan.conv_stdev = None;
                scan.bias_ms = None;
                scan.confidence = None;
                if scan.description.is_empty() {
                    scan.description = format!("[bias_error: {err}]");
                }
            }
        }
    }
}

fn bias_cfg_from_params(params: &AnalyzeParams) -> BiasCfg {
    BiasCfg {
        fingerprint_ms: params.fingerprint_ms,
        window_ms: params.window_ms,
        step_ms: params.step_ms,
        magic_offset_ms: params.magic_offset_ms,
        kernel_target: params.kernel_target,
        kernel_type: params.kernel_type,
        _full_spectrogram: params.full_spectrogram,
    }
}

struct AudioProbe {
    scan: AudioScan,
    decoded: Option<OggDecode>,
}
