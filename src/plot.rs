use std::fs;
use std::fs::File;
use std::io::BufWriter;

use png::{BitDepth, ColorType, Encoder};
use serde_json::Value;

use crate::cli::PlotCmd;
use crate::model::PlotReport;

pub fn run(args: &PlotCmd) -> Result<PlotReport, String> {
    if args.width == 0 || args.height == 0 {
        return Err("width and height must be > 0".to_string());
    }
    let text = fs::read_to_string(&args.input_json)
        .map_err(|e| format!("read {} failed: {e}", args.input_json.display()))?;
    let value: Value = serde_json::from_str(&text)
        .map_err(|e| format!("parse {} failed: {e}", args.input_json.display()))?;
    let mut biases = Vec::new();
    collect_biases(&value, &mut biases);
    if biases.is_empty() {
        return Err("no bias values found in JSON (bias_ms / bias_result / bias)".to_string());
    }
    write_bias_plot(
        &args.output_png,
        args.width,
        args.height,
        args.span_ms,
        &biases,
    )?;
    Ok(PlotReport {
        tool: "rnon".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        input_json: args.input_json.display().to_string(),
        output_png: args.output_png.display().to_string(),
        width: args.width,
        height: args.height,
        span_ms: args.span_ms,
        bias_count: biases.len(),
    })
}

fn collect_biases(value: &Value, out: &mut Vec<f64>) {
    match value {
        Value::Object(map) => {
            for key in ["bias_ms", "bias_result", "bias"] {
                if let Some(v) = map.get(key).and_then(parse_bias) {
                    out.push(v);
                }
            }
            for v in map.values() {
                collect_biases(v, out);
            }
        }
        Value::Array(items) => {
            for v in items {
                collect_biases(v, out);
            }
        }
        _ => {}
    }
}

fn parse_bias(value: &Value) -> Option<f64> {
    if let Some(v) = value.as_f64() {
        Some(v)
    } else {
        value.as_str().and_then(|s| s.parse::<f64>().ok())
    }
}

fn write_bias_plot(
    path: &std::path::Path,
    width: u32,
    height: u32,
    span_ms: f64,
    biases: &[f64],
) -> Result<(), String> {
    let mut image = vec![255u8; (width as usize) * (height as usize) * 4];
    let center_x = width / 2;
    draw_vline(&mut image, width, height, center_x, [96, 96, 96, 255]);
    for bias in biases {
        let x = bias_to_x(*bias, span_ms, width);
        draw_vline(&mut image, width, height, x, [220, 40, 40, 255]);
    }
    let file = File::create(path).map_err(|e| format!("create {} failed: {e}", path.display()))?;
    let writer = BufWriter::new(file);
    let mut encoder = Encoder::new(writer, width, height);
    encoder.set_color(ColorType::Rgba);
    encoder.set_depth(BitDepth::Eight);
    let mut png_writer = encoder
        .write_header()
        .map_err(|e| format!("png header {} failed: {e}", path.display()))?;
    png_writer
        .write_image_data(&image)
        .map_err(|e| format!("png write {} failed: {e}", path.display()))
}

fn bias_to_x(bias_ms: f64, span_ms: f64, width: u32) -> u32 {
    let span = if span_ms.abs() < f64::EPSILON {
        50.0
    } else {
        span_ms
    };
    let normalized = ((bias_ms + span) / (span * 2.0)).clamp(0.0, 1.0);
    (normalized * f64::from(width.saturating_sub(1))).round() as u32
}

fn draw_vline(image: &mut [u8], width: u32, height: u32, x: u32, rgba: [u8; 4]) {
    for y in 0..height {
        let idx = ((y * width + x) * 4) as usize;
        image[idx] = rgba[0];
        image[idx + 1] = rgba[1];
        image[idx + 2] = rgba[2];
        image[idx + 3] = rgba[3];
    }
}
