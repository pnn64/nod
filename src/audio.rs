use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use lewton::inside_ogg::OggStreamReader;

const PCM_SCALE: f32 = 32768.0;

#[derive(Debug, Clone)]
pub struct OggDecode {
    pub sample_rate_hz: u32,
    pub source_channels: u16,
    pub mono: Vec<f32>,
}

pub fn decode_ogg_mono_like_python(path: &Path) -> Result<OggDecode, String> {
    let file = File::open(path).map_err(|e| format!("open {} failed: {e}", path.display()))?;
    let mut reader = OggStreamReader::new(BufReader::new(file))
        .map_err(|e| format!("ogg header parse {} failed: {e}", path.display()))?;
    let sample_rate_hz = reader.ident_hdr.audio_sample_rate;
    let source_channels = u16::from(reader.ident_hdr.audio_channels);
    let mut mono = Vec::new();
    while let Some(packet) = reader
        .read_dec_packet_itl()
        .map_err(|e| format!("ogg decode {} failed: {e}", path.display()))?
    {
        append_python_mono_like(&packet, source_channels, &mut mono);
    }
    Ok(OggDecode {
        sample_rate_hz,
        source_channels,
        mono,
    })
}

pub fn append_python_mono_like(packet: &[i16], channels: u16, out: &mut Vec<f32>) {
    if channels == 2 {
        append_stereo_max(packet, out);
    } else {
        append_passthrough(packet, out);
    }
}

fn append_stereo_max(packet: &[i16], out: &mut Vec<f32>) {
    out.reserve(packet.len() / 2);
    let mut i = 0usize;
    while i + 1 < packet.len() {
        let left = packet[i];
        let right = packet[i + 1];
        let sample = if left > right { left } else { right };
        out.push(f32::from(sample) / PCM_SCALE);
        i += 2;
    }
}

fn append_passthrough(packet: &[i16], out: &mut Vec<f32>) {
    out.reserve(packet.len());
    for sample in packet {
        out.push(f32::from(*sample) / PCM_SCALE);
    }
}

pub fn peak_abs(samples: &[f32]) -> f32 {
    samples.iter().fold(0.0_f32, |acc, s| acc.max(s.abs()))
}

pub fn duration_seconds(sample_count: usize, sample_rate_hz: u32) -> f64 {
    if sample_rate_hz == 0 {
        0.0
    } else {
        sample_count as f64 / f64::from(sample_rate_hz)
    }
}

#[cfg(test)]
mod tests {
    use super::{append_python_mono_like, duration_seconds, peak_abs};

    #[test]
    fn stereo_is_collapsed_by_max_per_frame() {
        let mut out = Vec::new();
        let packet = [100i16, 200, -3200, -6400, 32000, 1000];
        append_python_mono_like(&packet, 2, &mut out);
        let expected = [200.0 / 32768.0, -3200.0 / 32768.0, 32000.0 / 32768.0];
        assert_eq!(out.len(), expected.len());
        for i in 0..out.len() {
            assert!((out[i] - expected[i]).abs() < 1e-7);
        }
    }

    #[test]
    fn non_stereo_is_passthrough_normalized() {
        let mut out = Vec::new();
        let packet = [32767i16, 0, -32768, 1024];
        append_python_mono_like(&packet, 1, &mut out);
        assert_eq!(out.len(), 4);
        assert!((out[0] - (32767.0 / 32768.0)).abs() < 1e-7);
        assert_eq!(out[1], 0.0);
        assert_eq!(out[2], -1.0);
        assert!((out[3] - (1024.0 / 32768.0)).abs() < 1e-7);
    }

    #[test]
    fn peak_and_duration_helpers_work() {
        let samples = [0.1_f32, -0.75, 0.2];
        assert!((peak_abs(&samples) - 0.75).abs() < 1e-7);
        assert!((duration_seconds(44100, 44100) - 1.0).abs() < 1e-12);
    }
}
