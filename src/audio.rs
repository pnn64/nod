use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use lewton::inside_ogg::OggStreamReader;

const PCM_INV_SCALE: f32 = 1.0 / 32768.0;
const OGG_BUF_CAP: usize = 256 * 1024;

#[derive(Debug, Clone)]
pub struct OggDecode {
    pub sample_rate_hz: u32,
    pub mono: Vec<f32>,
}

pub fn decode_ogg_mono_like_python(path: &Path) -> Result<OggDecode, String> {
    let file = File::open(path).map_err(|e| format!("open {} failed: {e}", path.display()))?;
    let mut reader = OggStreamReader::new(BufReader::with_capacity(OGG_BUF_CAP, file))
        .map_err(|e| format!("ogg header parse {} failed: {e}", path.display()))?;
    let sample_rate_hz = reader.ident_hdr.audio_sample_rate;
    let source_channels = usize::from(reader.ident_hdr.audio_channels);
    let mut mono = Vec::new();
    while let Some(packet) = reader
        .read_dec_packet()
        .map_err(|e| format!("ogg decode {} failed: {e}", path.display()))?
    {
        append_python_mono_like(&packet, source_channels, &mut mono);
    }
    Ok(OggDecode {
        sample_rate_hz,
        mono,
    })
}

fn append_python_mono_like(packet: &[Vec<i16>], channels: usize, out: &mut Vec<f32>) {
    if channels == 2 && packet.len() >= 2 {
        append_stereo_max(&packet[0], &packet[1], out);
    } else if packet.len() == 1 {
        append_passthrough(&packet[0], out);
    } else {
        append_interleaved_passthrough(packet, out);
    }
}

fn append_stereo_max(left: &[i16], right: &[i16], out: &mut Vec<f32>) {
    let len = left.len().min(right.len());
    out.reserve(len);
    let mut i = 0usize;
    while i < len {
        out.push(f32::from(left[i].max(right[i])) * PCM_INV_SCALE);
        i += 1;
    }
}

fn append_passthrough(packet: &[i16], out: &mut Vec<f32>) {
    out.reserve(packet.len());
    for s in packet {
        out.push(f32::from(*s) * PCM_INV_SCALE);
    }
}

fn append_interleaved_passthrough(packet: &[Vec<i16>], out: &mut Vec<f32>) {
    if packet.is_empty() {
        return;
    }
    let channels = packet.len();
    let frames = packet[0].len();
    out.reserve(channels * frames);
    let mut i = 0usize;
    while i < frames {
        for ch in packet {
            out.push(f32::from(ch[i]) * PCM_INV_SCALE);
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::append_python_mono_like;

    #[test]
    fn stereo_collapse_uses_channel_max() {
        let mut out = Vec::new();
        append_python_mono_like(&[vec![100, -3200], vec![200, -6400]], 2, &mut out);
        assert_eq!(out.len(), 2);
        assert!((out[0] - (200.0 / 32768.0)).abs() < 1e-7);
        assert!((out[1] - (-3200.0 / 32768.0)).abs() < 1e-7);
    }

    #[test]
    fn mono_passthrough_is_normalized() {
        let mut out = Vec::new();
        append_python_mono_like(&[vec![32767, 0, -32768]], 1, &mut out);
        assert_eq!(out.len(), 3);
        assert!((out[0] - (32767.0 / 32768.0)).abs() < 1e-7);
        assert_eq!(out[1], 0.0);
        assert_eq!(out[2], -1.0);
    }

    #[test]
    fn multichannel_passthrough_stays_interleaved() {
        let mut out = Vec::new();
        append_python_mono_like(&[vec![1, 2], vec![10, 20], vec![100, 200]], 1, &mut out);
        assert_eq!(out.len(), 6);
        assert!((out[0] - (1.0 / 32768.0)).abs() < 1e-7);
        assert!((out[1] - (10.0 / 32768.0)).abs() < 1e-7);
        assert!((out[2] - (100.0 / 32768.0)).abs() < 1e-7);
        assert!((out[3] - (2.0 / 32768.0)).abs() < 1e-7);
        assert!((out[4] - (20.0 / 32768.0)).abs() < 1e-7);
        assert!((out[5] - (200.0 / 32768.0)).abs() < 1e-7);
    }
}
