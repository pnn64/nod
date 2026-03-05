use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use lewton::inside_ogg::OggStreamReader;

const PCM_SCALE: f32 = 32768.0;

#[derive(Debug, Clone)]
pub struct OggDecode {
    pub sample_rate_hz: u32,
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
        mono,
    })
}

fn append_python_mono_like(packet: &[i16], channels: u16, out: &mut Vec<f32>) {
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
        let l = packet[i];
        let r = packet[i + 1];
        let s = if l > r { l } else { r };
        out.push(f32::from(s) / PCM_SCALE);
        i += 2;
    }
}

fn append_passthrough(packet: &[i16], out: &mut Vec<f32>) {
    out.reserve(packet.len());
    for s in packet {
        out.push(f32::from(*s) / PCM_SCALE);
    }
}

#[cfg(test)]
mod tests {
    use super::append_python_mono_like;

    #[test]
    fn stereo_collapse_uses_channel_max() {
        let mut out = Vec::new();
        append_python_mono_like(&[100, 200, -3200, -6400], 2, &mut out);
        assert_eq!(out.len(), 2);
        assert!((out[0] - (200.0 / 32768.0)).abs() < 1e-7);
        assert!((out[1] - (-3200.0 / 32768.0)).abs() < 1e-7);
    }

    #[test]
    fn mono_passthrough_is_normalized() {
        let mut out = Vec::new();
        append_python_mono_like(&[32767, 0, -32768], 1, &mut out);
        assert_eq!(out.len(), 3);
        assert!((out[0] - (32767.0 / 32768.0)).abs() < 1e-7);
        assert_eq!(out[1], 0.0);
        assert_eq!(out[2], -1.0);
    }
}
