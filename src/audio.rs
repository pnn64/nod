use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use lewton::inside_ogg::OggStreamReader;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OggProbe {
    pub sample_rate_hz: u32,
    pub source_channels: u16,
    pub mono_samples: usize,
}

pub fn probe_ogg_mono_like_python(path: &Path) -> Result<OggProbe, String> {
    let file = File::open(path).map_err(|e| format!("open {} failed: {e}", path.display()))?;
    let mut reader = OggStreamReader::new(BufReader::new(file))
        .map_err(|e| format!("ogg header parse {} failed: {e}", path.display()))?;
    let sample_rate_hz = reader.ident_hdr.audio_sample_rate;
    let source_channels = u16::from(reader.ident_hdr.audio_channels);
    let mut mono_samples = 0usize;
    while let Some(packet) = reader
        .read_dec_packet_itl()
        .map_err(|e| format!("ogg decode {} failed: {e}", path.display()))?
    {
        mono_samples += packet_mono_len(&packet, source_channels);
    }
    Ok(OggProbe {
        sample_rate_hz,
        source_channels,
        mono_samples,
    })
}

fn packet_mono_len(packet: &[i16], channels: u16) -> usize {
    if channels == 2 {
        packet.len() / 2
    } else {
        packet.len()
    }
}

#[cfg(test)]
mod tests {
    use super::packet_mono_len;

    #[test]
    fn stereo_packets_count_half_frames() {
        let packet = [1_i16, 2, 3, 4, 5, 6];
        assert_eq!(packet_mono_len(&packet, 2), 3);
    }

    #[test]
    fn mono_packets_count_all_samples() {
        let packet = [1_i16, 2, 3, 4];
        assert_eq!(packet_mono_len(&packet, 1), 4);
    }
}
