# rnon

Terminal-first Rust reimplementation scaffold for `nine-or-null` parity work.

## Current status

- `rnon analyze <path>`: scans simfiles, parses chart metadata through `rssp`, emits JSON.
- `rnon parity <path> --baseline <dir>`: validates MD5-sharded baseline fixture coverage.
- `rnon plot <input.json> <out.png>`: draws bias markers from JSON (`bias_ms`, `bias_result`, or `bias`).

This is intentionally phase-0: analysis math is not implemented yet. The scaffold exists to freeze CLI/fixture contracts and start parity workflows.

## Baseline layout

MD5-sharded baseline lookup matches the existing `rssp` corpus style:

`<baseline>/<md5[0..2]>/<md5>.json` or `<baseline>/<md5[0..2]>/<md5>.json.zst`

MD5 is computed from raw simfile bytes.

## Examples

```bash
cargo run -- analyze /path/to/Songs --output /tmp/rnon-scan.json
cargo run -- parity /path/to/Songs --baseline /path/to/baseline --fail-on-missing
cargo run -- plot /tmp/rnon-scan.json /tmp/bias.png --span-ms 20
```
