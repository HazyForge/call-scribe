# RustSec accepted risk for the Discord voice stack

Reviewed: 2026-07-27. Re-review no later than 2026-08-31 and whenever Songbird,
DAVEy, OpenMLS, or hpke-rs releases change.

Call Scribe compiles Songbird 0.6.0 for Discord voice support. Its DAVE
end-to-end encryption dependency currently leaves six RustSec records in
`Cargo.lock`. CI names each exception explicitly; a blanket audit bypass is not
permitted.

| Advisory | Locked package | Runtime reachability | Current disposition |
| --- | --- | --- | --- |
| RUSTSEC-2026-0209 | `libcrux-aesgcm 0.0.7` | Not present in `cargo tree --all-features --target all`; optional lockfile-only dependency | Accepted until upstream lock graph removes it; no patched release exists |
| RUSTSEC-2026-0211 | `libcrux-aesgcm 0.0.7` | Not present in `cargo tree --all-features --target all`; optional lockfile-only dependency | Accepted until upstream lock graph removes it; no patched release exists |
| RUSTSEC-2026-0124 | `libcrux-chacha20poly1305 0.0.7` | Not present in `cargo tree --all-features --target all`; optional lockfile-only dependency | Accepted until upstream accepts `0.0.8` or later |
| RUSTSEC-2026-0212 | `libcrux-secrets 0.0.5` | Compiled through `libcrux-sha3`; the defect is AArch64-specific and the anvil-primaris workload is constrained to an amd64 node | Accepted for the pinned amd64 deployment; do not schedule the worker on AArch64 |
| RUSTSEC-2026-0207 | `libcrux-sha3 0.0.8` | hpke-rs calls only the one-shot `shake256::<32/64>` API; the affected incremental multi-call squeeze API is not used | Accepted based on call-site reachability |
| RUSTSEC-2026-0208 | `libcrux-sha3 0.0.8` | hpke-rs calls only the portable one-shot API; the affected `avx2::x4::shake256` API is not used | Accepted based on call-site reachability |

These are upstream constraints, not declarations that the affected crates are
generally safe. Remove each CI exception as soon as Songbird's released
dependency graph permits it. A deployment that changes CPU architecture or the
HPKE/SHA3 call sites must repeat this analysis first.
