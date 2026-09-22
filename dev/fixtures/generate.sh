#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Generate the current Rust corpus without depending on an Iceberg checkout.
set -euo pipefail
fixture_source=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
fixture_build=${1:?Usage: generate.sh BUILD_DIRECTORY OUTPUT_DIRECTORY}
fixture_output=${2:?Usage: generate.sh BUILD_DIRECTORY OUTPUT_DIRECTORY}
mkdir -p -- "$fixture_build" "$fixture_output"
fixture_build=$(cd -- "$fixture_build" && pwd)
fixture_output=$(cd -- "$fixture_output" && pwd)
if [[ -e "$fixture_build/vortex" || -e "$fixture_build/generator" ]]; then
    echo 'Use a fresh build directory; vortex or generator already exists.' >&2
    exit 1
fi
revision=46a8d39c032b1e9f8ae28a13efe8abc9ebad556f
git init --quiet "$fixture_build/vortex"
git -C "$fixture_build/vortex" remote add origin "${VORTEX_GIT_URL:-https://github.com/vortex-data/vortex.git}"
git -C "$fixture_build/vortex" fetch --quiet --depth 1 origin "$revision"
git -C "$fixture_build/vortex" checkout --quiet --detach FETCH_HEAD
git -C "$fixture_build/vortex" apply "$fixture_source/alp-sliced-patches.patch"
mkdir -p "$fixture_build/generator"
cp "$fixture_source/Cargo.toml" "$fixture_source/Cargo.lock" "$fixture_build/generator/"
cp -R "$fixture_source/src" "$fixture_build/generator/src"
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-"$fixture_build/target"}
profile=${VORTEX_BUILD_PROFILE:-release}
for fixture in pco_regressions pco_modes zstd_legacy alp_patches zoned_stats default_writer delta_nullable independent_corpus independent_stats; do
    cargo run --locked --profile "$profile" --manifest-path "$fixture_build/generator/Cargo.toml" \
        --bin "$fixture" -- "$fixture_output"
done

# Independent source formulas supply the paired Parquet oracles.
"${PYTHON:-python3}" "$fixture_source/parquet_oracles.py" "$fixture_output"
