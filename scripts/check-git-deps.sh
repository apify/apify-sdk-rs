#!/usr/bin/env bash
# Fails when a dependency on apify-sdk-rs or crawlee-rs is not on the `master` branch: a branch,
# tag or revision of an unmerged change is for trying it only (see Cargo.toml).
set -euo pipefail
bad=$(grep -nE 'git = "https://github\.com/apify(-projects)?/(apify-sdk-rs|crawlee-rs)"' "${1:-Cargo.toml}" | grep -v 'branch = "master"' || true)
if [ -n "$bad" ]; then
    echo "Dependencies not on master (point them back at branch = \"master\" and run cargo update):"
    echo "$bad"
    exit 1
fi
