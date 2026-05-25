#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker run --rm \
  -e GRADLE_USER_HOME=/work/.gradle \
  -v "${ROOT}:/work" \
  -w /work \
  gradle:8.12-jdk21 \
  gradle --no-daemon "$@"
