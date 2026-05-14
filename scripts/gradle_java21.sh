#!/usr/bin/env bash
# Run Gradle on JDK 21 inside Docker.
#
# Uses the official `gradle` image (Gradle binary preinstalled) instead of `./gradlew`,
# so the wrapper does not download the distribution over HTTPS — avoids SSLHandshakeException
# on some networks (corporate proxies, TLS inspection, flaky paths to services.gradle.org).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -e GRADLE_USER_HOME=/work/.gradle \
  -v "${ROOT}:/work" \
  -w /work \
  gradle:8.12-jdk21 \
  gradle --no-daemon "$@"
