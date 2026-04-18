#!/usr/bin/env bash
set -euo pipefail

# Generate spark-defaults.conf from template using environment variables
envsubst < "${SPARK_HOME}/conf/spark-defaults.conf.template" \
         > "${SPARK_HOME}/conf/spark-defaults.conf"

exec "$@"
