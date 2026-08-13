#!/usr/bin/env bash
set -euo pipefail

BIN=/opt/kafka/bin
BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
GROUP=inference-workers

topic() {
  "$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1
}

pids=()
topic inference.jobs 6 & pids+=($!)
topic inference.jobs.retry 6 & pids+=($!)
topic inference.results 6 & pids+=($!)
topic inference.dlq 3 & pids+=($!)
for pid in "${pids[@]}"; do wait "$pid"; done

"$BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" \
  --entity-type groups --entity-name "$GROUP" --alter \
  --add-config share.delivery.count.limit=4,share.record.lock.duration.ms=30000,share.auto.offset.reset=earliest

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe
