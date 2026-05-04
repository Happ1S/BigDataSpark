#!/bin/bash
set -e

for f in /init-scripts/*.sql; do
    echo "Executing $f ..."
    clickhouse-client --host clickhouse < "$f"
done

echo 'ClickHouse init complete.'
