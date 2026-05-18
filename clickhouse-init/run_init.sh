#!/bin/bash
set -e

for f in /init-scripts/*.sql; do
    echo "Executing $f ..."
    clickhouse-client --host clickhouse --password spark123 < "$f"
done

echo 'ClickHouse init complete.'
