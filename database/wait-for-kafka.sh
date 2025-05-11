#!/bin/bash
set -e

host="$1"
port="$2"

echo "Waiting for Kafka at $host:$port..."
while ! nc -z "$host" "$port"; do
  sleep 1
done

echo "Kafka is up - executing command"
exec "${@:3}"