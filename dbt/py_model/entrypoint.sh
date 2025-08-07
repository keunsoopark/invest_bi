#!/bin/bash
set -e

echo "Entrypoint running with args: $@"

exec "$@"
