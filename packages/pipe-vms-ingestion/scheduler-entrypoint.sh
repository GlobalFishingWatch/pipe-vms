#!/bin/sh

set -e

. /venv/bin/activate

python -u main.py "$@"