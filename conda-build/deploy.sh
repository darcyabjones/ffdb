#!/usr/bin/env bash

set -eu

CONDA_OUTPUT=$(conda build --output-folder conda-build/build conda-build)
CREATED_FILE=$(echo "${CONDA_OUTPUT}" | sed -n '/^anaconda upload/s/anaconda upload //p')
echo "${CREATED_FILE}"
