#!/usr/bin/env bash

set -eu

wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh;

bash miniconda.sh -b -p $HOME/miniconda
export PATH="$HOME/miniconda/bin:$PATH"
hash -r
conda config --set always_yes yes --set changeps1 no
conda update -q conda

conda install conda-build

CONDA_OUTPUT=$(conda build --output-folder conda-build/build conda-build)
CREATED_FILE=$(echo "${CONDA_OUTPUT}" | sed -n '/^anaconda upload/s/anaconda upload //p')
echo "${CREATED_FILE}"

anaconda -t "${ANACONDA_UPLOAD}" upload --all -d "test" --skip-existing "${CREATED_FILE}"
