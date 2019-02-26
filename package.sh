#!/usr/bin/env bash

if [[ ! -d .venv ]]; then
    python3 -m venv .venv
fi

. .venv/bin/activate

pip install twine
pip install wheel

python setup.py sdist
python setup.py bdist_wheel --universal

