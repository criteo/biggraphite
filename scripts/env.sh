#!/bin/sh

export PROJECT_ROOT=$(git rev-parse --show-toplevel)

cd ${PROJECT_ROOT}

export BG_VENV=bg

test -d "${BG_VENV}" && INITIALIZE_BG_VENV=false || INITIALIZE_BG_VENV=true

virtualenv ${BG_VENV}
source ${BG_VENV}/bin/activate

# By default carbon and graphite-web are installed in /opt/graphite,
# We want NO prefix in order to have a good interaction with virtual env.
export GRAPHITE_NO_PREFIX=true

if ${INITIALIZE_BG_VENV}; then
    # Install Graphite Web and Carbon
    pip install graphite-web
    pip install carbon

    # Install the libffi-dev package from your distribution before running pip install
    pip install -r requirements.txt
    pip install -r tests-requirements.txt
    pip install -e .
fi

# In test environment, limit to 12 components
export BG_COMPONENTS_MAX_LEN=12
