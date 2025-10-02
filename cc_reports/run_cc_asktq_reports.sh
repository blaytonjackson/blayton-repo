#!/bin/bash

# navigate to script base directory
cd "$(dirname "$0")"

# paths
VENV_DIR="cc_reporting_env"
REQUIREMENTS="requirements.txt"
SCRIPT="cc_asktq_reporting/cc_asktq_reporting.py"

# set up virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    source "$VENV_DIR/bin/activate"
    echo "Installing dependencies from $REQUIREMENTS..."
    pip install --upgrade pip
    pip install -r "$REQUIREMENTS"
else
    echo "Virtual environment already exists. Activating..."
    source "$VENV_DIR/bin/activate"
fi

# run
echo "Running $SCRIPT..."
python "$SCRIPT"