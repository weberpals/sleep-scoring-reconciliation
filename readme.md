# Sleep Score Reconciliation Tool

This tool identifies disagreements between sleep scorers using Python and pandas.

## Setup Instructions

### 1. Install micromamba

If you haven't installed micromamba, follow the instructions [here](https://mamba.readthedocs.io/en/latest/installation.html#micromamba).

### 2. Create and activate the environment

An `environment.yml` file is provided in the project directory. To create and activate the environment, run these commands in your terminal:

```bash
# Navigate to the project directory
cd path/to/project/directory

# Create the environment from the yml file
micromamba create -f environment.yml

# Activate the environment
micromamba activate sleep-score

