
# GCMD Keywords Extractor

A Python tool for extracting GCMD (Global Change Master Directory) keywords used by metadata records from AODN's GeoNetwork catalog via the CSW service.

This tool assists the AODN Metadata Governance Officer in extracting GCMD keyword on-demand reports.

It works with the CSW service of both GeoNetwork3 and GeoNetwork4.

### Requirements

- Python 3.10
- Poetry
- Conda (recommended for creating a virtual environment)

### Installation

1. **Install Conda** (if not already installed):

    Follow the instructions at [Conda Installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).

2. **Create and activate a Conda virtual environment:**

    ```bash
    conda create -n gcmd_extractor python=3.10
    conda activate gcmd_extractor
    ```

3. **Install Poetry** (if not already installed):

    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```

    Make sure to add Poetry to your PATH as instructed during the installation.

4. **Clone the repository:**

    ```bash
    # after cloning the repo with git clone command
    cd geonetwork-gcmd-extractor
    ```

5. **Install dependencies using Poetry:**

    ```bash
    poetry install
    ```

### Usage

Configurations are defined in `config/config.json`, you can change CSW service source URL in there for example.

Run the script:

```bash
poetry run python main.py
```

For parameter usage instruction
```bash
poetry run python main.py --help
```

### NLP Grouping Similar Texts

There is an implementation for using NLP to fuzzy group similar texts regardless of typos, plurals, case sensitivity, etc. For example:

Inputs:
```python
["Sea surface tempoerature", "SEA SURFACE TEMPERATUR", "car", "cars", "elephant", "ellephent", "antarticca"]
```

Outputs:
```python
['SEA SURFACE TEMPERATURE', 'CAR', 'ELEPHANT', 'ANTARCTICA']
```

This module is not used in the processor class; it is there for reference purposes. To use it, after running `poetry install`, you might want to run `poetry run download-spacy-model` and then import it where needed.

```python
from utils.nlp_grouping import GroupingSimilarTexts
```

### Extracted results

Output files will be generated in the `outputs` folder.