
# GCMD Keywords Extractor

A Python script to connect to a CSW service and extract GCMD (Global Change Master Directory) keywords from metadata records from AODN's GeoNetwork catalog.

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
    git clone https://github.com/vietnguyengit/geonetwork-gcmd-extractor
    cd geonetwork-gcmd-extractor
    ```

5. **Install dependencies using Poetry:**

    ```bash
    poetry install
    ```

### Usage

Run the script:

```bash
poetry run python main.py
```

Output files will be generated in the `outputs` folder:

- `gcmd_keywords.txt`: Contains extracted GCMD keywords.
- `records_failed.txt`: Contains IDs of records that do not have GCMD keywords and may require manual inspection.
