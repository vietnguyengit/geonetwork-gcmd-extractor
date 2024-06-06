import subprocess
import sys


def run():
    subprocess.run([sys.executable, "-m", "spacy", "download", "en_core_web_md"])


if __name__ == "__main__":
    run()
