import os
from processor.processor import GCMDProcessor

if __name__ == "__main__":
    config_path = os.path.join("config", "config.json")
    processor = GCMDProcessor(config_path)
    processor.run()
