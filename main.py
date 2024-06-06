import os
import argparse
from processor.processor import GCMDProcessor

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GeoNetwork GCMD Extractor")
    parser.add_argument(
        "--test",
        type=int,
        help="Run in test mode with the specified number of records. If not provided, the full dataset will be processed.",
    )
    args = parser.parse_args()

    config_path = os.path.join("config", "config.json")
    processor = GCMDProcessor(config_path)

    if args.test:
        print(f"Running in test mode with {args.test} records.")
        print("---------------------------------")
        processor.run(total_records=args.test)
    else:
        processor.run()
