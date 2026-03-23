import json
import time
import sys
import os
from app.services.pipeline import run_pipeline
from app.core.logger import setup_logger
from app.api.api import app



setup_logger()

# Automatically detect file type and extract text.
def main():
    
    if len(sys.argv) < 2:
        print("Usage: python main.py <file_path>")
        sys.exit(1)

    file_path = sys.argv[1]

    if not os.path.exists(file_path):
        print("Error: File not found.")
        sys.exit(1)

    print("\nStarting hierarchical summarization pipeline...\n")

    start_time = time.time()

    try:
        result = run_pipeline(file_path)

        duration = round(time.time() - start_time, 2)

        output_file = "full_summary_output.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=4, ensure_ascii=False)

        print(f"Pipeline completed successfully in {duration} seconds.")
        print(f"Output saved to: {output_file}")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
