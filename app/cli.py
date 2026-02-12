import argparse
import sys
from pathlib import Path
from typing import List

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.backend.process import process_files, write_json_output


def collect_source_paths(inputs: List[str]) -> List[str]:
    collected: List[str] = []
    for raw in inputs:
        path = Path(raw).expanduser()
        if path.is_dir():
            for child in path.rglob("*"):
                if child.is_file():
                    collected.append(str(child))
        elif path.is_file():
            collected.append(str(path))
        else:
            print(f"[warn] input not found: {raw}")
    return collected


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run extraction and fill two embedded templates."
    )
    parser.add_argument(
        "--inputs",
        nargs="+",
        required=True,
        help="Input file paths or directories.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write output JSON and filled templates.",
    )
    parser.add_argument(
        "--require-llm",
        action="store_true",
        help="Force LLM usage when planning template fill.",
    )
    parser.add_argument(
        "--profile-path",
        default=None,
        help="Path to a profile YAML file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    file_paths = collect_source_paths(args.inputs)
    if not file_paths:
        print("[error] no valid input files found.")
        return 1

    result = process_files(
        file_paths=file_paths,
        output_dir=args.output_dir,
        require_llm=args.require_llm,
        profile_path=args.profile_path,
    )
    json_path = write_json_output(result, args.output_dir)

    fills = result.get("fills", {})
    print("JSON:", json_path)
    for key, info in fills.items():
        print(f"{key}:", info.get("output_path"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
