import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from core.pipeline import run_pipeline
from dotenv import load_dotenv

load_dotenv()

eml_files = list(Path(".cache/eml_parts").rglob("*.png"))[:1]
if eml_files:
    print(f"Testing with: {eml_files[0]}")
    ir = run_pipeline([str(eml_files[0])])
    
    print(f"Total sources: {len(ir.sources)}")
    for source in ir.sources:
        print(f"  - {source.source_type}: {source.filename} (parent: {source.parent_source_id})")
        if source.source_type == "image":
            print(f"    Blocks: {[b.type for b in source.blocks]}")
            print(f"    Extracted: {source.extracted}")
else:
    print("No PNG files found in .cache/eml_parts")
