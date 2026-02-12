import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from core.pipeline import run_pipeline

eml_path = Path(__file__).parent / "fixtures" / "sample_with_images.eml"

if not eml_path.exists():
    print(f"Error: {eml_path} not found")
    sys.exit(1)

ir = run_pipeline([str(eml_path)])

email_sources = [s for s in ir.sources if s.source_type == "email"]
assert len(email_sources) > 0, "No email source found"

image_sources = [s for s in ir.sources if s.source_type == "image"]
assert len(image_sources) > 0, "No image source found"

for img_source in image_sources:
    if img_source.extracted and isinstance(img_source.extracted, dict):
        extracted_fields = img_source.extracted.get("extracted_fields", {})
        tables = img_source.extracted.get("tables", [])
        has_content = len(extracted_fields) > 0 or len(tables) > 0
        assert has_content, f"Image source {img_source.filename} has no extracted_fields or tables"

print("OK")
