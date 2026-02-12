import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from core.pipeline import run_pipeline

if len(sys.argv) < 2:
    print("Usage: python test_eml_images.py <eml_file_path>")
    sys.exit(1)

eml_path = sys.argv[1]

print(f"Processing EML file: {eml_path}")
ir = run_pipeline([eml_path])

print(f"\nTotal sources: {len(ir.sources)}")

email_sources = [s for s in ir.sources if s.source_type == "email"]
print(f"Email sources: {len(email_sources)}")

image_sources = [s for s in ir.sources if s.source_type == "image"]
print(f"Image sources: {len(image_sources)}")

for img_source in image_sources:
    print(f"\nImage: {img_source.filename}")
    print(f"  Parent Source ID: {img_source.parent_source_id}")
    if img_source.extracted:
        if isinstance(img_source.extracted, dict):
            extracted_fields = img_source.extracted.get("extracted_fields", {})
            if extracted_fields:
                print(f"  Extracted Fields: {extracted_fields}")
            else:
                print(f"  No extracted_fields found")
            tables = img_source.extracted.get("tables", [])
            if tables:
                print(f"  Tables: {len(tables)} tables found")
        else:
            print(f"  Extracted: {img_source.extracted}")
    else:
        print(f"  No extracted data")

print("\nDone!")
