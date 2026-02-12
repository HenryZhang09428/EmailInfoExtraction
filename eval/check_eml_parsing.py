import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.pipeline import run_pipeline

eml_path = Path(__file__).parent / "fixtures" / "sample_with_inline_image.eml"

ir = run_pipeline([str(eml_path)])

email_sources = [s for s in ir.sources if s.source_type == "email"]
assert len(email_sources) > 0, "No email source found"

email_source = email_sources[0]
block_types = [b.type for b in email_source.blocks]
assert "eml_file_part" in block_types or "inline_image_file" in block_types, "No eml_file_part or inline_image_file block found"

has_image_attachment = False
for block in email_source.blocks:
    if block.type == "eml_file_part":
        content = block.content
        if isinstance(content, dict):
            content_type = content.get("content_type", "")
            if "image" in content_type:
                has_image_attachment = True
                break
    elif block.type == "inline_image_file":
        has_image_attachment = True
        break

if has_image_attachment:
    image_sources = [s for s in ir.sources if s.source_type == "image"]
    assert len(image_sources) > 0, "Email contains image but no image source found in IR"

print("OK")
