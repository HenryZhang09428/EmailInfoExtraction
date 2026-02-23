"""
API 模块 (API Module)
====================

FastAPI 后端：提供 /process 处理接口和 /download 下载接口。
支持文件上传或 JSON 传入路径两种方式。
"""

import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from app.backend.process import process_files, write_json_output

app = FastAPI(title="Email Extraction Backend")

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = Path.cwd() / "output"
FILE_REGISTRY: Dict[str, str] = {}


def _register_file(path: str) -> str:
    """将文件路径注册到下载注册表，返回 file_id。"""
    file_id = uuid4().hex
    FILE_REGISTRY[file_id] = path
    return file_id


@app.get("/download/{file_id}")
def download_file(file_id: str):
    """根据 file_id 下载文件。"""
    path = FILE_REGISTRY.get(file_id)
    if not path or not Path(path).exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path, filename=Path(path).name)


@app.post("/process")
async def process_endpoint(
    request: Request,
    files: Optional[List[UploadFile]] = File(default=None),
    require_llm: bool = False,
):
    """
    处理接口：支持 multipart 文件上传或 JSON body（paths、profile_path、require_llm）。
    返回 job_id、result、downloads（含下载 URL）。
    """
    input_paths: List[str] = []
    upload_dir: Optional[Path] = None

    if files:
        upload_dir = Path(tempfile.mkdtemp(prefix="uploads_"))
        for f in files:
            dest = upload_dir / f.filename
            content = await f.read()
            dest.write_bytes(content)
            input_paths.append(str(dest))
        profile_path = None
    else:
        try:
            data = await request.json()
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid JSON body") from e
        paths = data.get("paths")
        if data.get("require_llm") is not None:
            require_llm = bool(data.get("require_llm"))
        profile_path = data.get("profile_path")
        company = data.get("company")
        if (not profile_path) and company:
            company_norm = str(company).strip().lower()
            alias_map = {
                "顺丰": "shunfeng",
                "sf": "shunfeng",
                "shunfeng": "shunfeng",
                "天草": "tiancao",
                "tc": "tiancao",
                "tiancao": "tiancao",
                "楚天龙": "chutianlong",
                "ctl": "chutianlong",
                "chutianlong": "chutianlong",
            }
            key = alias_map.get(company_norm, company_norm)
            candidate = REPO_ROOT / "profiles" / f"{key}.yaml"
            if candidate.exists():
                profile_path = str(candidate)
        if not isinstance(paths, list) or not paths:
            raise HTTPException(status_code=400, detail="paths must be a non-empty list")
        input_paths = [str(Path(p).expanduser()) for p in paths]

    if not input_paths:
        raise HTTPException(status_code=400, detail="No input files provided")

    job_id = uuid4().hex
    output_dir = OUTPUT_ROOT / job_id
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = process_files(
            file_paths=input_paths,
            output_dir=str(output_dir),
            require_llm=require_llm,
            profile_path=profile_path,
        )
        json_path = write_json_output(result, str(output_dir))
    finally:
        if upload_dir and upload_dir.exists():
            shutil.rmtree(upload_dir, ignore_errors=True)

    downloads = {}
    for key, info in (result.get("fills") or {}).items():
        path = info.get("output_path")
        if path:
            file_id = _register_file(path)
            downloads[key] = {
                "file_id": file_id,
                "download_url": f"/download/{file_id}",
                "path": path,
            }
    json_id = _register_file(json_path)
    downloads["json"] = {
        "file_id": json_id,
        "download_url": f"/download/{json_id}",
        "path": json_path,
    }

    return JSONResponse(
        {
            "job_id": job_id,
            "result": result,
            "downloads": downloads,
        }
    )
