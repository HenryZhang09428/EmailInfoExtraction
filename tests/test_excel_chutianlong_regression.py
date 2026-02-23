import re
from pathlib import Path

import pytest

from core.extractors.email_extractor import EmailExtractor
from core.extractors.excel_extractor import ExcelExtractor
from core.llm import get_llm_client
from core.prompts_loader import get_prompts


REGRESSION_EML_PATH = Path(
    "/Users/zhanghengyu/Desktop/仕邦-Jan29/楚天龙/"
    "Fw_ 4月增减员-湖北楚天龙实业有限公司-仕邦（宁波）- 2.eml"
)


class DummyLLM:
    def chat_json(self, prompt: str, system=None, temperature=None, step=None, **kwargs):
        return {}

    def chat_json_once(self, prompt: str, system=None, temperature=None, step=None, timeout=None, **kwargs):
        return {}


def _extract_attachment_excel_from_eml(eml_path: Path) -> Path:
    extractor = EmailExtractor(DummyLLM(), prompts={})
    doc = extractor.safe_extract(str(eml_path))
    assert doc is not None
    derived = extractor.get_derived_files()
    excel_paths = [
        Path(p)
        for p in derived
        if isinstance(p, str) and Path(p).suffix.lower() in {".xls", ".xlsx"}
    ]
    assert excel_paths, f"No excel attachment exported from eml: {eml_path}"
    return excel_paths[0]


def _run_excel_extract_all_sheets(xls_path: Path):
    prompts = get_prompts()
    llm = get_llm_client()
    extractor = ExcelExtractor(llm, prompts=prompts)
    return extractor.safe_extract(str(xls_path), extract_all_sheets=True)


@pytest.fixture(scope="module")
def chutianlong_excel_path() -> Path:
    if not REGRESSION_EML_PATH.exists():
        pytest.skip(f"Regression eml not found: {REGRESSION_EML_PATH}")
    return _extract_attachment_excel_from_eml(REGRESSION_EML_PATH)


@pytest.fixture(scope="module")
def chutianlong_doc(chutianlong_excel_path: Path):
    return _run_excel_extract_all_sheets(chutianlong_excel_path)


def test_stage1_a_should_keep_target_and_filter_demo_records(chutianlong_doc):
    extracted = chutianlong_doc.extracted or {}
    data = extracted.get("data") or []
    assert isinstance(data, list)

    assert any(
        isinstance(r, dict)
        and r.get("name") == "袁海月"
        and str(r.get("id_number", "")) == "632822199309150325"
        for r in data
    ), "Expected target record (袁海月, 632822199309150325) missing"

    assert not any(
        isinstance(r, dict)
        and r.get("__sheet_name__") == "示例"
        and (r.get("name") == "测试" or "****" in str(r.get("id_number", "")))
        for r in data
    ), "Demo/sample rows from 示例 sheet should not be mixed into final records"


def test_stage1_b_social_security_remove_sheet_keys_and_id_check(chutianlong_doc):
    extracted = chutianlong_doc.extracted or {}
    data = extracted.get("data") or []
    ss_remove_records = [
        r for r in data if isinstance(r, dict) and r.get("__sheet_name__") == "社保减员"
    ]
    assert ss_remove_records, "Expected records from 社保减员 sheet"

    for rec in ss_remove_records:
        for k in rec.keys():
            assert re.match(r".+__\d+$", k) is None, f"Unexpected suffixed key in record: {k}"

    has_distinct_pair = any(
        (("ss_base" in r and "hf_base" in r) or ("ss_amount" in r and "hf_amount" in r))
        and (("ss_end_month" in r and "hf_end_month" in r) or ("ss_end_date" in r and "hf_end_date" in r))
        for r in ss_remove_records
    )
    assert has_distinct_pair, "Expected distinct SS/HF key pairs in 社保减员 records"

    bool_like = {True, False, "True", "False", "是", "否", "通过", "不通过"}
    assert not any(r.get("id_card") in bool_like for r in ss_remove_records), (
        "Boolean identity-check column should not pollute id_card field"
    )


def test_stage1_c_bank_account_mapping_should_not_be_employee_id(chutianlong_doc):
    extracted = chutianlong_doc.extracted or {}
    metadata = extracted.get("metadata") or {}
    per_sheet = metadata.get("per_sheet") or {}
    target_meta = per_sheet.get("人员参保登记报盘模板") or {}
    sem_map = target_meta.get("semantic_key_by_header") or {}
    assert isinstance(sem_map, dict)

    bank_key = sem_map.get("银行账号")
    allowed_bank_account_keys = {"bank_account", "account_number", "bank_card_number"}
    assert bank_key in allowed_bank_account_keys, (
        f"银行账号 should map to bank account semantic key, got: {bank_key!r}"
    )
    assert bank_key != "employee_id", "银行账号 must not be mapped to employee_id"

