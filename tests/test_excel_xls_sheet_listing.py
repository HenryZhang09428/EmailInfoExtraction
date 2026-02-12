import pytest

from core.extractors import excel_extractor as ee


def test_list_sheet_names_uses_xlrd_for_xls(monkeypatch, tmp_path):
    path = tmp_path / "sample.xls"
    path.write_bytes(b"")
    
    called = {"xlrd": False, "openpyxl": False}
    
    class DummyXlsWb:
        def sheet_names(self):
            return ["SheetA", "SheetB"]
    
    def fake_open_workbook(file_path):
        called["xlrd"] = True
        return DummyXlsWb()
    
    def fake_load_workbook(*args, **kwargs):
        called["openpyxl"] = True
        raise AssertionError("openpyxl should not be called for .xls")
    
    import xlrd
    monkeypatch.setattr(xlrd, "open_workbook", fake_open_workbook)
    monkeypatch.setattr(ee, "load_workbook", fake_load_workbook)
    
    sheet_names, backend = ee._list_sheet_names(str(path))
    
    assert sheet_names == ["SheetA", "SheetB"]
    assert backend == "xlrd"
    assert called["xlrd"] is True
    assert called["openpyxl"] is False


def test_extract_sheet_df_uses_engine_by_suffix(monkeypatch, tmp_path):
    engines = []
    
    def fake_read_excel(*args, **kwargs):
        engines.append(kwargs.get("engine"))
        return ee.pd.DataFrame()
    
    monkeypatch.setattr(ee.pd, "read_excel", fake_read_excel)
    
    df_xls, backend_xls, warnings_xls = ee._extract_sheet_df(
        str(tmp_path / "a.xls"),
        "Sheet1",
        ".xls"
    )
    df_xlsx, backend_xlsx, warnings_xlsx = ee._extract_sheet_df(
        str(tmp_path / "a.xlsx"),
        "Sheet1",
        ".xlsx"
    )
    
    assert engines == ["xlrd", "openpyxl"]
    assert backend_xls == "pandas_xlrd"
    assert backend_xlsx == "pandas_openpyxl"
    assert warnings_xls == []
    assert warnings_xlsx == []
