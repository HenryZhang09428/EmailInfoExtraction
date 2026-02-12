from core.extractors.excel_extractor import _cell_to_str


class DummyText:
    text = "员工工号"


class DummyPlain:
    plain = "姓名"


class DummyStr:
    def __str__(self) -> str:
        return "入职日期"


def test_cell_to_str_richtext_like_text() -> None:
    assert _cell_to_str(DummyText()) == "员工工号"


def test_cell_to_str_richtext_like_plain() -> None:
    assert _cell_to_str(DummyPlain()) == "姓名"


def test_cell_to_str_str_fallback() -> None:
    assert _cell_to_str(DummyStr()) == "入职日期"
