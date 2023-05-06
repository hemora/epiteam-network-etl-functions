from src.core.context import Context, ExtractContext
import pytest

def test_context_properties():
    """
    """
    ctxt = Context("2020", "01", "01")

    assert (ctxt.year == "2020") and (ctxt.month == "01") and (ctxt.day == "01")

def test_extract_context_properties_1():
    """
    """
    ctxt = ExtractContext("2020", "01", "01")

    assert (ctxt.year == "2020") and (ctxt.month == "01") and (ctxt.day == "01") \
        and (ctxt.data_source == "/datos/botanero/botanero/aws/movement_parquet/year=2020")

def test_extract_context_properties_2():
    """
    """
    ctxt = ExtractContext("2021", "01", "01")

    assert (ctxt.year == "2021") and (ctxt.month == "01") and (ctxt.day == "01") \
        and (ctxt.data_source == "/datos/botanero/botanero/aws/movement_parquet/year=2021")

def test_failed_extract_construction():
    """
    """
    with pytest.raises(SystemExit) as sys_ex:
        ctxt = ExtractContext("2023", "01", "01")

    assert sys_ex.type == SystemExit
    assert sys_ex.value.code == "Invalid Year"
