import pytest
from pipeline_oracle_parquet import sanitize_cnpj

def test_sanitize_cnpj_valid_with_formatting():
    assert sanitize_cnpj("12.345.678/0001-90") == "12345678000190"

def test_sanitize_cnpj_valid_without_formatting():
    assert sanitize_cnpj("12345678000190") == "12345678000190"

def test_sanitize_cnpj_none_value():
    with pytest.raises(ValueError, match="Informe um CNPJ válido."):
        sanitize_cnpj(None)

def test_sanitize_cnpj_empty_string():
    with pytest.raises(ValueError, match="Informe um CNPJ válido."):
        sanitize_cnpj("")

def test_sanitize_cnpj_no_digits():
    with pytest.raises(ValueError, match="Informe um CNPJ válido."):
        sanitize_cnpj("abc")

def test_sanitize_cnpj_mixed_characters():
    assert sanitize_cnpj("12.345abc") == "12345"
