import pytest
from tabelas_auditorias.utils import gtin_valido

@pytest.mark.parametrize("gtin", [
    "73513537",        # GTIN-8
    "012345678905",    # GTIN-12
    "7891000315507",   # GTIN-13
    "17891000315504",  # GTIN-14
])
def test_gtin_valido_valid_codes(gtin):
    assert gtin_valido(gtin) is True

@pytest.mark.parametrize("gtin", [
    "73513536",        # GTIN-8 invalid check digit
    "012345678904",    # GTIN-12 invalid check digit
    "7891000315508",   # GTIN-13 invalid check digit
    "17891000315505",  # GTIN-14 invalid check digit
])
def test_gtin_valido_invalid_check_digit(gtin):
    assert gtin_valido(gtin) is False

@pytest.mark.parametrize("gtin", [
    "1234567",         # 7 digits
    "123456789",       # 9 digits
    "1234567890",      # 10 digits
    "12345678901",     # 11 digits
    "123456789012345", # 15 digits
])
def test_gtin_valido_invalid_lengths(gtin):
    assert gtin_valido(gtin) is False

@pytest.mark.parametrize("gtin", [
    "",
    None,
])
def test_gtin_valido_empty_or_none(gtin):
    assert gtin_valido(gtin) is False

@pytest.mark.parametrize("gtin", [
    "7891000-315507",    # GTIN-13 with hyphen
    "0123.456.789-05",   # GTIN-12 with dots and hyphen
    " A 7891000315507",  # GTIN-13 with spaces and letter
    "17891000315504\n",  # GTIN-14 with newline
])
def test_gtin_valido_with_non_digit_chars(gtin):
    assert gtin_valido(gtin) is True
