import pytest
from decimal import Decimal
from tabelas_auditorias.utils import normalize_scalar

def test_normalize_scalar_none():
    assert normalize_scalar(None) is None

def test_normalize_scalar_decimal_integral():
    assert normalize_scalar(Decimal('10.0')) == 10
    assert isinstance(normalize_scalar(Decimal('10.0')), int)

def test_normalize_scalar_decimal_float():
    assert normalize_scalar(Decimal('10.5')) == 10.5
    assert isinstance(normalize_scalar(Decimal('10.5')), float)

def test_normalize_scalar_read():
    class Readable:
        def read(self):
            return "data"

    assert normalize_scalar(Readable()) == "data"

def test_normalize_scalar_read_exception():
    class FailingReadable:
        def read(self):
            raise ValueError("Cannot read")

        def __str__(self):
            return "FailingReadableObject"

    assert normalize_scalar(FailingReadable()) == "FailingReadableObject"

def test_normalize_scalar_int_conversion_exception():
    class FailingDecimal(Decimal):
        def __int__(self):
            raise ValueError("Cannot convert to int")

    assert normalize_scalar(FailingDecimal('10.0')) == 10.0
    assert isinstance(normalize_scalar(FailingDecimal('10.0')), float)

def test_normalize_scalar_other():
    assert normalize_scalar("string") == "string"
    assert normalize_scalar(10) == 10
    assert normalize_scalar(10.5) == 10.5
