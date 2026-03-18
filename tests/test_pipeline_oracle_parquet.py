import pytest
from pipeline_oracle_parquet import extract_bind_names

def test_extract_bind_names_simple():
    sql = "SELECT * FROM table WHERE id = :id AND name = :NAME"
    assert extract_bind_names(sql) == ['id', 'name']

def test_extract_bind_names_ignores_posix_classes():
    sql = "SELECT * FROM table WHERE field ~ '[:alnum:]' AND other ~ '[:alpha:]'"
    assert extract_bind_names(sql) == []

def test_extract_bind_names_with_numbers():
    sql = "SELECT * FROM table WHERE value = :value123"
    assert extract_bind_names(sql) == ['value123']

def test_extract_bind_names_deduplicates_case_insensitive():
    sql = "SELECT * FROM table WHERE id = :id AND old_id = :ID"
    # id is already seen, so :ID is not added.
    assert extract_bind_names(sql) == ['id']

def test_extract_bind_names_preserves_order():
    sql = "SELECT * FROM table WHERE z = :z AND a = :a AND b = :b"
    assert extract_bind_names(sql) == ['z', 'a', 'b']

def test_extract_bind_names_no_binds():
    sql = "SELECT * FROM table"
    assert extract_bind_names(sql) == []

def test_extract_bind_names_invalid_binds():
    sql = "SELECT * FROM table WHERE field = :123invalid OR other = :!@#"
    assert extract_bind_names(sql) == []

def test_extract_bind_names_at_start():
    sql = ":bind_at_start"
    assert extract_bind_names(sql) == ['bind_at_start']

def test_extract_bind_names_bracket_at_end():
    sql = "SELECT * FROM table WHERE id = :id AND something = '[:punct:]'"
    assert extract_bind_names(sql) == ['id']

def test_extract_bind_names_mixed_casing():
    sql = "SELECT * FROM table WHERE value = :VaLuE AND value2 = :VALUE"
    assert extract_bind_names(sql) == ['value']

def test_extract_bind_names_multiple_bracketed_classes():
    sql = "[:alnum:] [:alpha:] :valid_bind [:digit:]"
    assert extract_bind_names(sql) == ['valid_bind']
