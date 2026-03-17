import pytest
from fiscal_app.utils.text import natural_sort_key

def test_natural_sort_key_basic():
    # Test strings with numbers embedded
    assert natural_sort_key("item1") == ["item", 1, ""]
    assert natural_sort_key("item10") == ["item", 10, ""]
    assert natural_sort_key("item2") == ["item", 2, ""]

def test_natural_sort_key_sorting():
    # Test that it correctly sorts a list of strings
    unsorted_list = ["item10", "item2", "item1", "item100", "item20"]
    sorted_list = sorted(unsorted_list, key=natural_sort_key)
    assert sorted_list == ["item1", "item2", "item10", "item20", "item100"]

def test_natural_sort_key_edge_cases():
    # Test None
    assert natural_sort_key(None) == [""]

    # Test empty string
    assert natural_sort_key("") == [""]

    # Test purely numeric string
    assert natural_sort_key("123") == ["", 123, ""]

    # Test string without numbers
    assert natural_sort_key("abc") == ["abc"]

    # Test uppercase and lowercase mixed string (should lowercase)
    assert natural_sort_key("ItEm1") == ["item", 1, ""]

def test_natural_sort_key_complex():
    # Test strings with multiple numbers and characters
    unsorted_list = ["v1.2.10", "v1.2.2", "v1.10.1", "v2.0.0", "v1.2.1"]
    sorted_list = sorted(unsorted_list, key=natural_sort_key)
    assert sorted_list == ["v1.2.1", "v1.2.2", "v1.2.10", "v1.10.1", "v2.0.0"]
