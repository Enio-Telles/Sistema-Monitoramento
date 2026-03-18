import pytest
import polars as pl
from fiscal_app.services.parquet_service import ParquetService, FilterCondition

@pytest.fixture
def service():
    return ParquetService()

@pytest.fixture
def mock_df():
    return pl.DataFrame({
        "str_col": ["Maçã", "banana", "Laranja", "", None],
        "num_col": [10.5, 20.0, 5.0, None, 15.2],
        "mixed_col": ["10", "20,5", "abc", None, ""]
    })

@pytest.mark.parametrize("cond, expected_indices", [
    # String operations (case insensitive)
    (FilterCondition("str_col", "contém", "aÇã"), [0]),
    (FilterCondition("str_col", "contém", "na"), [1]),
    (FilterCondition("str_col", "igual", "Laranja"), [2]),
    (FilterCondition("str_col", "igual", "laranja"), []), # Igual is exact
    (FilterCondition("str_col", "começa com", "maç"), [0]),
    (FilterCondition("str_col", "termina com", "ja"), [2]),

    # Null operations
    (FilterCondition("str_col", "é nulo", ""), [3, 4]), # "" and None
    (FilterCondition("str_col", "não é nulo", ""), [0, 1, 2]),
    (FilterCondition("num_col", "é nulo", ""), [3]), # Only None is null for float
    (FilterCondition("num_col", "não é nulo", ""), [0, 1, 2, 4]),

    # Numeric operations (handles comma/dot replacement)
    (FilterCondition("num_col", ">", "10,5"), [1, 4]),
    (FilterCondition("num_col", ">=", "10.5"), [0, 1, 4]),
    (FilterCondition("num_col", "<", "15.2"), [0, 2]),
    (FilterCondition("num_col", "<=", "15,2"), [0, 2, 4]),

    # Mixed column test (should try to parse as float for > >= < <=)
    (FilterCondition("mixed_col", ">", "15"), []), # "20,5" > 15
    (FilterCondition("mixed_col", "<", "15"), [0]), # "10" < 15, "abc" is invalid so None

    # Edge cases
    (FilterCondition("str_col", "invalid_op", "test"), []), # Falls back to equal
    (FilterCondition("mixed_col", ">", "invalid_num"), []), # invalid condition value -> falls back to equal
])
def test_build_expr(service, mock_df, cond, expected_indices):
    expr = service._build_expr(cond)

    # Get the indices of the resulting rows to compare
    # We join with original df to get the original row numbers,
    # but a simpler way is to just add a row_nr column first

    df_with_row_nr = mock_df.with_row_index("row_nr")
    result = df_with_row_nr.filter(expr)
    actual_indices = result["row_nr"].to_list()

    assert actual_indices == expected_indices

def test_build_expr_fallback_to_equal_for_invalid_numeric_values(service, mock_df):
    cond = FilterCondition("mixed_col", ">", "abc")
    expr = service._build_expr(cond)
    result = mock_df.with_row_index("row_nr").filter(expr)
    actual_indices = result["row_nr"].to_list()
    # It falls back to string equality: mixed_col == "abc"
    assert actual_indices == [2]
