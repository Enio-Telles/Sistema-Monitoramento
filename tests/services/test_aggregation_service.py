import pytest
from pathlib import Path
import polars as pl
from fiscal_app.services.aggregation_service import AggregationService

def test_load_editable_table_target_exists(tmp_path):
    service = AggregationService(log_file=tmp_path / "log.json")
    cnpj = "12345678901234"
    cnpj_dir = tmp_path / cnpj

    target_path = service.target_table_path(cnpj_dir, cnpj)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.touch()

    result = service.load_editable_table(cnpj_dir, cnpj)

    assert result == target_path

def test_load_editable_table_source_not_exists(tmp_path):
    service = AggregationService(log_file=tmp_path / "log.json")
    cnpj = "12345678901234"
    cnpj_dir = tmp_path / cnpj

    with pytest.raises(FileNotFoundError, match="A tabela de origem para agregação não foi encontrada."):
        service.load_editable_table(cnpj_dir, cnpj)

def test_load_editable_table_source_exists(tmp_path):
    service = AggregationService(log_file=tmp_path / "log.json")
    cnpj = "12345678901234"
    cnpj_dir = tmp_path / cnpj

    source_path = service.source_table_path(cnpj_dir, cnpj)
    source_path.parent.mkdir(parents=True, exist_ok=True)

    df = pl.DataFrame({"a": [1, 2, 3]})
    df.write_parquet(source_path)

    target_path = service.target_table_path(cnpj_dir, cnpj)

    assert not target_path.exists()

    result = service.load_editable_table(cnpj_dir, cnpj)

    assert result == target_path
    assert target_path.exists()

    # Verify content
    target_df = pl.read_parquet(target_path)
    assert target_df.equals(df)
