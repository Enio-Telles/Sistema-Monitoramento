import pytest
from fiscal_app.services.aggregation_service import AggregationService

def test_build_aggregated_row_less_than_two_rows():
    service = AggregationService()

    # Empty list
    with pytest.raises(ValueError, match="Selecione pelo menos duas linhas para agregar."):
        service.build_aggregated_row([])

    # Single row
    with pytest.raises(ValueError, match="Selecione pelo menos duas linhas para agregar."):
        service.build_aggregated_row([{"lista_codigos": "[COD1; 1]"}])

def test_build_aggregated_row_no_parsed_codes():
    service = AggregationService()

    # Rows without lista_codigos
    with pytest.raises(ValueError, match="Não foi possível identificar códigos nas linhas selecionadas."):
        service.build_aggregated_row([
            {"descricao": "Item 1"},
            {"descricao": "Item 2"}
        ])

    # Rows with empty lista_codigos
    with pytest.raises(ValueError, match="Não foi possível identificar códigos nas linhas selecionadas."):
        service.build_aggregated_row([
            {"lista_codigos": []},
            {"lista_codigos": None}
        ])
