import sys
from unittest.mock import MagicMock

# Mock pandas before importing normalizar_texto because it's not installed in the environment
mock_pd = MagicMock()
sys.modules["pandas"] = mock_pd

import pytest
from tabelas_auditorias.utils import normalizar_texto

def test_normalizar_texto_none():
    assert normalizar_texto(None) is None

def test_normalizar_texto_empty():
    assert normalizar_texto("") is None
    assert normalizar_texto("   ") is None

def test_normalizar_texto_stopwords_only():
    assert normalizar_texto("o a os as") is None
    assert normalizar_texto("DE DA DO DAS DOS") is None

def test_normalizar_texto_accents():
    # ÁÉÍÓÚ ÀÈÌÒÙ ÂÊÎÔÛ ÃÕ Ç -> AEIOU AEIOU AEIOU AO C
    assert normalizar_texto("CAFÉ") == "CAFE"
    assert normalizar_texto("AÇÃO") == "ACAO"
    assert normalizar_texto("PÁ PARA") == "PA"  # "PARA" is a stopword

def test_normalizar_texto_special_characters():
    assert normalizar_texto("PRODUTO-TESTE @2023!") == "PRODUTO TESTE 2023"
    assert normalizar_texto("PRODUTO_ABC.XYZ") == "PRODUTO ABC XYZ"

def test_normalizar_texto_multiple_spaces_and_case():
    assert normalizar_texto("  produto   MUITO   bom  ") == "PRODUTO MUITO BOM"
    assert normalizar_texto("AbC dEf") == "ABC DEF"

def test_normalizar_texto_typical_description():
    # STOPWORDS = {"A", "AS", "O", "OS", "DE", "DA", "DO", "DAS", "DOS", "COM", "PARA", "POR", "E", "EM", "NA", "NO", "NAS", "NOS", "UM", "UMA"}
    input_text = "ARROZ INTEGRAL TIPO 1 PACOTE COM 5KG"
    # ARROZ INTEGRAL TIPO 1 PACOTE 5KG ("COM" is removed)
    expected = "ARROZ INTEGRAL TIPO 1 PACOTE 5KG"
    assert normalizar_texto(input_text) == expected

def test_normalizar_texto_no_valid_tokens_after_filtering():
    assert normalizar_texto("... @@@ ---") is None
    assert normalizar_texto("para o") is None
