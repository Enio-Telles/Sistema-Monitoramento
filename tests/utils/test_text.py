import pytest
from fiscal_app.utils.text import normalize_text

@pytest.mark.parametrize(
    "input_text, expected",
    [
        # None and empty
        (None, ""),
        ("", ""),
        ("   ", ""),

        # Case normalization
        ("texto", "TEXTO"),
        ("TeXtO", "TEXTO"),

        # Accents
        ("café", "CAFE"),
        ("pão", "PAO"),
        ("Ação", "ACAO"),
        ("áéíóú", "AEIOU"),
        ("âêô", "AEO"),
        ("ç", "C"),

        # Special characters
        ("!@#$%^&*()", ""),
        ("text-with-dash", "TEXT WITH DASH"),
        ("text_with_underscore", "TEXT WITH UNDERSCORE"),
        ("text.with.dot", "TEXT WITH DOT"),

        # Stopwords
        ("um texto de exemplo", "TEXTO EXEMPLO"),
        ("a casa com um telhado", "CASA TELHADO"),
        ("pão de queijo", "PAO QUEIJO"),
        ("venda de mercadoria", "VENDA MERCADORIA"),

        # Extra spaces
        ("  texto   com  espaços  ", "TEXTO ESPACOS"),

        # Numbers
        ("produto 123", "PRODUTO 123"),
        ("123 456", "123 456"),

        # Combination
        ("  Café com Leite!  ", "CAFE LEITE"),
        ("Ação de Graças (2023)", "ACAO GRACAS 2023"),
    ]
)
def test_normalize_text(input_text, expected):
    """Test the normalize_text function with various inputs."""
    assert normalize_text(input_text) == expected
