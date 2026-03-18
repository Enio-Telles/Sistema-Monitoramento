from fiscal_app.utils.text import remove_accents

def test_remove_acentos_retorna_none_para_none():
    assert remove_accents(None) is None

def test_remove_acentos_retorna_vazio_para_vazio():
    assert remove_accents("") == ""

def test_remove_acentos_remove_acentos_de_vogais():
    assert remove_accents("áéíóú") == "aeiou"
    assert remove_accents("ÀÈÌÒÙ") == "AEIOU"
    assert remove_accents("âêîôû") == "aeiou"
    assert remove_accents("äëïöü") == "aeiou"

def test_remove_acentos_remove_til_e_cedilha():
    assert remove_accents("çãõ") == "cao"
    assert remove_accents("ÇÃÕ") == "CAO"

def test_remove_acentos_mantem_string_sem_acentos():
    assert remove_accents("texto sem acento") == "texto sem acento"
    assert remove_accents("1234567890") == "1234567890"
    assert remove_accents("!@#$%^&*()") == "!@#$%^&*()"

def test_remove_acentos_lida_com_palavras_compostas():
    assert remove_accents("maçã") == "maca"
    assert remove_accents("Pão de Açúcar") == "Pao de Acucar"
    assert remove_accents("João Ninguém") == "Joao Ninguem"

def test_remove_acentos_converte_outros_tipos_para_string_sem_acentos():
    # A função tem `str(text)`, então lidaria com inteiros convertendo para string
    assert remove_accents(123) == "123"
    assert remove_accents(45.6) == "45.6"
