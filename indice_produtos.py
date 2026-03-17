"""
Módulo de geração de índice de produtos
--------------------------------------

Este módulo define uma função utilitária para construir uma lista indexada de
produtos a partir de um ``DataFrame`` contendo as características básicas de cada
produto. A lista indexada visa identificar unicamente um produto com base nos
campos fornecidos pelo pipeline (``codigo``, ``descricao``, ``descr_compl``,
``tipo_item``, ``ncm``, ``cest`` e ``gtin``) e registrar todas as unidades
encontradas para esse conjunto de atributos. Cada combinação distinta desses
campos recebe uma ``chave_produto`` em formato de hash para ser usada como chave de
referência entre tabelas.

Exemplo de uso
--------------

```python
import pandas as pd
from indice_produtos import criar_indice_produtos

# Carregue o DataFrame base de produtos (por exemplo, utilizando
# tabelas_auditorias.processing.build_produtos_base)
produtos_base = carregar_produtos(...)

# Crie o índice de produtos
indice = criar_indice_produtos(produtos_base)

# Resultado contém as colunas:
# ["chave_produto", "codigo", "descricao", "descr_compl", "tipo_item",
#  "ncm", "cest", "gtin", "lista_unidades"]
```

Implementação
-------------

Para criar o índice, o ``DataFrame`` de entrada é agrupado pelos campos
``codigo``, ``descricao``, ``descr_compl``, ``tipo_item``, ``ncm``, ``cest`` e
``gtin``. Em seguida, para cada grupo, gera‑se a lista de unidades encontradas
no campo ``unid``. O campo ``lista_unidades`` contém a lista ordenada das
unidades únicas observadas, ignorando nulos e strings vazias. Por fim, um
identificador hash ``chave_produto`` é atribuído a cada linha do índice, garantindo reprodutibilidade.
"""

from __future__ import annotations

import hashlib
import pandas as pd

CHAVE_COLS = [
    "codigo",
    "descricao",
    "descr_compl",
    "tipo_item",
    "ncm",
    "cest",
    "gtin",
]


def _clean_series(s: pd.Series) -> pd.Series:
    """Limpa a série, convertendo strings vazias e similares em NA."""
    s = s.astype("string").str.strip()
    s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "<NA>": pd.NA})
    return s


def _stable_hash_key(row: pd.Series) -> str:
    """Gera uma chave hash estável para as colunas-chave do produto."""
    raw = "||".join("" if pd.isna(row[c]) else str(row[c]) for c in CHAVE_COLS)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def criar_indice_produtos(df_produtos: pd.DataFrame) -> pd.DataFrame:
    """Gera uma lista indexada de produtos com base nas colunas chave.

    Este utilitário recebe um ``DataFrame`` contendo ao menos as colunas
    ``codigo``, ``descricao``, ``descr_compl``, ``tipo_item``, ``ncm``, ``cest``,
    ``gtin`` e ``unid``. Ele consolida linhas duplicadas (produtos com as
    mesmas características) e agrupa as unidades observadas na coluna
    ``lista_unidades``. Um identificador hash determinístico ``chave_produto`` é
    então atribuído a cada produto distinto.

    Parâmetros
    ----------
    df_produtos: pandas.DataFrame
        DataFrame com as colunas necessárias para a identificação de produtos.

    Retorna
    -------
    pandas.DataFrame
        DataFrame contendo as colunas:

        - ``chave_produto`` (str): identificador sequencial único em hash determinístico (SHA-1 prefixado a 16 caracteres).
        - ``codigo`` (string): código original do produto.
        - ``descricao`` (string): descrição principal.
        - ``descr_compl`` (string): descrição complementar.
        - ``tipo_item`` (string): tipo do item.
        - ``ncm`` (string): código NCM.
        - ``cest`` (string): código CEST.
        - ``gtin`` (string): código GTIN.
        - ``lista_unidades`` (list[str]): lista de unidades distintas
          encontradas para esta combinação de campos.
    """
    base = df_produtos.copy()

    for col in CHAVE_COLS + ["unid"]:
        if col not in base.columns:
            base[col] = pd.Series([pd.NA] * len(base), dtype="string")
        base[col] = _clean_series(base[col])

    agrupado = (
        base.groupby(CHAVE_COLS, dropna=False)
        .agg(
            lista_unidades=("unid", lambda x: sorted({str(v) for v in x.dropna() if str(v).strip()}))
        )
        .reset_index()
    )

    agrupado["chave_produto"] = agrupado.apply(_stable_hash_key, axis=1)

    cols_saida = [
        "chave_produto",
        "codigo",
        "descricao",
        "descr_compl",
        "tipo_item",
        "ncm",
        "cest",
        "gtin",
        "lista_unidades",
    ]
    return agrupado[cols_saida].sort_values(
        ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"],
        kind="stable",
    ).reset_index(drop=True)

