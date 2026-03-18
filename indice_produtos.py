"""
Módulo de geração de índice de produtos
--------------------------------------

Este módulo define uma função utilitária para construir uma lista indexada de
produtos a partir de um ``DataFrame`` contendo as características básicas de cada
produto. A lista indexada visa identificar unicamente um produto com base nos
campos fornecidos pelo pipeline (``codigo``, ``descricao``, ``descr_compl``,
``tipo_item``, ``ncm``, ``cest`` e ``gtin``) e registrar todas as unidades
encontradas para esse conjunto de atributos. Cada combinação distinta desses
campos recebe uma ``chave_produto`` sequencial para ser usada como chave de
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
identificador sequencial ``chave_produto`` é atribuído a cada linha do índice.

Essa função é desacoplada do restante do código do projeto para facilitar a
integração gradual. Ela pode ser invocada logo após a construção da tabela
``produtos`` em ``build_produtos_base`` e salva em um arquivo Parquet, por
exemplo ``indice_produtos_{cnpj}.parquet``. As demais tabelas (bloco_h, NFe,
NFCe, C170) podem então fazer ``merge`` ou ``join`` com essa lista indexada
utilizando os mesmos campos de chave para obter a ``chave_produto`` e,
posteriormente, correlacionar com o ``codigo_padrao``.
"""

from __future__ import annotations

from typing import Any, Iterable, List, Optional

import pandas as pd


def _normalize_unit_list(values: Iterable[Optional[str]]) -> List[str]:
    """Converte um iterável de unidades em uma lista única ordenada.

    Parâmetros
    ----------
    values: Iterable[Optional[str]]
        Sequência contendo valores de unidade coletados das linhas do
        ``DataFrame`` original. Valores ``None`` ou strings vazias são
        descartados.

    Retorna
    -------
    list[str]
        Lista ordenada (ordem lexicográfica) de unidades únicas.
    """
    cleaned = [str(v).strip() for v in values if pd.notna(v) and str(v).strip() != ""]
    return sorted(set(cleaned))


def criar_indice_produtos(produtos: pd.DataFrame) -> pd.DataFrame:
    """Gera uma lista indexada de produtos com base nas colunas chave.

    Este utilitário recebe um ``DataFrame`` contendo ao menos as colunas
    ``codigo``, ``descricao``, ``descr_compl``, ``tipo_item``, ``ncm``, ``cest``,
    ``gtin`` e ``unid``. Ele consolida linhas duplicadas (produtos com as
    mesmas características) e agrupa as unidades observadas na coluna
    ``lista_unidades``. Um identificador inteiro sequencial ``chave_produto`` é
    então atribuído a cada produto distinto.

    Parâmetros
    ----------
    produtos: pandas.DataFrame
        DataFrame com as colunas necessárias para a identificação de produtos.

    Retorna
    -------
    pandas.DataFrame
        DataFrame contendo as colunas:

        - ``chave_produto`` (int): identificador sequencial único.
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
    required_cols = {"codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"}
    missing = required_cols - set(produtos.columns)
    if missing:
        raise ValueError(
            f"O DataFrame de produtos deve conter as colunas {sorted(required_cols)}, mas está faltando: {sorted(missing)}"
        )

    # Certifique‑se de que as colunas chave são do tipo string para evitar que valores
    # numéricos causem distinções indesejadas durante o agrupamento.
    produtos_normalizado = produtos.copy()
    for col in ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]:
        produtos_normalizado[col] = produtos_normalizado[col].astype("string").str.strip()

    # Agrupa por todas as colunas chave exceto "unid" e consolida a lista de unidades
    agrupado = (
        produtos_normalizado.groupby(
            ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"], dropna=False
        )
        .agg(lista_unidades=("unid", _normalize_unit_list))
        .reset_index()
    )

    # Gera chave sequencial iniciando em 1
    agrupado.insert(0, "chave_produto", range(1, len(agrupado) + 1))

    return agrupado
