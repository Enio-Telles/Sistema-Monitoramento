from pathlib import Path
import pandas as pd
from indice_produtos import criar_indice_produtos
from .constants import FINAL_SCHEMA, CODIGOS_DESAG_SCHEMA
from .utils import (
    coalesce_columns_ci,
    load_parquet_if_exists,
    normalizar_texto,
    normalizar_unidade,
    somente_digitos,
    gtin_valido,
    ncm_valido,
    cest_valido,
    codigo_num_sort,
    unique_sorted,
    empty_with_schema
)

def canonicalize_nfe_like(df: pd.DataFrame, fonte: str) -> pd.DataFrame:
    data_mov = pd.to_datetime(
        coalesce_columns_ci(df, ["dhemi", "dh_emi", "dh_emi_utc", "emi_dh", "data_emissao"]),
        errors="coerce",
    )

    return pd.DataFrame(
        {
            "fonte": fonte,
            "codigo": coalesce_columns_ci(df, ["prod_cprod", "cod_item", "codigo_produto"]).astype("string"),
            "descricao": coalesce_columns_ci(df, ["prod_xprod", "descr_item", "descricao_produto"]).astype("string"),
            "descr_compl": pd.Series([None] * len(df), index=df.index, dtype="string"),
            "tipo_item": pd.Series([None] * len(df), index=df.index, dtype="string"),
            "ncm": coalesce_columns_ci(df, ["prod_ncm", "cod_ncm", "ncm"]).astype("string"),
            "cest": coalesce_columns_ci(df, ["prod_cest", "cest"]).astype("string"),
            "gtin": coalesce_columns_ci(df, ["prod_cean", "prod_ceantrib", "prod_cbarra", "prod_ean", "prod_eantrib"]).astype("string"),
            "unid": coalesce_columns_ci(df, ["prod_ucom", "prod_utrib", "unid"]).astype("string"),
            "data_mov": data_mov,
        }
    )


def canonicalize_c170(df: pd.DataFrame) -> pd.DataFrame:
    data_mov = pd.to_datetime(coalesce_columns_ci(df, ["dt_doc"]), errors="coerce")
    return pd.DataFrame(
        {
            "fonte": "c170",
            "codigo": coalesce_columns_ci(df, ["cod_item"]).astype("string"),
            "descricao": coalesce_columns_ci(df, ["descr_item"]).astype("string"),
            "descr_compl": coalesce_columns_ci(df, ["descr_compl"]).astype("string"),
            "tipo_item": coalesce_columns_ci(df, ["tipo_item"]).astype("string"),
            "ncm": coalesce_columns_ci(df, ["cod_ncm"]).astype("string"),
            "cest": coalesce_columns_ci(df, ["cest"]).astype("string"),
            "gtin": coalesce_columns_ci(df, ["cod_barra"]).astype("string"),
            "unid": coalesce_columns_ci(df, ["unid"]).astype("string"),
            "data_mov": data_mov,
        }
    )


def canonicalize_bloco_h(df: pd.DataFrame) -> pd.DataFrame:
    data_mov = pd.to_datetime(coalesce_columns_ci(df, ["dt_inv"]), errors="coerce")
    return pd.DataFrame(
        {
            "fonte": "bloco_h",
            "codigo": coalesce_columns_ci(df, ["codigo_produto"]).astype("string"),
            "descricao": coalesce_columns_ci(df, ["descricao_produto"]).astype("string"),
            "descr_compl": coalesce_columns_ci(df, ["obs_complementar"]).astype("string"),
            "tipo_item": coalesce_columns_ci(df, ["tipo_item"]).astype("string"),
            "ncm": coalesce_columns_ci(df, ["cod_ncm"]).astype("string"),
            "cest": coalesce_columns_ci(df, ["cest"]).astype("string"),
            "gtin": coalesce_columns_ci(df, ["cod_barra"]).astype("string"),
            "unid": coalesce_columns_ci(df, ["unidade_medida"]).astype("string"),
            "data_mov": data_mov,
        }
    )


def pick_mode_by_group(df: pd.DataFrame, group_col: str, value_col: str, alias: str) -> pd.DataFrame:
    base = df[[group_col, value_col, "data_mov"]].copy()
    base = base[base[value_col].notna() & (base[value_col].astype(str).str.strip() != "")]
    if base.empty:
        return pd.DataFrame(columns=[group_col, alias])

    agg = (
        base.groupby([group_col, value_col], dropna=False)
        .agg(freq_valor=(value_col, "size"), ult_data_valor=("data_mov", "max"))
        .reset_index()
    )
    agg[value_col] = agg[value_col].astype("string")
    agg = agg.sort_values(
        by=[group_col, "freq_valor", "ult_data_valor", value_col],
        ascending=[True, False, False, True],
        kind="stable",
    )
    agg = agg.drop_duplicates(subset=[group_col], keep="first")
    return agg[[group_col, value_col]].rename(columns={value_col: alias})


def format_codigo_lista(codigo: str, qtd_descricoes_diferentes: int) -> str:
    return f"[{codigo}; {int(qtd_descricoes_diferentes)}]"


def alinhar_nomenclatura_documento(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "descricao_normalizada": "descrição_normalizada",
        "ncm_padrao": "NCM_padrao",
        "cest_padrao": "CEST_padrao",
        "gtin_padrao": "GTIN_padrao",
    }
    df = df.rename(columns=rename_map)
    ordered_cols = [
        "descrição_normalizada",
        "descricao",
        "codigo_padrao",
        "qtd_codigos",
        "lista_codigos",
        "lista_tipo_item",
        "lista_ncm",
        "lista_cest",
        "lista_gtin",
        "lista_unid",
        "tipo_item_padrao",
        "NCM_padrao",
        "CEST_padrao",
        "GTIN_padrao",
        "lista_fontes",
        "lista_descricoes",
        "lista_descricoes_normalizadas",
        "descricao_padrao",
        "verificado",
    ]
    existing = [c for c in ordered_cols if c in df.columns]
    return df[existing].copy()


def build_produtos_base(pasta_cnpj: Path, cnpj: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []

    nfe = load_parquet_if_exists(pasta_cnpj / f"nfe_{cnpj}.parquet")
    nfce = load_parquet_if_exists(pasta_cnpj / f"nfce_{cnpj}.parquet")
    c170 = load_parquet_if_exists(pasta_cnpj / f"c170_simplificada_{cnpj}.parquet")
    bloco_h = load_parquet_if_exists(pasta_cnpj / f"bloco_h_{cnpj}.parquet")

    if nfe is not None:
        frames.append(canonicalize_nfe_like(nfe, "nfe"))
    if nfce is not None:
        frames.append(canonicalize_nfe_like(nfce, "nfce"))
    if c170 is not None:
        frames.append(canonicalize_c170(c170))
    if bloco_h is not None:
        frames.append(canonicalize_bloco_h(bloco_h))

    if not frames:
        return pd.DataFrame(columns=["fonte", "codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid", "data_mov"])

    produtos = pd.concat(frames, ignore_index=True)
    for col in ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]:
        produtos[col] = produtos[col].astype("string").str.strip()
    return produtos



def _normalizar_campos_indice(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in ["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin", "unid"]:
        if col not in out.columns:
            out[col] = pd.Series([None] * len(out), dtype="string")
        out[col] = out[col].astype("string").str.strip()

    out["ncm"] = out["ncm"].map(lambda x: somente_digitos(x) if pd.notna(x) else x).astype("string")
    out["ncm"] = out["ncm"].where(out["ncm"].map(ncm_valido), pd.NA)

    out["cest"] = out["cest"].map(lambda x: somente_digitos(x) if pd.notna(x) else x).astype("string")
    out["cest"] = out["cest"].where(out["cest"].map(cest_valido), pd.NA)

    out["gtin"] = out["gtin"].map(lambda x: somente_digitos(x) if pd.notna(x) else x).astype("string")
    out["gtin"] = out["gtin"].where(out["gtin"].map(gtin_valido), pd.NA)

    out["unid"] = out["unid"].map(normalizar_unidade).astype("string")
    return out


def _gerar_indice_produtos(produtos: pd.DataFrame) -> pd.DataFrame:
    base = _normalizar_campos_indice(produtos)
    return criar_indice_produtos(base)


def _montar_mapeamento_detalhado(
    produtos: pd.DataFrame,
    codigo_padrao: pd.DataFrame,
    mapa_desag: pd.DataFrame,
    descricao_representativa: pd.DataFrame,
) -> pd.DataFrame:
    mapeamento = produtos[["codigo", "descricao_normalizada"]].drop_duplicates().copy()
    mapeamento = mapeamento.rename(columns={"codigo": "codigo_original"})

    mapeamento = mapeamento.merge(codigo_padrao, on="descricao_normalizada", how="left")

    if not mapa_desag.empty:
        mapeamento = mapeamento.merge(
            mapa_desag[["codigo", "descricao_normalizada", "codigo_desagregado"]],
            left_on=["codigo_original", "descricao_normalizada"],
            right_on=["codigo", "descricao_normalizada"],
            how="left",
        ).drop(columns=["codigo"])
    else:
        mapeamento["codigo_desagregado"] = pd.Series([None] * len(mapeamento), dtype="string")

    def categorizar(row):
        orig = str(row["codigo_original"])
        padrao = str(row["codigo_padrao"])
        desag = row["codigo_desagregado"]

        if pd.notna(desag):
            return str(desag), "SEGREGADO", f"Código {orig} segregado em {desag}"
        if orig != padrao:
            return padrao, "AGRUPADO", f"Código {orig} agrupado sob {padrao}"
        return padrao, "REPRESENTANTE", f"Código {orig} mantido como representante do grupo"

    mapeamento[["codigo_final", "situacao", "detalhe"]] = mapeamento.apply(
        lambda row: pd.Series(categorizar(row)),
        axis=1,
    )

    mapeamento = mapeamento.merge(
        descricao_representativa, on="descricao_normalizada", how="left"
    ).rename(columns={"descricao": "descricao_final"})

    return mapeamento[
        [
            "codigo_original",
            "descricao_normalizada",
            "codigo_padrao",
            "codigo_desagregado",
            "codigo_final",
            "descricao_final",
            "situacao",
            "detalhe",
        ]
    ].sort_values(["situacao", "codigo_original", "descricao_normalizada"], kind="stable")


def _enriquecer_indice_com_codigo_consolidado(
    indice_produtos: pd.DataFrame,
    mapeamento_detalhado: pd.DataFrame,
) -> pd.DataFrame:
    indice = indice_produtos.copy()
    indice["descricao_normalizada"] = indice["descricao"].map(normalizar_texto).astype("string")

    indice = indice.merge(
        mapeamento_detalhado[
            ["codigo_original", "descricao_normalizada", "codigo_final", "situacao", "descricao_final"]
        ],
        left_on=["codigo", "descricao_normalizada"],
        right_on=["codigo_original", "descricao_normalizada"],
        how="left",
    ).drop(columns=["codigo_original"])

    return indice.sort_values(["codigo", "descricao"], kind="stable").reset_index(drop=True)


def _aplicar_codigo_consolidado_em_tabela(
    df_original: pd.DataFrame,
    df_canonico: pd.DataFrame,
    mapeamento_detalhado: pd.DataFrame,
    indice_produtos: pd.DataFrame,
    colunas_saida_extra: dict[str, str] | None = None,
) -> pd.DataFrame:
    base = df_original.copy()
    canon = _normalizar_campos_indice(df_canonico.copy())

    canon["descricao_normalizada"] = canon["descricao"].map(normalizar_texto).astype("string")

    canon = canon.merge(
        indice_produtos[
            ["chave_produto", "codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"]
        ],
        on=["codigo", "descricao", "descr_compl", "tipo_item", "ncm", "cest", "gtin"],
        how="left",
    )

    canon = canon.merge(
        mapeamento_detalhado[
            ["codigo_original", "descricao_normalizada", "codigo_final", "situacao", "descricao_final"]
        ],
        left_on=["codigo", "descricao_normalizada"],
        right_on=["codigo_original", "descricao_normalizada"],
        how="left",
    ).drop(columns=["codigo_original"])

    base["chave_produto"] = canon["chave_produto"].values
    base["codigo_consolidado"] = canon["codigo_final"].values
    base["situacao_codigo_consolidado"] = canon["situacao"].values
    base["descricao_consolidada"] = canon["descricao_final"].values

    if colunas_saida_extra:
        for origem, destino in colunas_saida_extra.items():
            if origem in canon.columns:
                base[destino] = canon[origem].values

    return base


def materializar_tabelas_consolidacao(pasta_cnpj: Path, cnpj: str) -> dict[str, Path]:
    produtos_dir = pasta_cnpj / "produtos"
    produtos_dir.mkdir(parents=True, exist_ok=True)

    produtos = build_produtos_base(pasta_cnpj, cnpj)
    if produtos.empty:
        path_unif = produtos_dir / f"tabela_descricoes_unificadas_{cnpj}.parquet"
        path_desag = produtos_dir / f"codigos_desagregados_{cnpj}.parquet"
        path_final = produtos_dir / f"tabela_produtos_{cnpj}.parquet"
        path_mapa = produtos_dir / f"mapeamento_codigos_{cnpj}.parquet"
        path_mapa_det = produtos_dir / f"mapeamento_codigos_detalhado_{cnpj}.parquet"
        path_indice = produtos_dir / f"indice_produtos_{cnpj}.parquet"
        path_indice_cons = produtos_dir / f"indice_produtos_consolidado_{cnpj}.parquet"
        empty_with_schema(FINAL_SCHEMA).to_parquet(path_unif, index=False)
        empty_with_schema(CODIGOS_DESAG_SCHEMA).to_parquet(path_desag, index=False)
        empty_with_schema(FINAL_SCHEMA).to_parquet(path_final, index=False)

        import pandas as pd
        pd.DataFrame(columns=[
            "codigo_original", "codigo_final", "descricao_final", "situacao", "detalhe"
        ]).to_parquet(path_mapa, index=False)

        pd.DataFrame(columns=[
            "codigo_original", "descricao_normalizada", "codigo_padrao", "codigo_desagregado",
            "codigo_final", "descricao_final", "situacao", "detalhe"
        ]).to_parquet(path_mapa_det, index=False)

        pd.DataFrame(columns=[
            "chave_produto", "codigo", "descricao", "descr_compl",
            "tipo_item", "ncm", "cest", "gtin", "lista_unidades"
        ]).to_parquet(path_indice, index=False)

        pd.DataFrame(columns=[
            "chave_produto", "codigo", "descricao", "descr_compl",
            "tipo_item", "ncm", "cest", "gtin", "lista_unidades",
            "descricao_normalizada", "codigo_final", "situacao", "descricao_final"
        ]).to_parquet(path_indice_cons, index=False)

        return {
            "tabela_descricoes_unificadas": path_unif,
            "codigos_desagregados": path_desag,
            "tabela_produtos": path_final,
            "mapeamento_codigos": path_mapa,
            "mapeamento_codigos_detalhado": path_mapa_det,
            "indice_produtos": path_indice,
            "indice_produtos_consolidado": path_indice_cons,
        }

    produtos = produtos[produtos["codigo"].notna() & produtos["descricao"].notna()].copy()
    produtos["descricao_normalizada"] = produtos["descricao"].map(normalizar_texto).astype("string")
    produtos["unid_padronizada"] = produtos["unid"].map(normalizar_unidade).astype("string")
    produtos["gtin_limpo"] = produtos["gtin"].map(lambda x: somente_digitos(x) if gtin_valido(x) else None).astype("string")
    produtos["ncm_limpo"] = produtos["ncm"].map(lambda x: somente_digitos(x) if ncm_valido(x) else None).astype("string")
    produtos["cest_limpo"] = produtos["cest"].map(lambda x: somente_digitos(x) if cest_valido(x) else None).astype("string")
    produtos["codigo_num_sort"] = produtos["codigo"].map(codigo_num_sort)
    produtos["score_integridade"] = (
        produtos["tipo_item"].notna().astype(int) * 4
        + produtos["gtin_limpo"].notna().astype(int) * 3
        + produtos["ncm_limpo"].notna().astype(int) * 2
        + produtos["cest_limpo"].notna().astype(int)
    )

    produtos = produtos[produtos["descricao_normalizada"].notna()].copy()

    codigo_stats = (
        produtos.groupby("codigo", dropna=False)
        .agg(qtd_descricoes_diferentes=("descricao_normalizada", "nunique"))
        .reset_index()
    )

    produtos = produtos.merge(codigo_stats, on="codigo", how="left")
    produtos["codigo_lista_fmt"] = produtos.apply(
        lambda row: format_codigo_lista(str(row["codigo"]), int(row["qtd_descricoes_diferentes"])),
        axis=1,
    )

    candidatos = (
        produtos.groupby(["descricao_normalizada", "codigo"], dropna=False)
        .agg(
            frequencia_codigo_no_grupo=("codigo", "size"),
            score_integridade_codigo=("score_integridade", "max"),
            data_ult_mov_codigo_grupo=("data_mov", "max"),
            codigo_num_sort=("codigo_num_sort", "min"),
        )
        .reset_index()
    )
    candidatos = candidatos.sort_values(
        by=[
            "descricao_normalizada",
            "frequencia_codigo_no_grupo",
            "score_integridade_codigo",
            "data_ult_mov_codigo_grupo",
            "codigo_num_sort",
            "codigo",
        ],
        ascending=[True, False, False, False, True, True],
        kind="stable",
    )
    codigo_padrao = candidatos.drop_duplicates(subset=["descricao_normalizada"], keep="first")[["descricao_normalizada", "codigo"]]
    codigo_padrao = codigo_padrao.rename(columns={"codigo": "codigo_padrao"})

    descricao_representativa = pick_mode_by_group(produtos, "descricao_normalizada", "descricao", "descricao")
    tipo_item_padrao = pick_mode_by_group(produtos, "descricao_normalizada", "tipo_item", "tipo_item_padrao")
    ncm_padrao = pick_mode_by_group(produtos, "descricao_normalizada", "ncm_limpo", "ncm_padrao")
    cest_padrao = pick_mode_by_group(produtos, "descricao_normalizada", "cest_limpo", "cest_padrao")
    gtin_padrao = pick_mode_by_group(produtos, "descricao_normalizada", "gtin_limpo", "gtin_padrao")

    produtos["lista_descricoes"] = produtos["descricao"].apply(lambda x: [x] if x else [])
    produtos["descricao_padrao"] = produtos["descricao_normalizada"]

    tabela_descricoes_unificadas = (
        produtos.groupby("descricao_normalizada", dropna=False)
        .agg(
            lista_codigos=("codigo_lista_fmt", unique_sorted),
            lista_tipo_item=("tipo_item", unique_sorted),
            lista_ncm=("ncm_limpo", unique_sorted),
            lista_cest=("cest_limpo", unique_sorted),
            lista_gtin=("gtin_limpo", unique_sorted),
            lista_unid=("unid_padronizada", unique_sorted),
            lista_fontes=("fonte", unique_sorted),
            lista_descricoes=("lista_descricoes", lambda x: sorted(set(y for lst in x for y in lst))),
            lista_descricoes_normalizadas=("descricao_normalizada", lambda x: sorted(set(x))),
            qtd_codigos=("codigo_lista_fmt", "nunique"),
            descricao_padrao=("descricao_padrao", "first"),
        )
        .reset_index()
    )

    tabela_descricoes_unificadas = tabela_descricoes_unificadas.merge(descricao_representativa, on="descricao_normalizada", how="left")
    tabela_descricoes_unificadas = tabela_descricoes_unificadas.merge(codigo_padrao, on="descricao_normalizada", how="left")
    for extra in [tipo_item_padrao, ncm_padrao, cest_padrao, gtin_padrao]:
        tabela_descricoes_unificadas = tabela_descricoes_unificadas.merge(extra, on="descricao_normalizada", how="left")

    tabela_descricoes_unificadas = tabela_descricoes_unificadas[
        [
            "descricao_normalizada",
            "descricao",
            "codigo_padrao",
            "qtd_codigos",
            "lista_codigos",
            "lista_tipo_item",
            "lista_ncm",
            "lista_cest",
            "lista_gtin",
            "lista_unid",
            "tipo_item_padrao",
            "ncm_padrao",
            "cest_padrao",
            "gtin_padrao",
            "lista_fontes",
            "lista_descricoes",
            "lista_descricoes_normalizadas",
            "descricao_padrao",
        ]
    ].sort_values(["descricao_normalizada", "descricao"], kind="stable")
    tabela_descricoes_unificadas["verificado"] = False
    tabela_descricoes_unificadas = alinhar_nomenclatura_documento(tabela_descricoes_unificadas)

    codigos_ambiguos = set(codigo_stats.loc[codigo_stats["qtd_descricoes_diferentes"] > 1, "codigo"].astype(str))
    mapa_desag = produtos[["codigo", "descricao_normalizada"]].drop_duplicates().copy()
    mapa_desag = mapa_desag[mapa_desag["codigo"].astype(str).isin(codigos_ambiguos)].copy()
    if not mapa_desag.empty:
        mapa_desag = mapa_desag.sort_values(["codigo", "descricao_normalizada"], kind="stable")
        mapa_desag["seq_desag"] = mapa_desag.groupby("codigo").cumcount() + 1
        mapa_desag["codigo_desagregado"] = mapa_desag.apply(
            lambda row: f"{row['codigo']}_separado_{int(row['seq_desag']):02d}", axis=1
        )
    else:
        mapa_desag = pd.DataFrame(columns=["codigo", "descricao_normalizada", "seq_desag", "codigo_desagregado"])

    if mapa_desag.empty:
        codigos_desagregados = empty_with_schema(CODIGOS_DESAG_SCHEMA)
    else:
        base_codigos_desag = produtos.merge(
            mapa_desag[["codigo", "descricao_normalizada", "codigo_desagregado"]],
            on=["codigo", "descricao_normalizada"],
            how="inner",
        )
        desc_cod_desag = pick_mode_by_group(base_codigos_desag, "codigo_desagregado", "descricao", "descricao")
        codigos_desagregados = (
            base_codigos_desag.groupby("codigo_desagregado", dropna=False)
            .agg(
                lista_tipo_item=("tipo_item", unique_sorted),
                lista_ncm=("ncm_limpo", unique_sorted),
                lista_cest=("cest_limpo", unique_sorted),
                lista_gtin=("gtin_limpo", unique_sorted),
                lista_unid=("unid_padronizada", unique_sorted),
                lista_descricoes_normalizadas=("descricao_normalizada", lambda x: sorted(set(x))),
                qtd_codigos=("codigo_desagregado", "nunique"),
                descricao_padrao=("descricao", "first"),
            )
            .reset_index()
        )
        codigos_desagregados = codigos_desagregados.merge(desc_cod_desag, on="codigo_desagregado", how="left")
        codigos_desagregados = codigos_desagregados[
            [
                "codigo_desagregado",
                "descricao",
                "lista_tipo_item",
                "lista_ncm",
                "lista_cest",
                "lista_gtin",
                "lista_unid",
                "descricao_padrao",
            ]
        ].sort_values(["codigo_desagregado"], kind="stable")

    replacement_map = {
        (str(row["codigo"]), str(row["descricao_normalizada"])): str(row["codigo_desagregado"])
        for _, row in mapa_desag.iterrows()
    }

    def montar_lista_codigos_desag(desc_norm: str) -> list[str]:
        subset = produtos.loc[produtos["descricao_normalizada"] == desc_norm, ["codigo", "qtd_descricoes_diferentes"]].drop_duplicates()
        saida = []
        for _, row in subset.sort_values(["codigo"], kind="stable").iterrows():
            codigo_original = str(row["codigo"])
            codigo_final = replacement_map.get((codigo_original, str(desc_norm)), codigo_original)
            qtd = 1 if codigo_final != codigo_original else int(row["qtd_descricoes_diferentes"])
            saida.append(format_codigo_lista(codigo_final, qtd))
        return unique_sorted(saida)

    tabela_final = tabela_descricoes_unificadas.rename(columns={"descrição_normalizada": "descricao_normalizada", "NCM_padrao": "ncm_padrao", "CEST_padrao": "cest_padrao", "GTIN_padrao": "gtin_padrao"}).copy()
    tabela_final["codigo_padrao"] = tabela_final.apply(
        lambda row: replacement_map.get((str(row["codigo_padrao"]), str(row["descricao_normalizada"])), row["codigo_padrao"]),
        axis=1,
    )
    tabela_final["lista_codigos"] = tabela_final["descricao_normalizada"].map(montar_lista_codigos_desag)
    tabela_final = tabela_final.sort_values(["descricao_normalizada", "descricao"], kind="stable")
    tabela_final = alinhar_nomenclatura_documento(tabela_final)

    # -------------------------------------------------------------------------
    # Geração do mapeamento detalhado e resumido de códigos consolidados
    # -------------------------------------------------------------------------
    mapeamento_detalhado = _montar_mapeamento_detalhado(
        produtos=produtos,
        codigo_padrao=codigo_padrao,
        mapa_desag=mapa_desag,
        descricao_representativa=descricao_representativa,
    )

    mapeamento_resumido = mapeamento_detalhado[
        ["codigo_original", "codigo_final", "descricao_final", "situacao", "detalhe"]
    ].sort_values(["situacao", "codigo_original"], kind="stable")

    # -------------------------------------------------------------------------
    # Geração do índice técnico de produtos
    # -------------------------------------------------------------------------
    indice_produtos = _gerar_indice_produtos(produtos)
    indice_produtos_consolidado = _enriquecer_indice_com_codigo_consolidado(
        indice_produtos=indice_produtos,
        mapeamento_detalhado=mapeamento_detalhado,
    )

    # -------------------------------------------------------------------------
    # Aplicação do código consolidado e da chave_produto nas tabelas de origem
    # -------------------------------------------------------------------------
    nfe = load_parquet_if_exists(pasta_cnpj / f"nfe_{cnpj}.parquet")
    nfce = load_parquet_if_exists(pasta_cnpj / f"nfce_{cnpj}.parquet")
    c170 = load_parquet_if_exists(pasta_cnpj / f"c170_simplificada_{cnpj}.parquet")
    bloco_h = load_parquet_if_exists(pasta_cnpj / f"bloco_h_{cnpj}.parquet")

    nfe_integrada = None
    nfce_integrada = None
    c170_integrada = None
    bloco_h_integrada = None

    if nfe is not None:
        nfe_integrada = _aplicar_codigo_consolidado_em_tabela(
            df_original=nfe,
            df_canonico=canonicalize_nfe_like(nfe, "nfe"),
            mapeamento_detalhado=mapeamento_detalhado,
            indice_produtos=indice_produtos,
        )

    if nfce is not None:
        nfce_integrada = _aplicar_codigo_consolidado_em_tabela(
            df_original=nfce,
            df_canonico=canonicalize_nfe_like(nfce, "nfce"),
            mapeamento_detalhado=mapeamento_detalhado,
            indice_produtos=indice_produtos,
        )

    if c170 is not None:
        c170_integrada = _aplicar_codigo_consolidado_em_tabela(
            df_original=c170,
            df_canonico=canonicalize_c170(c170),
            mapeamento_detalhado=mapeamento_detalhado,
            indice_produtos=indice_produtos,
        )

    if bloco_h is not None:
        bloco_h_integrada = _aplicar_codigo_consolidado_em_tabela(
            df_original=bloco_h,
            df_canonico=canonicalize_bloco_h(bloco_h),
            mapeamento_detalhado=mapeamento_detalhado,
            indice_produtos=indice_produtos,
        )

    # -------------------------------------------------------------------------
    # Persistência
    # -------------------------------------------------------------------------
    path_unif = produtos_dir / f"tabela_descricoes_unificadas_{cnpj}.parquet"
    path_desag = produtos_dir / f"codigos_desagregados_{cnpj}.parquet"
    path_final = produtos_dir / f"tabela_produtos_{cnpj}.parquet"
    path_mapa = produtos_dir / f"mapeamento_codigos_{cnpj}.parquet"
    path_mapa_det = produtos_dir / f"mapeamento_codigos_detalhado_{cnpj}.parquet"
    path_indice = produtos_dir / f"indice_produtos_{cnpj}.parquet"
    path_indice_cons = produtos_dir / f"indice_produtos_consolidado_{cnpj}.parquet"

    tabela_descricoes_unificadas.to_parquet(path_unif, index=False)
    codigos_desagregados.to_parquet(path_desag, index=False)
    tabela_final.to_parquet(path_final, index=False)
    mapeamento_resumido.to_parquet(path_mapa, index=False)
    mapeamento_detalhado.to_parquet(path_mapa_det, index=False)
    indice_produtos.to_parquet(path_indice, index=False)
    indice_produtos_consolidado.to_parquet(path_indice_cons, index=False)

    if nfe_integrada is not None:
        nfe_integrada.to_parquet(produtos_dir / f"nfe_itens_consolidados_{cnpj}.parquet", index=False)

    if nfce_integrada is not None:
        nfce_integrada.to_parquet(produtos_dir / f"nfce_itens_consolidados_{cnpj}.parquet", index=False)

    if c170_integrada is not None:
        c170_integrada.to_parquet(produtos_dir / f"c170_itens_consolidados_{cnpj}.parquet", index=False)

    if bloco_h_integrada is not None:
        bloco_h_integrada.to_parquet(produtos_dir / f"bloco_h_itens_consolidados_{cnpj}.parquet", index=False)

    return {
        "tabela_descricoes_unificadas": path_unif,
        "codigos_desagregados": path_desag,
        "tabela_produtos": path_final,
        "mapeamento_codigos": path_mapa,
        "mapeamento_codigos_detalhado": path_mapa_det,
        "indice_produtos": path_indice,
        "indice_produtos_consolidado": path_indice_cons,
    }
