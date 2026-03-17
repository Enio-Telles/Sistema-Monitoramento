import re
import unicodedata
import math
from typing import Any, Iterable
from decimal import Decimal
from pathlib import Path
import pandas as pd
from .constants import STOPWORDS, UNIT_SYNONYMS

def _remover_acentos(texto: str | None) -> str | None:
    if texto is None:
        return None
    texto = unicodedata.normalize("NFKD", str(texto))
    return "".join(ch for ch in texto if not unicodedata.combining(ch))


def normalizar_texto(texto: str | None) -> str | None:
    if texto is None:
        return None
    texto = _remover_acentos(str(texto).upper())
    texto = re.sub(r"[^A-Z0-9\s]", " ", texto)
    tokens = [tok for tok in texto.split() if tok and tok not in STOPWORDS]
    return " ".join(tokens) if tokens else None


def normalizar_unidade(unid: str | None) -> str | None:
    if unid is None:
        return None
    u = _remover_acentos(str(unid).upper()).strip()
    u = re.sub(r"[^A-Z0-9]", "", u)
    return UNIT_SYNONYMS.get(u, u or None)


def somente_digitos(valor: str | None) -> str | None:
    if valor is None:
        return None
    digits = re.sub(r"\D", "", str(valor))
    return digits or None


def gtin_valido(gtin: str | None) -> bool:
    gtin = somente_digitos(gtin)
    if gtin is None or len(gtin) not in {8, 12, 13, 14}:
        return False
    soma = 0
    fator = 3
    for ch in reversed(gtin[:-1]):
        soma += int(ch) * fator
        fator = 1 if fator == 3 else 3
    dv = (10 - (soma % 10)) % 10
    return dv == int(gtin[-1])


def ncm_valido(ncm: str | None) -> bool:
    ncm = somente_digitos(ncm)
    return bool(ncm and len(ncm) == 8)


def cest_valido(cest: str | None) -> bool:
    cest = somente_digitos(cest)
    return bool(cest and len(cest) == 7)


def codigo_num_sort(codigo: str | None) -> float:
    if codigo is None:
        return math.inf
    digits = re.sub(r"\D", "", str(codigo))
    return float(digits) if digits else math.inf


def unique_sorted(values: Iterable[Any]) -> list[Any]:
    vistos = set()
    saida = []
    for val in values:
        if pd.isna(val) or val in (None, ""):
            continue
        chave = str(val)
        if chave not in vistos:
            vistos.add(chave)
            saida.append(chave)
    return sorted(saida)


def normalize_scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Decimal):
        if value == value.to_integral_value():
            try:
                return int(value)
            except Exception:
                return float(value)
        return float(value)
    if hasattr(value, "read"):
        try:
            return value.read()
        except Exception:
            return str(value)
    return value


def normalize_df_types(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].map(normalize_scalar)
    return df


def column_map_ci(df: pd.DataFrame) -> dict[str, str]:
    return {str(col).lower(): col for col in df.columns}


def coalesce_columns_ci(df: pd.DataFrame, candidates: Iterable[str], default: Any = None) -> pd.Series:
    cmap = column_map_ci(df)
    for col in candidates:
        real = cmap.get(col.lower())
        if real is not None:
            return df[real]
    return pd.Series([default] * len(df), index=df.index)


def load_parquet_if_exists(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    df = pd.read_parquet(path)
    if df.empty and len(df.columns) == 0:
        return None
    return df


def empty_with_schema(schema: dict[str, str]) -> pd.DataFrame:
    data: dict[str, pd.Series] = {}
    for col, dtype in schema.items():
        if dtype == "object":
            data[col] = pd.Series([], dtype="object")
        else:
            data[col] = pd.Series([], dtype=dtype)
    return pd.DataFrame(data)


class COSEFINClassifier:
    """Classificador de mercadorias para inferir o código CO_SEFIN."""

    def __init__(self, ref_dir: Path):
        self.ref_dir = ref_dir
        self.df_cest_ncm = load_parquet_if_exists(ref_dir / "sitafe_cest_ncm.parquet")
        self.df_cest = load_parquet_if_exists(ref_dir / "sitafe_cest.parquet")
        self.df_ncm = load_parquet_if_exists(ref_dir / "sitafe_ncm.parquet")

        # Normalização preventiva das bases de referência e deduplicação
        if self.df_cest_ncm is not None:
            self.df_cest_ncm["it_nu_cest"] = self.df_cest_ncm["it_nu_cest"].astype("string").str.strip()
            self.df_cest_ncm["it_nu_ncm"] = self.df_cest_ncm["it_nu_ncm"].astype("string").str.strip()
            self.df_cest_ncm["it_co_sefin"] = self.df_cest_ncm["it_co_sefin"].astype("string").str.strip()
            self.df_cest_ncm = self.df_cest_ncm.drop_duplicates(subset=["it_nu_cest", "it_nu_ncm"], keep="first")
            # F1: it_nu_cest, it_nu_ncm -> it_co_sefin

        if self.df_cest is not None:
            self.df_cest["cest"] = self.df_cest["cest"].astype("string").str.strip()
            self.df_cest["co-sefin"] = self.df_cest["co-sefin"].astype("string").str.strip()
            self.df_cest = self.df_cest.drop_duplicates(subset=["cest"], keep="first")
            # F2: cest -> co-sefin

        if self.df_ncm is not None:
            self.df_ncm["ncm"] = self.df_ncm["ncm"].astype("string").str.strip()
            self.df_ncm["co-sefin"] = self.df_ncm["co-sefin"].astype("string").str.strip()
            self.df_ncm = self.df_ncm.drop_duplicates(subset=["ncm"], keep="first")
            # F3: ncm -> co-sefin

    def classify(self, df: pd.DataFrame) -> pd.Series:
        """Inferência hierárquica do co_sefin_inferido baseada em NCM e CEST."""
        if df.empty:
            return pd.Series([None] * len(df), index=df.index, dtype="string")

        # 1. Extrair combinações únicas de (ncm_limpo, cest_limpo) para minimizar os joins
        unique_pairs = df[["ncm_limpo", "cest_limpo"]].drop_duplicates().copy()
        unique_pairs["ncm"] = unique_pairs["ncm_limpo"].astype("string").str.strip()
        unique_pairs["cest"] = unique_pairs["cest_limpo"].astype("string").str.strip()

        # Iniciar a coluna de resultado
        unique_pairs["co_sefin"] = pd.Series([None] * len(unique_pairs), index=unique_pairs.index, dtype="string")

        # Tier 1: CEST + NCM
        if self.df_cest_ncm is not None:
            m1 = unique_pairs.merge(
                self.df_cest_ncm[["it_nu_cest", "it_nu_ncm", "it_co_sefin"]],
                left_on=["cest", "ncm"],
                right_on=["it_nu_cest", "it_nu_ncm"],
                how="left"
            )
            unique_pairs["co_sefin"] = unique_pairs["co_sefin"].fillna(m1["it_co_sefin"].set_axis(unique_pairs.index))

        # Tier 2: CEST
        if self.df_cest is not None:
            m2 = unique_pairs.merge(
                self.df_cest[["cest", "co-sefin"]],
                on="cest",
                how="left"
            )
            unique_pairs["co_sefin"] = unique_pairs["co_sefin"].fillna(m2["co-sefin"].set_axis(unique_pairs.index))

        # Tier 3: NCM
        if self.df_ncm is not None:
            m3 = unique_pairs.merge(
                self.df_ncm[["ncm", "co-sefin"]],
                on="ncm",
                how="left"
            )
            unique_pairs["co_sefin"] = unique_pairs["co_sefin"].fillna(m3["co-sefin"].set_axis(unique_pairs.index))

        # 2. Fazer o map de volta para o dataframe original usando left join no index

        # Como o merge original não preserva o índice original e a ordem pode mudar
        # usamos um truque com o index original

        # Recriamos com o index para garantir alinhamento exato
        df_idx = df[["ncm_limpo", "cest_limpo"]].copy()
        df_idx["_orig_index"] = df_idx.index

        # Merge mantendo índice original para reordenação
        mapped = df_idx.merge(
            unique_pairs[["ncm_limpo", "cest_limpo", "co_sefin"]],
            on=["ncm_limpo", "cest_limpo"],
            how="left"
        )

        mapped = mapped.set_index("_orig_index")
        mapped.index.name = df.index.name

        return mapped["co_sefin"].astype("string")