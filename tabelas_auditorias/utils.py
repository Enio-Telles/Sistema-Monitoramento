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


def coalesce_columns_ci(
    df: pd.DataFrame, candidates: Iterable[str], default: Any = None
) -> pd.Series:
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

        # Normalização preventiva das bases de referência
        if self.df_cest_ncm is not None:
            self.df_cest_ncm["it_nu_cest"] = (
                self.df_cest_ncm["it_nu_cest"].astype("string").str.strip()
            )
            self.df_cest_ncm["it_nu_ncm"] = (
                self.df_cest_ncm["it_nu_ncm"].astype("string").str.strip()
            )
            self.df_cest_ncm["it_co_sefin"] = (
                self.df_cest_ncm["it_co_sefin"].astype("string").str.strip()
            )
            # F1: it_nu_cest, it_nu_ncm -> it_co_sefin

        if self.df_cest is not None:
            self.df_cest["cest"] = self.df_cest["cest"].astype("string").str.strip()
            self.df_cest["co-sefin"] = (
                self.df_cest["co-sefin"].astype("string").str.strip()
            )
            # F2: cest -> co-sefin

        if self.df_ncm is not None:
            self.df_ncm["ncm"] = self.df_ncm["ncm"].astype("string").str.strip()
            self.df_ncm["co-sefin"] = (
                self.df_ncm["co-sefin"].astype("string").str.strip()
            )
            # F3: ncm -> co-sefin

    def classify(self, df: pd.DataFrame) -> pd.Series:
        """Inferência hierárquica do co_sefin_inferido baseada em NCM e CEST."""
        res = pd.Series([None] * len(df), index=df.index, dtype="string")

        # Normalização das colunas de entrada para o merge
        work = pd.DataFrame(
            {
                "ncm": df["ncm_limpo"].astype("string").str.strip(),
                "cest": df["cest_limpo"].astype("string").str.strip(),
            },
            index=df.index,
        )

        # Tier 1: CEST + NCM
        if self.df_cest_ncm is not None:
            m1 = work.merge(
                self.df_cest_ncm[["it_nu_cest", "it_nu_ncm", "it_co_sefin"]],
                left_on=["cest", "ncm"],
                right_on=["it_nu_cest", "it_nu_ncm"],
                how="left",
            )
            res = res.fillna(m1["it_co_sefin"].set_axis(df.index))

        # Tier 2: CEST
        if self.df_cest is not None:
            m2 = work.merge(self.df_cest[["cest", "co-sefin"]], on="cest", how="left")
            res = res.fillna(m2["co-sefin"].set_axis(df.index))

        # Tier 3: NCM
        if self.df_ncm is not None:
            m3 = work.merge(self.df_ncm[["ncm", "co-sefin"]], on="ncm", how="left")
            res = res.fillna(m3["co-sefin"].set_axis(df.index))

        return res
