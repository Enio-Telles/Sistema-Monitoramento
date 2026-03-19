from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from fiscal_app.config import AGGREGATION_LOG_FILE
from fiscal_app.utils.text import natural_sort_key, normalize_text

CODE_ENTRY_RE = re.compile(r"\[(.*?);\s*(\d+)\]")


@dataclass
class AggregationResult:
    target_path: Path
    aggregated_row: dict[str, Any]
    removed_keys: list[tuple[str, str]]


class AggregationService:
    def __init__(self, log_file: Path = AGGREGATION_LOG_FILE) -> None:
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def target_table_path(cnpj_dir: Path, cnpj: str) -> Path:
        return cnpj_dir / "produtos" / f"tabela_produtos_editavel_{cnpj}.parquet"

    @staticmethod
    def source_table_path(cnpj_dir: Path, cnpj: str) -> Path:
        return cnpj_dir / "produtos" / f"tabela_produtos_{cnpj}.parquet"

    def load_editable_table(self, cnpj_dir: Path, cnpj: str) -> Path:
        source = self.source_table_path(cnpj_dir, cnpj)
        target = self.target_table_path(cnpj_dir, cnpj)
        if target.exists():
            return target
        if not source.exists():
            raise FileNotFoundError(
                "A tabela de origem para agregação não foi encontrada."
            )
        df = pl.read_parquet(source)
        df.write_parquet(target, compression="snappy")
        return target

    @staticmethod
    def _ensure_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [str(item) for item in value if item not in (None, "")]
        text = str(value).strip()
        if not text:
            return []
        return [text]

    @staticmethod
    def _parse_codigo_entries(raw_values: list[Any]) -> list[tuple[str, int, str]]:
        parsed: list[tuple[str, int, str]] = []
        for raw in raw_values:
            for entry in AggregationService._ensure_list(raw):
                if entry and entry[0] == "[":
                    match = CODE_ENTRY_RE.match(entry)
                    if match:
                        codigo = match.group(1).strip()
                        freq = int(match.group(2))
                        parsed.append((codigo, freq, entry))
                        continue
                parsed.append((entry, 1, entry))
        return parsed

    @staticmethod
    def _pick_mode(values: list[Any]) -> str | None:
        cleaned = [
            str(v).strip() for v in values if v not in (None, "", []) and str(v).strip()
        ]
        if not cleaned:
            return None
        counts = Counter(cleaned)
        max_count = max(counts.values())
        candidates = [value for value, count in counts.items() if count == max_count]
        return sorted(candidates, key=natural_sort_key)[0]

    @staticmethod
    def _merge_list_columns(rows: list[dict[str, Any]], column: str) -> list[str]:
        values = []
        for row in rows:
            values.extend(AggregationService._ensure_list(row.get(column)))
        return sorted(set(values), key=natural_sort_key)

    def build_aggregated_row(
        self,
        rows: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> dict[str, Any]:
        if len(rows) < 2:
            raise ValueError("Selecione pelo menos duas linhas para agregar.")

        parsed_codes = self._parse_codigo_entries(
            [row.get("lista_codigos") for row in rows]
        )
        if not parsed_codes:
            raise ValueError(
                "Não foi possível identificar códigos nas linhas selecionadas."
            )

        freq_counter: Counter[str] = Counter()
        original_entries: dict[str, int] = {}
        for codigo, freq, _raw in parsed_codes:
            freq_counter[codigo] += freq
            original_entries[codigo] = max(original_entries.get(codigo, 0), freq)

        max_freq = max(freq_counter.values())
        top_codes = [
            codigo for codigo, freq in freq_counter.items() if freq == max_freq
        ]
        codigo_padrao = sorted(top_codes, key=natural_sort_key)[0]

        descricao = (
            descricao_resultante
            or self._pick_mode([row.get("descricao") for row in rows])
            or rows[0].get("descricao")
            or ""
        ).strip()
        descricao_norm = (
            descricao_normalizada_resultante or normalize_text(descricao)
        ).strip()
        if not descricao_norm:
            descricao_norm = normalize_text(descricao)

        # Pick descricao_padrao: the one with more words in descrição_normalizada
        descricao_padrao = (
            rows[0].get("descricao_padrao")
            or rows[0].get("descrição_normalizada")
            or ""
        )
        max_words = 0
        for row in rows:
            dn = str(row.get("descrição_normalizada") or "")
            word_count = len(dn.split())
            if word_count > max_words:
                max_words = word_count
                descricao_padrao = row.get("descrição_normalizada") or dn

        # Merge lista_descricoes_normalizadas
        all_norm = []
        for row in rows:
            norms = self._ensure_list(row.get("lista_descricoes_normalizadas"))
            if not norms:
                norm = row.get("descrição_normalizada") or row.get(
                    "descricao_normalizada"
                )
                if norm:
                    all_norm.append(str(norm))
            else:
                all_norm.extend(norms)
        lista_descricoes_normalizadas = sorted(set(all_norm), key=natural_sort_key)

        merged_codigo_entries = [
            f"[{codigo}; {original_entries[codigo]}]"
            for codigo in sorted(original_entries, key=natural_sort_key)
        ]
        aggregated = {
            "descrição_normalizada": descricao_norm,
            "descricao": descricao,
            "descricao_padrao": descricao_padrao,
            "lista_codigos": merged_codigo_entries,
            "lista_tipo_item": self._merge_list_columns(rows, "lista_tipo_item"),
            "lista_ncm": self._merge_list_columns(rows, "lista_ncm"),
            "lista_cest": self._merge_list_columns(rows, "lista_cest"),
            "lista_gtin": self._merge_list_columns(rows, "lista_gtin"),
            "lista_unid": self._merge_list_columns(rows, "lista_unid"),
            "lista_descricoes": self._merge_list_columns(rows, "lista_descricoes"),
            "lista_descricoes_normalizadas": lista_descricoes_normalizadas,
            "codigo_padrao": codigo_padrao,
            "qtd_codigos": len(original_entries),
            "tipo_item_padrao": self._pick_mode(
                [row.get("tipo_item_padrao") for row in rows]
            ),
            "NCM_padrao": self._pick_mode([row.get("NCM_padrao") for row in rows]),
            "CEST_padrao": self._pick_mode([row.get("CEST_padrao") for row in rows]),
            "GTIN_padrao": self._pick_mode([row.get("GTIN_padrao") for row in rows]),
            "verificado": False,
        }
        return aggregated

    def aggregate_rows(
        self,
        cnpj_dir: Path,
        cnpj: str,
        rows: list[dict[str, Any]],
        descricao_resultante: str | None = None,
        descricao_normalizada_resultante: str | None = None,
    ) -> AggregationResult:
        target = self.load_editable_table(cnpj_dir, cnpj)
        current = pl.read_parquet(target)

        # Ensure descricao_padrao exists for compatibility with older files
        if "descricao_padrao" not in current.columns:
            # Use 'descrição_normalizada' or 'descricao_normalizada' depending on what's available
            norm_col = (
                "descrição_normalizada"
                if "descrição_normalizada" in current.columns
                else "descricao_normalizada"
            )
            current = current.with_columns(pl.col(norm_col).alias("descricao_padrao"))
        removed_keys = [
            (
                str(row.get("descrição_normalizada") or ""),
                str(row.get("descricao") or ""),
            )
            for row in rows
        ]
        aggregated_row = self.build_aggregated_row(
            rows, descricao_resultante, descricao_normalizada_resultante
        )

        key_set = set(removed_keys)
        kept_rows = []
        for row in current.iter_rows(named=True):
            key = (
                str(row.get("descrição_normalizada") or ""),
                str(row.get("descricao") or ""),
            )
            if key not in key_set:
                kept_rows.append(row)

        updated_rows = kept_rows + [aggregated_row]
        updated = pl.DataFrame(updated_rows, schema=current.schema)
        updated.write_parquet(target, compression="snappy")

        self._append_log(
            cnpj=cnpj, target=target, rows=rows, aggregated_row=aggregated_row
        )
        return AggregationResult(
            target_path=target, aggregated_row=aggregated_row, removed_keys=removed_keys
        )

    def _append_log(
        self,
        cnpj: str,
        target: Path,
        rows: list[dict[str, Any]],
        aggregated_row: dict[str, Any],
    ) -> None:
        payload = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "cnpj": cnpj,
            "arquivo_destino": str(target),
            "linhas_origem": [
                {
                    "descrição_normalizada": row.get("descrição_normalizada"),
                    "descricao": row.get("descricao"),
                    "codigo_padrao": row.get("codigo_padrao"),
                }
                for row in rows
            ],
            "resultado": aggregated_row,
            "regra_codigo_padrao": "Maior frequência em lista_codigos; em empate, menor código em ordem alfanumérica.",
        }
        with self.log_file.open("a", encoding="utf-8") as fp:
            fp.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def load_log_lines(self, limit: int = 200) -> list[str]:
        if not self.log_file.exists():
            return []
        lines = self.log_file.read_text(encoding="utf-8").splitlines()
        return lines[-limit:]
