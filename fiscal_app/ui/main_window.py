from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import polars as pl
from PySide6.QtCore import QDate, QThread, Qt, Signal, QUrl
from PySide6.QtGui import QDesktopServices
from PySide6.QtWidgets import (
    QAbstractItemView,
    QDateEdit,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QComboBox,
    QSplitter,
    QStatusBar,
    QTabWidget,
    QTableView,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from fiscal_app.config import APP_NAME, CONSULTAS_ROOT, DEFAULT_PAGE_SIZE
from fiscal_app.models.table_model import PolarsTableModel
from fiscal_app.services.aggregation_service import AggregationService
from fiscal_app.services.export_service import ExportService
from fiscal_app.services.parquet_service import FilterCondition, ParquetService
from fiscal_app.services.pipeline_service import PipelineResult, PipelineService
from fiscal_app.services.registry_service import RegistryService
from fiscal_app.ui.dialogs import ColumnSelectorDialog
from fiscal_app.utils.text import display_cell, normalize_text, remove_accents


class PipelineWorker(QThread):
    finished_ok = Signal(object)
    failed = Signal(str)

    def __init__(self, service: PipelineService, cnpj: str, data_limite: str | None = None) -> None:
        super().__init__()
        self.service = service
        self.cnpj = cnpj
        self.data_limite = data_limite

    def run(self) -> None:
        try:
            result = self.service.run_for_cnpj(self.cnpj, self.data_limite)
        except Exception as exc:  # pragma: no cover - UI
            self.failed.emit(str(exc))
            return
        if result.ok:
            self.finished_ok.emit(result)
        else:
            message = (result.stderr or result.stdout or "Falha sem detalhes.").strip()
            self.failed.emit(message)


@dataclass
class ViewState:
    current_cnpj: str | None = None
    current_file: Path | None = None
    current_page: int = 1
    page_size: int = DEFAULT_PAGE_SIZE
    all_columns: list[str] | None = None
    visible_columns: list[str] | None = None
    filters: list[FilterCondition] | None = None
    total_rows: int = 0


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.resize(1560, 920)

        self.registry_service = RegistryService()
        self.parquet_service = ParquetService()
        self.pipeline_service = PipelineService(output_root=CONSULTAS_ROOT)
        self.export_service = ExportService()
        self.aggregation_service = AggregationService()

        self.state = ViewState(filters=[])
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.table_model = PolarsTableModel()
        self.aggregation_table_model = PolarsTableModel(checkable=True)
        self.results_table_model = PolarsTableModel(checkable=True)
        self.aggregation_basket: list[dict] = []
        self.aggregation_results: list[dict] = []
        self.pipeline_worker: PipelineWorker | None = None

        self._build_ui()
        self._connect_signals()
        self.refresh_cnpjs()
        self.refresh_logs()

    def _build_ui(self) -> None:
        central = QWidget()
        self.setCentralWidget(central)
        root_layout = QVBoxLayout(central)

        splitter = QSplitter(Qt.Horizontal)
        root_layout.addWidget(splitter)

        splitter.addWidget(self._build_left_panel())
        splitter.addWidget(self._build_right_panel())
        splitter.setSizes([310, 1200])

        self.status = QStatusBar()
        self.setStatusBar(self.status)
        self.status.showMessage("Pronto.")

    def _build_left_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        cnpj_box = QGroupBox("CNPJs")
        cnpj_layout = QVBoxLayout(cnpj_box)
        input_line = QHBoxLayout()
        self.cnpj_input = QLineEdit()
        self.cnpj_input.setPlaceholderText("Digite o CNPJ com ou sem máscara")
        self.btn_run_pipeline = QPushButton("Analisar CNPJ")
        input_line.addWidget(self.cnpj_input)
        input_line.addWidget(self.btn_run_pipeline)
        cnpj_layout.addLayout(input_line)

        date_line = QHBoxLayout()
        date_line.addWidget(QLabel("Data limite EFD:"))
        self.date_input = QDateEdit()
        self.date_input.setCalendarPopup(True)
        self.date_input.setDate(QDate.currentDate())
        self.date_input.setDisplayFormat("dd/MM/yyyy")
        date_line.addWidget(self.date_input)
        cnpj_layout.addLayout(date_line)

        actions = QHBoxLayout()
        self.btn_refresh_cnpjs = QPushButton("Atualizar lista")
        self.btn_open_cnpj_folder = QPushButton("Abrir pasta")
        actions.addWidget(self.btn_refresh_cnpjs)
        actions.addWidget(self.btn_open_cnpj_folder)
        cnpj_layout.addLayout(actions)

        self.cnpj_list = QListWidget()
        cnpj_layout.addWidget(self.cnpj_list)
        layout.addWidget(cnpj_box)

        files_box = QGroupBox("Arquivos Parquet do CNPJ")
        files_layout = QVBoxLayout(files_box)
        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabels(["Arquivo", "Local"])
        files_layout.addWidget(self.file_tree)
        layout.addWidget(files_box)

        notes = QLabel(
            "Fluxo recomendado: analise um CNPJ, abra a tabela desejada, filtre, selecione colunas e exporte. "
            "Para agregação, trabalhe sobre a tabela desagregada e monte o lote na aba Agregação."
        )
        notes.setWordWrap(True)
        layout.addWidget(notes)
        return panel

    def _build_right_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        header = QHBoxLayout()
        self.lbl_context = QLabel("Nenhum arquivo selecionado")
        self.lbl_context.setWordWrap(True)
        header.addWidget(self.lbl_context)
        header.addStretch()
        layout.addLayout(header)

        self.tabs = QTabWidget()
        self.tabs.addTab(self._build_tab_consulta(), "Consulta")
        self.tabs.addTab(self._build_tab_agregacao(), "Agregação")
        self.tabs.addTab(self._build_tab_logs(), "Logs")
        layout.addWidget(self.tabs)
        return panel

    def _build_tab_consulta(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        filter_box = QGroupBox("Filtros")
        filter_layout = QVBoxLayout(filter_box)
        form = QHBoxLayout()
        self.filter_column = QComboBox()
        self.filter_operator = QComboBox()
        self.filter_operator.addItems(["contém", "igual", "começa com", "termina com", ">", ">=", "<", "<=", "é nulo", "não é nulo"])
        self.filter_value = QLineEdit()
        self.filter_value.setPlaceholderText("Valor do filtro")
        self.btn_add_filter = QPushButton("Adicionar filtro")
        self.btn_clear_filters = QPushButton("Limpar filtros")
        form.addWidget(QLabel("Coluna"))
        form.addWidget(self.filter_column)
        form.addWidget(QLabel("Operador"))
        form.addWidget(self.filter_operator)
        form.addWidget(QLabel("Valor"))
        form.addWidget(self.filter_value)
        form.addWidget(self.btn_add_filter)
        form.addWidget(self.btn_clear_filters)
        filter_layout.addLayout(form)

        self.filter_list = QListWidget()
        self.filter_list.setMaximumHeight(90)
        filter_layout.addWidget(self.filter_list)

        filter_actions = QHBoxLayout()
        self.btn_remove_filter = QPushButton("Remover filtro selecionado")
        self.btn_choose_columns = QPushButton("Selecionar colunas")
        self.btn_prev_page = QPushButton("Página anterior")
        self.btn_next_page = QPushButton("Próxima página")
        self.lbl_page = QLabel("Página 0/0")
        filter_actions.addWidget(self.btn_remove_filter)
        filter_actions.addWidget(self.btn_choose_columns)
        filter_actions.addStretch()
        filter_actions.addWidget(self.btn_prev_page)
        filter_actions.addWidget(self.lbl_page)
        filter_actions.addWidget(self.btn_next_page)
        filter_layout.addLayout(filter_actions)
        layout.addWidget(filter_box)

        export_box = QGroupBox("Exportação")
        export_layout = QHBoxLayout(export_box)
        self.btn_export_excel_full = QPushButton("Excel - tabela completa")
        self.btn_export_excel_filtered = QPushButton("Excel - tabela filtrada")
        self.btn_export_excel_visible = QPushButton("Excel - colunas visíveis")
        self.btn_export_docx = QPushButton("Relatório Word")
        self.btn_export_html_txt = QPushButton("TXT com HTML")
        for btn in [
            self.btn_export_excel_full,
            self.btn_export_excel_filtered,
            self.btn_export_excel_visible,
            self.btn_export_docx,
            self.btn_export_html_txt,
        ]:
            export_layout.addWidget(btn)
        layout.addWidget(export_box)

        quick_filter_layout = QHBoxLayout()
        self.qf_norm = QLineEdit()
        self.qf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.qf_desc = QLineEdit()
        self.qf_desc.setPlaceholderText("Filtrar Descrição")
        self.qf_ncm = QLineEdit()
        self.qf_ncm.setPlaceholderText("Filtrar NCM")
        self.qf_cest = QLineEdit()
        self.qf_cest.setPlaceholderText("Filtrar CEST")
        
        for w in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest]:
            w.setMaximumWidth(200)
            quick_filter_layout.addWidget(w)
        quick_filter_layout.addStretch()
        layout.addLayout(quick_filter_layout)

        self.table_view = QTableView()
        self.table_view.setModel(self.table_model)
        self.table_view.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setSortingEnabled(False)
        self.table_view.setWordWrap(True)
        self.table_view.verticalHeader().setDefaultSectionSize(60)
        self.table_view.horizontalHeader().setMinimumSectionSize(40)
        self.table_view.horizontalHeader().setDefaultSectionSize(200)
        self.table_view.horizontalHeader().setMaximumSectionSize(300)
        self.table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        self.table_view.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.table_view, 1)
        return tab

    def _build_tab_agregacao(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)

        top_box = QGroupBox("Tabela Editável (Selecione linhas para agregar)")
        top_layout = QVBoxLayout(top_box)
        
        toolbar = QHBoxLayout()
        self.btn_open_editable_table = QPushButton("Abrir tabela editável _2")
        self.btn_execute_aggregation = QPushButton("Agregar Descrições (da seleção)")
        toolbar.addWidget(self.btn_open_editable_table)
        toolbar.addWidget(self.btn_execute_aggregation)
        toolbar.addStretch()
        top_layout.addLayout(toolbar)

        agg_qf_layout = QHBoxLayout()
        self.aqf_norm = QLineEdit()
        self.aqf_norm.setPlaceholderText("Filtrar Desc. Norm")
        self.aqf_desc = QLineEdit()
        self.aqf_desc.setPlaceholderText("Filtrar Descrição")
        self.aqf_ncm = QLineEdit()
        self.aqf_ncm.setPlaceholderText("Filtrar NCM")
        self.aqf_cest = QLineEdit()
        self.aqf_cest.setPlaceholderText("Filtrar CEST")

        for w in [self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            w.setMaximumWidth(200)
            agg_qf_layout.addWidget(w)
        agg_qf_layout.addStretch()
        top_layout.addLayout(agg_qf_layout)

        self.aggregation_table_view = QTableView()
        self.aggregation_table_view.setModel(self.aggregation_table_model)
        self.aggregation_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.aggregation_table_view.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.aggregation_table_view.setAlternatingRowColors(True)
        self.aggregation_table_view.setWordWrap(True)
        self.aggregation_table_view.verticalHeader().setDefaultSectionSize(60)
        self.aggregation_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.aggregation_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.aggregation_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.aggregation_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        top_layout.addWidget(self.aggregation_table_view, 1)
        layout.addWidget(top_box, 3)

        bottom_box = QGroupBox("Resultados da Sessão (Historico)")
        bottom_layout = QVBoxLayout(bottom_box)
        self.results_table_view = QTableView()
        self.results_table_view.setModel(self.results_table_model)
        self.results_table_view.setSelectionBehavior(QAbstractItemView.SelectItems)
        self.results_table_view.setAlternatingRowColors(True)
        self.results_table_view.setWordWrap(True)
        self.results_table_view.verticalHeader().setDefaultSectionSize(60)
        self.results_table_view.horizontalHeader().setMinimumSectionSize(40)
        self.results_table_view.horizontalHeader().setDefaultSectionSize(200)
        self.results_table_view.horizontalHeader().setMaximumSectionSize(300)
        self.results_table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
        bottom_layout.addWidget(self.results_table_view, 1)
        layout.addWidget(bottom_box, 1)

        return tab

    def _build_tab_logs(self) -> QWidget:
        tab = QWidget()
        layout = QVBoxLayout(tab)
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        layout.addWidget(self.log_view)
        return tab

    def _connect_signals(self) -> None:
        self.btn_refresh_cnpjs.clicked.connect(self.refresh_cnpjs)
        self.btn_run_pipeline.clicked.connect(self.run_pipeline_for_input)
        self.cnpj_list.itemSelectionChanged.connect(self.on_cnpj_selected)
        self.file_tree.itemClicked.connect(self.on_file_activated)
        self.file_tree.itemDoubleClicked.connect(self.on_file_activated)
        self.btn_open_cnpj_folder.clicked.connect(self.open_cnpj_folder)

        self.btn_add_filter.clicked.connect(self.add_filter_from_form)
        self.btn_clear_filters.clicked.connect(self.clear_filters)
        self.btn_remove_filter.clicked.connect(self.remove_selected_filter)
        self.btn_choose_columns.clicked.connect(self.choose_columns)
        self.btn_prev_page.clicked.connect(self.prev_page)
        self.btn_next_page.clicked.connect(self.next_page)

        self.btn_export_excel_full.clicked.connect(lambda: self.export_excel("full"))
        self.btn_export_excel_filtered.clicked.connect(lambda: self.export_excel("filtered"))
        self.btn_export_excel_visible.clicked.connect(lambda: self.export_excel("visible"))
        self.btn_export_docx.clicked.connect(self.export_docx)
        self.btn_export_html_txt.clicked.connect(self.export_txt_html)

        self.btn_open_editable_table.clicked.connect(self.open_editable_aggregation_table)
        self.btn_execute_aggregation.clicked.connect(self.execute_aggregation)

        for qf in [self.qf_norm, self.qf_desc, self.qf_ncm, self.qf_cest,
                   self.aqf_norm, self.aqf_desc, self.aqf_ncm, self.aqf_cest]:
            qf.returnPressed.connect(self.apply_quick_filters)

    def show_error(self, title: str, message: str) -> None:
        QMessageBox.critical(self, title, message)

    def show_info(self, title: str, message: str) -> None:
        QMessageBox.information(self, title, message)

    def refresh_cnpjs(self) -> None:
        known = {record.cnpj for record in self.registry_service.list_records()}
        known.update(self.parquet_service.list_cnpjs())
        current = self.state.current_cnpj
        self.cnpj_list.clear()
        for cnpj in sorted(known):
            self.cnpj_list.addItem(cnpj)
        if current:
            matches = self.cnpj_list.findItems(current, Qt.MatchExactly)
            if matches:
                self.cnpj_list.setCurrentItem(matches[0])

    def run_pipeline_for_input(self) -> None:
        try:
            cnpj = self.pipeline_service.sanitize_cnpj(self.cnpj_input.text())
        except Exception as exc:
            self.show_error("CNPJ inválido", str(exc))
            return

        self.btn_run_pipeline.setEnabled(False)
        self.status.showMessage(f"Executando pipeline para {cnpj}...")
        
        data_limite = self.date_input.date().toString("dd/MM/yyyy")
        self.pipeline_worker = PipelineWorker(self.pipeline_service, cnpj, data_limite)
        self.pipeline_worker.finished_ok.connect(self.on_pipeline_finished)
        self.pipeline_worker.failed.connect(self.on_pipeline_failed)
        self.pipeline_worker.start()

    def on_pipeline_finished(self, result: PipelineResult) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.registry_service.upsert(result.cnpj, ran_now=True)
        self.status.showMessage(f"Pipeline concluído para {result.cnpj}.")
        self.refresh_cnpjs()
        matches = self.cnpj_list.findItems(result.cnpj, Qt.MatchExactly)
        if matches:
            self.cnpj_list.setCurrentItem(matches[0])
        self.show_info("Consulta concluída", result.stdout or f"CNPJ {result.cnpj} processado com sucesso.")

    def on_pipeline_failed(self, message: str) -> None:
        self.btn_run_pipeline.setEnabled(True)
        self.status.showMessage("Falha na execução do pipeline.")
        self.show_error("Falha ao consultar o banco", message)

    def on_cnpj_selected(self) -> None:
        item = self.cnpj_list.currentItem()
        if item is None:
            return
        cnpj = item.text()
        self.state.current_cnpj = cnpj
        self.registry_service.upsert(cnpj, ran_now=False)
        self.refresh_file_tree(cnpj)

    def refresh_file_tree(self, cnpj: str) -> None:
        self.file_tree.clear()
        base_dir = self.parquet_service.cnpj_dir(cnpj)
        raw_root = QTreeWidgetItem(["Tabelas brutas", str(base_dir)])
        prod_root = QTreeWidgetItem(["Produtos", str(base_dir / "produtos")])
        self.file_tree.addTopLevelItem(raw_root)
        self.file_tree.addTopLevelItem(prod_root)

        first_leaf: QTreeWidgetItem | None = None
        for path in self.parquet_service.list_parquet_files(cnpj):
            parent = prod_root if path.parent.name == "produtos" else raw_root
            item = QTreeWidgetItem([path.name, str(path.parent)])
            item.setData(0, Qt.UserRole, str(path))
            parent.addChild(item)
            if first_leaf is None:
                first_leaf = item
        raw_root.setExpanded(True)
        prod_root.setExpanded(True)
        if first_leaf is not None:
            self.file_tree.setCurrentItem(first_leaf)
            self.on_file_activated(first_leaf, 0)

    def on_file_activated(self, item: QTreeWidgetItem, _column: int) -> None:
        raw_path = item.data(0, Qt.UserRole)
        if not raw_path:
            return
        self.state.current_file = Path(raw_path)
        self.state.current_page = 1
        self.state.filters = []
        self.current_page_df_all = pl.DataFrame()
        self.current_page_df_visible = pl.DataFrame()
        self.load_current_file(reset_columns=True)
        self.tabs.setCurrentIndex(0)

    def load_current_file(self, reset_columns: bool = False) -> None:
        if self.state.current_file is None:
            return
        try:
            all_columns = self.parquet_service.get_schema(self.state.current_file)
        except Exception as exc:
            self.show_error("Erro ao abrir Parquet", str(exc))
            return
        self.state.all_columns = all_columns
        if reset_columns or not self.state.visible_columns:
            self.state.visible_columns = all_columns[:]
        self.filter_column.clear()
        self.filter_column.addItems(all_columns)
        self.reload_table()

    def reload_table(self, update_main_view: bool = True) -> None:
        if self.state.current_file is None:
            return
        try:
            page_result = self.parquet_service.get_page(
                parquet_path=self.state.current_file,
                conditions=self.state.filters or [],
                visible_columns=self.state.visible_columns or [],
                page=self.state.current_page,
                page_size=self.state.page_size,
            )
            self.state.total_rows = page_result.total_rows
            self.current_page_df_all = page_result.df_all_columns
            self.current_page_df_visible = page_result.df_visible

            if update_main_view:
                self.table_model.set_dataframe(self.current_page_df_visible)
                self._update_page_label()
                self._update_context_label()
                self._refresh_filter_list_widget()
                self.table_view.resizeColumnsToContents()
        except Exception as exc:
            self.show_error("Erro ao carregar dados", str(exc))

    def _update_page_label(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page > total_pages:
            self.state.current_page = total_pages
        self.lbl_page.setText(f"Página {self.state.current_page}/{total_pages} | Linhas filtradas: {self.state.total_rows}")

    def _update_context_label(self) -> None:
        if self.state.current_file is None:
            self.lbl_context.setText("Nenhum arquivo selecionado")
            return
        self.lbl_context.setText(
            f"CNPJ: {self.state.current_cnpj or '-'} | Arquivo: {self.state.current_file.name} | "
            f"Colunas visíveis: {len(self.state.visible_columns or [])}/{len(self.state.all_columns or [])}"
        )

    def add_filter_from_form(self) -> None:
        column = self.filter_column.currentText().strip()
        operator = self.filter_operator.currentText().strip()
        value = self.filter_value.text().strip()
        if not column:
            self.show_error("Filtro inválido", "Selecione uma coluna para filtrar.")
            return
        if operator not in {"é nulo", "não é nulo"} and value == "":
            self.show_error("Filtro inválido", "Informe um valor para o filtro escolhido.")
            return
        self.state.filters = self.state.filters or []
        self.state.filters.append(FilterCondition(column=column, operator=operator, value=value))
        self.state.current_page = 1
        self.filter_value.clear()
        self.reload_table()

    def clear_filters(self) -> None:
        self.state.filters = []
        self.state.current_page = 1
        self.reload_table()

    def remove_selected_filter(self) -> None:
        row = self.filter_list.currentRow()
        if row < 0 or not self.state.filters:
            return
        self.state.filters.pop(row)
        self.state.current_page = 1
        self.reload_table()

    def _refresh_filter_list_widget(self) -> None:
        self.filter_list.clear()
        for cond in self.state.filters or []:
            text = f"{cond.column} {cond.operator} {cond.value}".strip()
            self.filter_list.addItem(text)

    def choose_columns(self) -> None:
        if not self.state.all_columns:
            return
        dialog = ColumnSelectorDialog(self.state.all_columns, self.state.visible_columns or self.state.all_columns, self)
        if dialog.exec():
            selected = dialog.selected_columns()
            if not selected:
                self.show_error("Seleção inválida", "Pelo menos uma coluna deve permanecer visível.")
                return
            self.state.visible_columns = selected
            self.state.current_page = 1
            self.reload_table()

    def prev_page(self) -> None:
        if self.state.current_page > 1:
            self.state.current_page -= 1
            self.reload_table()

    def next_page(self) -> None:
        total_pages = max(1, ((self.state.total_rows - 1) // self.state.page_size) + 1 if self.state.total_rows else 1)
        if self.state.current_page < total_pages:
            self.state.current_page += 1
            self.reload_table()

    def _save_dialog(self, title: str, pattern: str) -> Path | None:
        filename, _ = QFileDialog.getSaveFileName(self, title, str(CONSULTAS_ROOT), pattern)
        return Path(filename) if filename else None

    def _filters_text(self) -> str:
        return " | ".join(f"{f.column} {f.operator} {f.value}".strip() for f in self.state.filters or [])

    def _dataset_for_export(self, mode: str) -> pl.DataFrame:
        if self.state.current_file is None:
            raise ValueError("Nenhum arquivo selecionado.")
        if mode == "full":
            return self.parquet_service.load_dataset(self.state.current_file)
        if mode == "filtered":
            return self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [])
        if mode == "visible":
            return self.parquet_service.load_dataset(
                self.state.current_file,
                self.state.filters or [],
                self.state.visible_columns or [],
            )
        raise ValueError(f"Modo de exportação não suportado: {mode}")

    def export_excel(self, mode: str) -> None:
        try:
            df = self._dataset_for_export(mode)
            target = self._save_dialog("Salvar Excel", "Excel (*.xlsx)")
            if not target:
                return
            self.export_service.export_excel(target, df, sheet_name=self.state.current_file.stem if self.state.current_file else "Dados")
            self.show_info("Exportação concluída", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação para Excel", str(exc))

    def export_docx(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            target = self._save_dialog("Salvar relatório Word", "Word (*.docx)")
            if not target:
                return
            self.export_service.export_docx(
                target,
                title="Relatório Padronizado de Análise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            self.show_info("Relatório gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação para Word", str(exc))

    def export_txt_html(self) -> None:
        try:
            if self.state.current_file is None:
                raise ValueError("Nenhum arquivo selecionado.")
            df = self.parquet_service.load_dataset(self.state.current_file, self.state.filters or [], self.state.visible_columns or [])
            html_report = self.export_service.build_html_report(
                title="Relatório Padronizado de Análise Fiscal",
                cnpj=self.state.current_cnpj or "",
                table_name=self.state.current_file.name,
                df=df,
                filters_text=self._filters_text(),
                visible_columns=self.state.visible_columns or [],
            )
            target = self._save_dialog("Salvar TXT com HTML", "TXT (*.txt)")
            if not target:
                return
            self.export_service.export_txt_with_html(target, html_report)
            self.show_info("Relatório HTML/TXT gerado", f"Arquivo gerado em:\n{target}")
        except Exception as exc:
            self.show_error("Falha na exportação TXT/HTML", str(exc))

    def open_editable_aggregation_table(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ na lista.")
            return
        try:
            cnpj_dir = self.parquet_service.cnpj_dir(self.state.current_cnpj)
            target = self.aggregation_service.load_editable_table(cnpj_dir, self.state.current_cnpj)
            df = pl.read_parquet(target)
            self.state.all_columns = df.columns
            self.aggregation_table_model.set_dataframe(df)
            self.aggregation_table_view.resizeColumnsToContents()
        except Exception as exc:
            self.show_error("Falha ao abrir tabela editável", str(exc))
            return

        self.state.current_file = target
        self.state.current_page = 1
        self.state.filters = []
        # We don't necessarily want to load it in the Consulta tab here if the user is in Agregação
        # self.load_current_file(reset_columns=True)
        self.tabs.setCurrentIndex(1)

    def execute_aggregation(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ antes de agregar.")
            return

        rows_top = self.aggregation_table_model.get_checked_rows()
        rows_bottom = self.results_table_model.get_checked_rows()
        
        # Merge and de-duplicate
        combined = []
        seen = set()
        for r in (rows_top + rows_bottom):
            key = (str(r.get("descrição_normalizada") or ""), str(r.get("descricao") or ""))
            if key not in seen:
                seen.add(key)
                combined.append(r)

        if len(combined) < 2:
            self.show_error("Seleção insuficiente", "Marque pelo menos duas linhas com 'Visto' (pode ser em ambas as tabelas) para agregar.")
            return

        try:
            result = self.aggregation_service.aggregate_rows(
                cnpj_dir=self.parquet_service.cnpj_dir(self.state.current_cnpj),
                cnpj=self.state.current_cnpj,
                rows=combined,
            )
            
            # Update history: remove any rows from combined that were in history
            keys_to_remove = set()
            for r in combined:
                keys_to_remove.add((str(r.get("descrição_normalizada") or ""), str(r.get("descricao") or "")))
            
            self.aggregation_results = [
                r for r in self.aggregation_results 
                if (str(r.get("descrição_normalizada") or ""), str(r.get("descricao") or "")) not in keys_to_remove
            ]
            self.aggregation_results.insert(0, result.aggregated_row)
            
            self.results_table_model.set_dataframe(pl.DataFrame(self.aggregation_results))
            self.results_table_view.resizeColumnsToContents()
            
            # Clear checks and reload top table
            self.aggregation_table_model.clear_checked()
            self.results_table_model.clear_checked()
            self.open_editable_aggregation_table()
            
            self.show_info(
                "Agregação concluída",
                f"As {len(combined)} descrições foram unificadas em:\n'{result.aggregated_row['descricao']}'"
            )
        except Exception as exc:
            self.show_error("Falha na agregação", str(exc))

    def apply_quick_filters(self) -> None:
        idx = self.tabs.currentIndex()
        if idx == 0: # Consulta
            fields = {
                "descricao_normalizada": self.qf_norm.text().strip(),
                "descricao": self.qf_desc.text().strip(),
                "ncm_padrao": self.qf_ncm.text().strip(),
                "cest_padrao": self.qf_cest.text().strip(),
            }
        elif idx == 1: # Agregação
            fields = {
                "descricao_normalizada": self.aqf_norm.text().strip(),
                "descricao": self.aqf_desc.text().strip(),
                "ncm_padrao": self.aqf_ncm.text().strip(),
                "cest_padrao": self.aqf_cest.text().strip(),
            }
        else:
            return

        # Keep non-quick filters if any, but replace quick filter columns
        quick_cols = set(fields.keys())
        new_filters = [f for f in (self.state.filters or []) if f.column not in quick_cols]
        
        for col, val in fields.items():
            if val:
                # Need to be flexible with column names as they might differ across files
                # We'll use the one present in the schema
                actual_col = col
                if self.state.all_columns:
                    # Match case-insensitive if needed, or handle variations like NCM_padrao
                    alternatives = {
                        "ncm_padrao": ["ncm_padrao", "NCM_padrao", "lista_ncm"],
                        "cest_padrao": ["cest_padrao", "CEST_padrao", "lista_cest"],
                    }
                    if col in alternatives:
                        for alt in alternatives[col]:
                            if alt in self.state.all_columns:
                                actual_col = alt
                                break
                    elif col not in self.state.all_columns:
                        # try case-insensitive and accent-insensitive match
                        target_clean = remove_accents(col).lower()
                        for c in self.state.all_columns:
                            if remove_accents(c).lower() == target_clean:
                                actual_col = c
                                break

                new_filters.append(FilterCondition(column=actual_col, operator="contém", value=val))
        
        self.state.filters = new_filters
        self.state.current_page = 1
        
        if idx == 0:
            self.reload_table()
        else:
            # For aggregation tab, we might want to just reload the file or filter in-memory.
            # But reload_table updates the main table. Let's make it work for aggregation too if it's the current file.
            # Actually, open_editable_aggregation_table reloads from disk.
            # Let's use the standard flow.
            self.reload_table(update_main_view=(idx==0))
            if idx == 1:
                # Update aggregation table with the filtered results
                self.aggregation_table_model.set_dataframe(self.current_page_df_all)
                self.aggregation_table_view.resizeColumnsToContents()

    def refresh_logs(self) -> None:
        self.log_view.setPlainText("\n".join(self.aggregation_service.load_log_lines()))

    def open_cnpj_folder(self) -> None:
        if not self.state.current_cnpj:
            self.show_error("CNPJ não selecionado", "Selecione um CNPJ para abrir a pasta.")
            return
        target = self.parquet_service.cnpj_dir(self.state.current_cnpj)
        if not target.exists():
            self.show_error("Pasta inexistente", f"A pasta {target} ainda não foi criada.")
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(str(target)))
