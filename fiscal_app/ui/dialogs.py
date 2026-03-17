from __future__ import annotations

from PySide6.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QListWidget,
    QListWidgetItem,
    QVBoxLayout,
)
from PySide6.QtCore import Qt


class ColumnSelectorDialog(QDialog):
    def __init__(self, columns: list[str], visible_columns: list[str], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Selecionar colunas visíveis")
        self.resize(420, 520)

        self.list_widget = QListWidget()
        visible = set(visible_columns)
        for col in columns:
            item = QListWidgetItem(col)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Checked if col in visible else Qt.Unchecked)
            self.list_widget.addItem(item)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(self.list_widget)
        layout.addWidget(buttons)

    def selected_columns(self) -> list[str]:
        selected = []
        for idx in range(self.list_widget.count()):
            item = self.list_widget.item(idx)
            if item.checkState() == Qt.Checked:
                selected.append(item.text())
        return selected
