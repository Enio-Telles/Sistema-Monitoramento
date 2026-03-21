# Palette Journal

## 2025-02-14 - Adding accessibility to PySide6 Inputs
**Learning:** In PySide6 UI applications, `QLineEdit` inputs used for main actions or filtering often lack an easy way to clear them or submit them quickly via the keyboard. Adding `.setClearButtonEnabled(True)` and connecting the `.returnPressed` signal to the primary execution action vastly improves keyboard navigation and overall usability with minimal code.
**Action:** Always enable the clear button and bind the return key for critical input fields in Qt/PySide6 applications.
