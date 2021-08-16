from PyQt5.QtWidgets import QMainWindow
from PyQt5 import uic
from PyQt5.QtWidgets import QTableWidgetItem
from PyQt5.QtCore import Qt, QTimer, QTime

# Qt Designer의 객체를 form_class 에 할당
form_class = uic.loadUiType("pytrader.ui")[0]


class ContractWindow(QMainWindow, form_class):
    def __init__(self, kiwoom_instance, logger_):
        super().__init__()
        self.kiwoom = kiwoom_instance
        self.logger = logger_
        self.setupUi(self)