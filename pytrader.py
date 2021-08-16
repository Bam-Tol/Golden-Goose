import sys
from PyQt5.QtWidgets import QMainWindow, QApplication
from kiwoom_event import KiwoomEvent
import window_setting
from logger import define_logger


logger = define_logger()


class MyWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        kiwoom = KiwoomEvent(logger)
        self.window_0345 = window_setting.WindowSetting0345(kiwoom, logger)
        self.window_0156 = window_setting.WindowSetting0156(kiwoom, logger)
        self.window_4989 = window_setting.WindowSetting4989(kiwoom, logger)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    myWindow = MyWindow()
    myWindow.window_0345.show()
    myWindow.window_0156.show()
    myWindow.window_4989.show()
    app.exec_()
