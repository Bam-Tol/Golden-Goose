from PyQt5.QtWidgets import QMainWindow
from PyQt5 import uic
from PyQt5.QtWidgets import QTableWidgetItem
from PyQt5.QtCore import Qt, QTimer, QTime
import pandas as pd

# Qt Designer의 객체를 form_class 에 할당
form_class_0345 = uic.loadUiType("pytrader_0345.ui")[0]  # 실시간 잔고
form_class_0156 = uic.loadUiType("pytrader_0156.ui")[0]  # 실시간 조건 검색
form_class_4989 = uic.loadUiType("pytrader_4989.ui")[0]  # 매수/매도/미체결
# 실시간 체결
# 일별 수익률, 누적 수익률

class WindowSetting0345(QMainWindow, form_class_0345):
    def __init__(self, kiwoom_instance, logger_):
        super().__init__()
        self.kiwoom = kiwoom_instance
        self.logger = logger_
        self.setupUi(self)
        self.check_status()

        # [0345] 실시간 잔고 계산

        # 실시간 등록
        self.kiwoom.real_time_registration()
        # self.auto_registration(20)  # 20초에 한 번씩 호출

        # 화면에 뿌리기
        self._display_profit_ratio()
        self.auto_display(0.1)  # 0.1 초에 한 번 씩 또는 0.5 초에 한 번 씩 호출

    """ [0345] 화면 구성 메서드 """
    def _display_profit_ratio(self):
        # self.logger.debug("수익률 display")
        possession_df, summary_df = self.kiwoom.real_time_profit_ratio()

        # [0345] 실시간 잔고(합계)
        self._set_table_widget(self.tableWidget_03451, summary_df)

        # [0345] 실시간 잔고(개별 종목)
        possession_columns = ['종목코드', '종목명', '평가손익(추정)', '수익률(추정)', '보유량', '평가금액', '매매가능수량',
                              '매수금액', '손익분기(추정)', '현재가', '수수료(추정)', '세금(추정)']
        self._set_table_widget(self.tableWidget_03452, possession_df[possession_columns])

    @staticmethod
    def _set_table_widget(table_widget, table_df):
        table_widget_columns = table_df.columns
        item_count = len(table_df[table_widget_columns[0]])
        table_widget.setRowCount(item_count)
        for row in range(item_count):
            for col, key in enumerate(table_widget_columns):
                item = QTableWidgetItem(str(table_df[key][row]))
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
                table_widget.setItem(row, col, item)
        table_widget.resizeRowsToContents()

    """ 이하 timeout을 위한 메서드"""

    def check_status(self):
        # 1초에 한 번씩 이벤트 발생 시킴
        timer = QTimer(self)
        timer.start(1000)
        timer.timeout.connect(self._timeout)

    def _timeout(self):
        current_time = QTime.currentTime()
        text_time = current_time.toString("hh:mm:ss")
        time_msg = "현재시간: " + text_time
        state = self.kiwoom.get_connect_state()
        if state == 1:
            state_msg = "서버 연결 정상"
        else:
            state_msg = "서버 연결 끊김"
        self.statusbar.showMessage(state_msg + " | " + time_msg)

    def auto_registration(self, seconds):
        timer2 = QTimer(self)
        timer2.start(1000 * seconds)  # 20초
        timer2.timeout.connect(self._timeout2)

    def _timeout2(self):
        self.kiwoom.real_time_registration()

    def auto_display(self, seconds):
        timer3 = QTimer(self)
        timer3.start(1000 * seconds)  # 0.1초
        timer3.timeout.connect(self._timeout3)

    def _timeout3(self):
        self._display_profit_ratio()


class WindowSetting0156(QMainWindow, form_class_0156):
    """
    매수 조건
    1. 거래량의 증가 속도가 평균보다 n배 이상 가팔라야 한다.
    2. 매수 잔량이 매도 잔량보다 많아야 한다(?)
    3. 시가보다 2%이상 오르면 안된다.
    """
    def __init__(self, kiwoom_instance, logger_):
        super().__init__()
        self.kiwoom = kiwoom_instance
        self.logger = logger_
        self.subscribed_df = pd.DataFrame()

        self.setupUi(self)
        self.check_status()

        # 조건식 목록 조회
        condition_name_dict = self._load_condition_name()
        self.listWidget_01561.addItems(list(condition_name_dict.keys()))

        # 클릭 시 이벤트 처리
        self.listWidget_01561.setAlternatingRowColors(True)
        self.listWidget_01562.setAlternatingRowColors(True)

        self.toolButton_01561.clicked.connect(self.go_right)
        self.toolButton_01562.clicked.connect(self.go_left)

        # 0.1초에 한 번 씩 업데이트
        self.auto_update(0.1)

    # 조건식 목록을 listWidget에 추가
    def _load_condition_name(self):
        return self.kiwoom.get_condition_name()

    # 구독 버튼 클릭 시
    def go_right(self):
        self._subscribe_current_item(self.listWidget_01561)
        self._move_current_item(self.listWidget_01561, self.listWidget_01562)

    # 구독 취소 버튼 클릭 시
    def go_left(self):
        self._unsubscribe_current_item(self.listWidget_01562)
        self._move_current_item(self.listWidget_01562, self.listWidget_01561)

    @staticmethod
    def _move_current_item(source, target):
        if source.currentItem():
            row = source.currentRow()
            target.addItem(source.takeItem(row))  # pop

    # 구독 버튼 클릭시 발생
    def _subscribe_current_item(self, source):
        if source.currentItem():
            condition_name = source.currentItem().text()
            # 조건식 만족하는 dictionary 가져오기(self.condition_tr_dict 와 동일)
            condition_tr_dict = self.kiwoom.get_condition_tr_dict(condition_name)

            # 구독 신청
            self.subscribed_df = self.kiwoom.init_condition_tr_dict(condition_tr_dict)

            # [0156] 조건식을 만족하는 종목 테이블에 추가
            self._set_table_widget(self.tableWidget_01561, self.subscribed_df)

    # 구독 취소 버튼 클릭시 발생
    def _unsubscribe_current_item(self, source):
        if source.currentItem():
            condition_name = source.currentItem().text()
            # 구독 취소하기(self.condition_tr_dict 와 동일)
            self.subscribed_df = self.kiwoom.stop_condition_tr_dict(condition_name, self.subscribed_df)

            # [0156] 조건식을 만족하는 종목 테이블에 추가
            self._set_table_widget(self.tableWidget_01561, self.subscribed_df)

    @staticmethod
    def _set_table_widget(table_widget, subscribed_df):
        # row 길이 정의
        item_count = len(subscribed_df['조건식'])
        table_widget.setRowCount(item_count)
        table_widget_columns = subscribed_df.columns.tolist()
        # 테이블에 숫자 집어넣기
        for row in range(item_count):
            for col, column_name in enumerate(table_widget_columns):
                item = QTableWidgetItem(str(subscribed_df[column_name][row]))
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
                table_widget.setItem(row, col, item)
        table_widget.resizeRowsToContents()

    """ 이하 timeout을 위한 메서드"""

    def check_status(self):
        # 1초에 한 번씩 이벤트 발생 시킴
        timer = QTimer(self)
        timer.start(1000)
        timer.timeout.connect(self._timeout)

    def _timeout(self):
        current_time = QTime.currentTime()
        text_time = current_time.toString("hh:mm:ss")
        time_msg = "현재시간: " + text_time
        state = self.kiwoom.get_connect_state()
        if state == 1:
            state_msg = "서버 연결 정상"
        else:
            state_msg = "서버 연결 끊김"
        self.statusbar.showMessage(state_msg + " | " + time_msg)

    def auto_update(self, seconds):
        timer2 = QTimer(self)
        timer2.start(1000 * seconds)  # seconds초
        timer2.timeout.connect(self._timeout2)

    def _timeout2(self):
        # self.logger.debug('조건식 종목을 update 합니다.')
        """ 얘는 그냥 실시간으로 들어온 현재가를 update 하는 용도임"""

        if len(self.subscribed_df) > 0:
            self.subscribed_df = self.kiwoom.update_real_time_price(self.subscribed_df)
            self._set_table_widget(self.tableWidget_01561, self.subscribed_df)

    """ #### 이상 확정 메서드 #### """


class WindowSetting4989(QMainWindow, form_class_4989):
    """
    (06/06) 해야할 일: 매수/매도 체결시, d2_deposit & possession_list 최신화하기
    """
    def __init__(self, kiwoom_instance, logger_):
        super().__init__()
        self.kiwoom = kiwoom_instance
        self.logger = logger_
        self.setupUi(self)
        self.non_traded_df = self.kiwoom.get_non_chegyul_jongmok()
        self.check_status()

        # 미체결 list initializer 해야함...

        # 보유종목에 없고, 미체결 종목에 없는 조건식 목록 조회
        filtered_df = self.kiwoom.buy_available_list()
        self._set_table_widget_49891(self.tableWidget_49891, filtered_df)
        self._set_table_widget_49893(self.tableWidget_49893, self.non_traded_df)

        # # 클릭 시 이벤트 처리
        # self.listWidget_01561.setAlternatingRowColors(True)
        # self.listWidget_01562.setAlternatingRowColors(True)

        # self.toolButton_01561.clicked.connect(self.go_right)
        # self.toolButton_01562.clicked.connect(self.go_left)

        # 1초에 한 번 씩 업데이트
        self.auto_update(1)

    @staticmethod
    def _set_table_widget_49891(table_widget, subscribed_df):
        # row 길이 정의
        item_count = len(subscribed_df['조건식'])
        table_widget.setRowCount(item_count)
        # table_widget_columns = subscribed_df.columns.tolist()
        table_widget_columns = ['조건식', '종목코드', '종목명']
        # 테이블에 숫자 집어넣기
        for row in range(item_count):
            for col, column_name in enumerate(table_widget_columns):
                item = QTableWidgetItem(str(subscribed_df[column_name][row]))
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
                table_widget.setItem(row, col, item)
        table_widget.resizeRowsToContents()

    @staticmethod
    def _set_table_widget_49893(table_widget, subscribed_df):
        # row 길이 정의
        item_count = len(subscribed_df['종목코드'])
        table_widget.setRowCount(item_count)
        # table_widget_columns = subscribed_df.columns.tolist()
        table_widget_columns = ['종목코드', '종목명']
        # 테이블에 숫자 집어넣기
        for row in range(item_count):
            for col, column_name in enumerate(table_widget_columns):
                item = QTableWidgetItem(str(subscribed_df[column_name][row]))
                item.setTextAlignment(Qt.AlignVCenter | Qt.AlignRight)
                table_widget.setItem(row, col, item)
        table_widget.resizeRowsToContents()


    """ 이하 timeout을 위한 메서드"""

    def check_status(self):
        # 1초에 한 번씩 이벤트 발생 시킴
        timer = QTimer(self)
        timer.start(1000)
        timer.timeout.connect(self._timeout)

    def _timeout(self):
        current_time = QTime.currentTime()
        text_time = current_time.toString("hh:mm:ss")
        time_msg = "현재시간: " + text_time
        state = self.kiwoom.get_connect_state()
        if state == 1:
            state_msg = "서버 연결 정상"
        else:
            state_msg = "서버 연결 끊김"
        self.statusbar.showMessage(state_msg + " | " + time_msg)

    def auto_update(self, seconds):
        timer2 = QTimer(self)
        timer2.start(1000 * seconds)  # seconds초
        timer2.timeout.connect(self._timeout2)
        # timer2.timeout.connect(self._timeout3)
        timer2.timeout.connect(self._timeout4)

    def _timeout2(self):
        # 매수할 종목
        filtered_df = self.kiwoom.buy_available_list()
        self._set_table_widget_49891(self.tableWidget_49891, filtered_df)

        # 매수 신청
        if self.checkBox_49891.isChecked():
            self.kiwoom.buy_condition_list(filtered_df)

    def _timeout3(self):
        # 매도할 종목
        filtered_df = self.kiwoom.buy_available_list()
        self._set_table_widget_49892(self.tableWidget_49892, filtered_df)

        # 매도
        if self.checkBox_49892.isChecked():
            self.kiwoom.sell_condition_list()

    def _timeout4(self):
        # 미체결 종목
        self.non_traded_df = self.kiwoom.get_non_traded_df(self.non_traded_df)
        self._set_table_widget_49893(self.tableWidget_49893, self.non_traded_df)

        # 미체결 종목 자동 취소
        # if self.checkBox_49893.isChecked():
        #     self.kiwoom.sell_condition_list()







