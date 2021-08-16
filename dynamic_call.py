from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop

import time
from datetime import timedelta
import datetime


# 오늘 날짜를 설정하는 함수
def date_setting():
    today = datetime.datetime.today().strftime("%Y%m%d")
    today_detail = datetime.datetime.today().strftime("%Y%m%d%H%M")
    today_date_form = datetime.datetime.strptime(today, "%Y%m%d").date()
    return today, today_detail, today_date_form


class DynamicCall(QAxWidget):
    """ DynamicCall dynamicCall 메서드를 모아놓은 클래스 입니다. """
    def __init__(self):
        super().__init__()
        self.account_number = None
        self.tr_counter = 0
        self.tr_request_list = []
        self.today, self.today_detail, self.today_date_form = date_setting()

    """ 0. 로그인과 버전처리"""
    def comm_connect(self):
        self.login_event_loop = QEventLoop()
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec()

    def get_login_info(self):
        return self.dynamicCall("GetLoginInfo(QString)", "ACCNO").split(';')[0]

    # 키움 서버와의 연결 상태 확인
    def get_connect_state(self):
        return self.dynamicCall("GetConnectState()")

    """ 1. 조회와 실시간 데이터 처리"""
    def set_input_value(self, id_name, value):
        self.dynamicCall("SetInputValue(QString, QString)", id_name, value)

    # TR 요청은 1초에 5회 이상 조회 시 정지먹음
    def comm_rq_data(self, rqname, trcode, prev_next, screen_no):
        self.tr_counter += 1
        self.logger.debug(f"TR 요청 횟수: {self.tr_counter}")
        self._limit_checker()

        error_code = self.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, prev_next, screen_no)

        if error_code == -200:
            raise self.logger.error('요청제한 횟수를 초과하였습니다.')
        elif error_code == 0:
            self.logger.debug(f"서버에 TR을 요청했습니다. 메서드: _comm_rq_data")
            self.tr_event_loop = QEventLoop()  # 키움증권 서버로부터 이벤트가 올 때까지 대기하기 위한 루프
            self.tr_event_loop.exec_()

    def comm_kw_rq_data(self, code_list, code_counts, screen_no):
        """
        복수 종목 조회를 위한 함수
        :param code_list: list 형식
        :param code_counts: list의 길이
        :return: OnReceiveTrData 이벤트 발생

          BSTR sArrCode,    // 조회하려는 종목코드 리스트
          BOOL bNext,   // 연속조회 여부 0:기본값, 1:연속조회(지원안함)
          int nCodeCount,   // 종목코드 갯수
          int nTypeFlag,    // 0:주식 종목, 3:선물옵션 종목
          BSTR sRQName,   // 사용자 구분명
          BSTR sScreenNo    // 화면번호
        """
        self.tr_counter += 1
        self.logger.debug(f"TR 요청 횟수: {self.tr_counter}")
        self._limit_checker()

        error_code = self.dynamicCall("CommKwRqData(QString, QBoolean, int, int, QString, QString)",
                                      ";".join(code_list), 0, code_counts, 0, "관심종목조회", screen_no)

        if error_code == -200:
            raise self.logger.error('요청제한 횟수를 초과하였습니다.')
        if error_code == 0:
            self.logger.debug(f"서버에 TR을 요청했습니다. 메서드: CommKwRqData")
            self.tr_event_loop = QEventLoop()  # 키움증권 서버로부터 이벤트가 올 때까지 대기하기 위한 루프
            self.tr_event_loop.exec_()

    def _limit_checker(self):
        time.sleep(0.1)
        now = datetime.datetime.now()
        self.tr_request_list.append(now)
        self.tr_request_list = [elem for elem in self.tr_request_list if now - elem < timedelta(seconds=3600)]
        counts_by_hour = len(self.tr_request_list)
        if counts_by_hour < 990:
            pass
        else:
            self.logger.debug("요청 횟수가 1시간 이내에 990번 이상입니다... 1시간 이내 요청 횟수가 300번 미만이 될 때까지 일시정지합니다...")
            while counts_by_hour > 300:
                time.sleep(60)
                now = datetime.datetime.now()
                self.logger.debug(f"1분 대기합니다.. 현재 1시간 이내 요청 횟수는 {counts_by_hour} 입니다...")
                self.tr_request_list = [elem for elem in self.tr_request_list if now - elem < timedelta(seconds=3600)]
                counts_by_hour = len(self.tr_request_list)
            self.logger.debug("재개합니다...")

    # 레코드 반복횟수를 반환
    def get_repeat_cnt(self, trcode, rqname):
        return self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)

    # TR 이벤트 수신 데이터 반환
    def get_comm_data(self, trcode, rqname, index, item_name):
        result = self.dynamicCall("GetCommData(QString, QString, int, QString)", trcode, rqname, index, item_name)
        return result.strip()

    def get_comm_real_data(self, code, fid):
        return self.dynamicCall("GetCommRealData(QString, int)", code, fid)

    """ 2. 주문과 잔고처리 """
    # 주문 화면을 따로 만든다.
    def send_order(self, order_type, jongmok_code, quantity, price, hoga_gb, order_no):
        self.dynamicCall("SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
                         ["send_order_req", "0101", self.account_number, order_type, jongmok_code, quantity, price,
                          hoga_gb, order_no])

        self.tr_event_loop = QEventLoop()
        self.tr_event_loop.exec_()

    def get_chejan_data(self, fid_list):
        my_list = []
        for fid in fid_list:
            my_list.append(self.dynamicCall("GetChejanData(int)", fid))
        return my_list

    """ 3. 조건 검색 """
    def get_condition_load(self):
        self.dynamicCall("GetConditionLoad()")
        self.tr_condition_ver_loop = QEventLoop()  # 키움증권 서버로부터 이벤트가 올 때까지 대기하기 위한 루프
        self.tr_condition_ver_loop.exec_()

    def get_condition_name_list(self):
        return self.dynamicCall("GetConditionNameList()")

    def send_condition(self, condition_name):
        condition_number = self.condition_name_dict[condition_name]
        self.dynamicCall("SendCondition(QString, QString, int, int)", '0'+str(156+condition_number), condition_name, condition_number, 1)
        self.tr_condition_loop = QEventLoop()  # 키움증권 서버로부터 이벤트가 올 때까지 대기하기 위한 루프
        self.tr_condition_loop.exec_()

    def stop_condition(self, condition_name):
        condition_number = self.condition_name_dict[condition_name]
        self.dynamicCall("SendConditionStop(QString, QString, int)", '0'+str(156+condition_number), condition_name, condition_number)

    def set_real_remove(self, screen_no, code):
        """
        실시간 조회 해지 함수
        :param screen_no: 화면번호 또는 ALL
        :param code: 종목코드 또는 ALL
        ex) SetRealRemove("0150", "039490");  // "0150"화면에서 "039490"종목 실시간 해지
            SetRealRemove("ALL", "ALL");  // 모든 화면에서 모든종목 실시간 해지
            SetRealRemove("0150", "ALL");  // "0150"화면에서 모든종목 실시간 해지
            SetRealRemove("ALL", "039490");  // 모든 화면에서 "039490"종목 실시간 해지
        """
        self.dynamicCall("SetRealRemove(QString, QString)", screen_no, code)

    def set_real_reg(self, screen_no, code_list, fid_list, option_type):
        """
        실시간 조회 등록 함수
        :param screen_no: 화면번호
        :param code_list: 종목코드 리스트
        :param fid_list: 실시간 FID 리스트
        :param option_type: 실시간 등록 타입, 0 or 1

        종목코드와 FID 리스트를 이용해서 실시간 시세를 등록하는 함수입니다.
        한번에 등록가능한 종목과 FID갯수는 100종목, 100개 FID 입니다.
        실시간 등록타입을 0으로 설정하면 등록한 종목들은 실시간 해지되고 등록한 종목만 실시간 시세가 등록됩니다.
        실시간 등록타입을 1로 설정하면 먼저 등록한 종목들과 함께 실시간 시세가 등록됩니다
        """
        self.dynamicCall("SetRealReg(QString, QString, QString, QString)", screen_no, code_list, fid_list, option_type)

    """ 4. 기타함수 """
    def get_master_code_name(self, code):
        self.dynamicCall("GetMasterCodeName(QString)", code)


    """ 번외) Caller 와 Handler에서 둘 다 사용하는 함수 """

