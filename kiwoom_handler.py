from dynamic_call import DynamicCall
import re
import pandas as pd
import time
import copy


class RealTypeDictInitializer:
    def __init__(self):
        self.real_type_001_df = self._init_real_type_001()  # real type: 주식 시세
        self.real_type_002_df = self._init_real_type_002()  # real type: 주식 체결
        self.real_type_004_df = self._init_real_type_004()  # real type: 주식호가잔량
        self.chejan_df = self._init_chejan()

    @staticmethod
    def _init_real_type_001():
        columns = ['']
        df = pd.DataFrame(columns=columns)
        return df

    @staticmethod
    def _init_real_type_002():
        fid_encode = ['체결시간', '현재가', '전일대비', '등락율', '(최우선)매도호가', '(최우선)매수호가', '거래량', '누적거래량',
                      '누적거래대금', '시가', '고가', '저가', '전일대비기호', '전일거래량대비(계약,주)', '거래대금증감', '전일거래량대비(비율)',
                      '거래회전율', '거래비용', '체결강도', '시가총액(억)', '장구분', 'KO접근도', '상한가발생시간', '하한가발생시간']
        fid = [20, 10, 11, 12, 27, 28, 15, 13, 15, 16, 17, 18, 25, 26, 29, 30, 31, 32, 228, 311, 290, 691, 567, 568]
        real_type_002_df = pd.DataFrame({'fid': fid, 'fid_encode': fid_encode})
        return real_type_002_df

    @staticmethod
    def _init_real_type_004():
        columns = ['계좌번호',]
        df = pd.DataFrame(columns=columns)
        return df

    @staticmethod
    def _init_chejan():
        columns = ['계좌번호', '주문번호', '관리자사번', '종목코드', '주문업무분류', '주문상태',
                   '종목명', '주문수량', '주문가격', '미체결수량', '체결누계금액', '원주문번호',
                   '주문구분', '매매구분', '매도수구분', '주문/체결시간', '체결번호', '체결가', '체결량',
                   '현재가', '(최우선)매도호가', '(최우선)매수호가', '단위체결가', '단위체결량',
                   '당일매매수수료', '당일매매세금', '거부사유', '화면번호', '터미널번호', '신용구분', '대출일']
        chejan_df = pd.DataFrame(columns=columns)
        return chejan_df


class Handler(DynamicCall, RealTypeDictInitializer):
    """
    Handler 클래스는 이벤트가 발생했을 때 이를 처리하기 위한 함수들을 모아 놓은 클래스 입니다.
    """

    def __init__(self):
        super().__init__()
        self._set_event_connector()
        self.condition_tr_dict = {}

    def _set_event_connector(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        self.OnReceiveMsg.connect(self._handler_message)

        self.OnEventConnect.connect(self._handler_login)
        self.OnReceiveTrData.connect(self._handler_tr_data)

        self.OnReceiveConditionVer.connect(self._handler_condition_ver_data)
        self.OnReceiveTrCondition.connect(self._handler_tr_condition_data)
        self.OnReceiveRealCondition.connect(self._handler_real_condition_data)

        self.OnReceiveRealData.connect(self._handler_real_data)
        self.OnReceiveChejanData.connect(self._handler_chejan_data)

    """ 이벤트 수신을 위한 함수 """

    def _handler_login(self, err_code):
        if err_code == 0:
            self.logger.debug("로그인 성공")
        self.login_event_loop.exit()

    def _handler_message(self, screen_no, rq_name, tr_code, msg):
        self.logger.debug(f"{screen_no, rq_name, tr_code, msg}")

    def _handler_real_data(self, code, real_type, real_data):
        self.logger.debug(f"서버로부터 [실시간] 이벤트를 전달 받았습니다. 메서드: _handler_real_data")
        self.logger.debug(f"종목 코드: {code}, 실시간 타입: {real_type}")
        if real_type == '주식체결':
            self._real_type_002(code)
        elif real_type == '주식호가잔량':
            pass
        else:
            pass

    def _handler_chejan_data(self, gubun, item_cnt, fid_list):
        # 이거 거의 무쓸모 이벤트임 -> 아닌가...? -> 이걸로 실시간 잔고 만들 수 있음(물론 쉽지 않겠지만)

        self.logger.debug(f"서버로부터 [체결] 이벤트를 전달 받았습니다. 메서드: _handler_chejan_data")
        self.logger.debug(f"구분: {gubun}, item_cnt: {item_cnt}, fid_list: {fid_list}")
        fid_list = fid_list.rstrip(';').split(';')
        # code_name_index = fid_list.index('302')

        if gubun == '0':  # 접수 및 체결
            result_list = self.get_chejan_data(fid_list)
            my_dict = dict(zip(self.chejan_df.columns, result_list))
            self.chejan_df = self.chejan_df.append(my_dict, ignore_index=True)

            # self.logger.debug(f"{result}")
        elif gubun == '1':  # 국내주식잔고 변경
            # 얘가 복수 종목 조회해주면 좋은데
            # result = self.get_chejan_data(fid_list[code_name_index])
            # self.logger.debug(f"{result}")
            pass
        elif gubun == '4':  # 파생잔고 변경
            # result = self.get_chejan_data(fid_list[code_name_index])
            # self.logger.debug(f"{result}")
            pass

    def _handler_condition_ver_data(self, ret, msg):
        self.logger.debug(f"서버로부터 [조건식 불러오기] 이벤트를 전달 받았습니다. 메서드: _handler_condition_ver_data")
        if ret == 1:
            self.logger.debug(f"{msg}")
            condition_list_str = self.get_condition_name_list()
            condition_list = condition_list_str.rstrip(';').split(';')

            condition_name_dict = {}
            for elem in condition_list:
                condition_name_dict[elem.split('^')[1]] = int(elem.split('^')[0])
            self.condition_name_dict = condition_name_dict
        self.tr_condition_ver_loop.exit()

    def _handler_tr_condition_data(self, screen_no, code_list, condition_name, condition_num, is_next):
        """
        조건식에 해당하는 종목 가져오기
        :param screen_no: 화면번호
        :param code_list: 종목코드 리스트
        :param condition_name: 조건식 이름
        :param condition_num: 조건식 번호
        :param is_next: 연속조회 여부 (0- 연속x, 1- 연속)
        :return : self.condition_tr_dict
        """
        self.logger.debug(f"서버로부터 [{condition_name} 조건식 해당 리스트 초깃값] 이벤트를 전달 받았습니다. 메서드: _handler_tr_condition_data")
        self.logger.debug(f"{screen_no, code_list, condition_name, condition_num, is_next}")
        self.condition_tr_dict[condition_name] = code_list.rstrip(';').split(';')

        # [0156] set_real_reg 추가 등록 (real_type 주식 체결) comm_kw로 자동 구독
        # self.set_real_reg(screen_no, code_list, '10;11;12', 1)
        self.tr_condition_loop.exit()

    def _handler_real_condition_data(self, code, event_type, condition_name, condition_index):
        """
        조건식에 실시간 편입 또는 이탈하는 종목 발생 시 호출
        :param code: 종목코드
        :param event_type: 이벤트 종류, 'I'- 종목 편입, 'D'- 종목 이탈
        :param condition_name: 조건식 이름
        :param condition_index: 조건식 고유번호
        :return: self.condition_tr_dict
        """
        self.logger.debug(f"서버로부터 [실시간 {condition_name} 조건식] 이벤트를 전달 받았습니다. 메서드: _handler_real_condition_data")

        if event_type == 'I':
            self.logger.debug(f"{code}가 {condition_name} 조건식에 편입 되었습니다.")
            incorporated_list = code.rstrip(';').split(';')
            tmp = copy.deepcopy(self.condition_tr_dict[condition_name])
            self.condition_tr_dict[condition_name] = tmp + [elem for elem in incorporated_list if elem not in tmp]
            self.logger.debug(f"{incorporated_list} 종목이 {condition_name} 조건식에 편입되었습니다.")

            # [0156] set_real_reg 추가 등록
            condition_number = self.condition_name_dict[condition_name]
            self.set_real_reg('0'+str(156+condition_number), code, '10;11;12', 1)

        elif event_type == 'D':
            self.logger.debug(f"{code}가 {condition_name} 조건식에서 이탈 되었습니다.")
            broke_away_list = code.rstrip(';').split(';')
            tmp = copy.deepcopy(self.condition_tr_dict[condition_name])
            self.condition_tr_dict[condition_name] = [elem for elem in tmp if elem not in broke_away_list]
            self.logger.debug(f"{broke_away_list} 종목이 {condition_name} 조건식에서 이탈되었습니다.")

            # [0156] set_real_remove 종목만 해지
            condition_number = self.condition_name_dict[condition_name]
            self.set_real_remove('0'+str(156+condition_number), code)
        else:
            self.logger.error("오잉?!")

    def _handler_tr_data(self, screen_no, rqname, trcode, record_name, prev_next, unused1, unused2, unused3, unused4):
        # self.logger.debug(f"서버로부터 [{rqname}] TR 이벤트를 전달 받았습니다. 메서드: _receive_tr_data")

        # prev_next가 2이면 똑같은 TR을 한 번 더 요청
        if prev_next == '2':
            self.remained_data = True
        else:
            self.remained_data = False

        if rqname == "opw00001_req":
            self.logger.debug("d+2 예수금을 가져옵니다.")
            self._result_opw00001 = self._opw00001(rqname, trcode)
        elif rqname == "opw00018_req":
            self.logger.debug("총 평가 잔고를 가져옵니다.")
            self._result_opw00018 = self._opw00018(rqname, trcode)
        elif rqname == 'opt10073_req':
            self.logger.debug("당일 체결량과 매도 손익을 가져옵니다.")
            # self._result_opt10073 = self._opt10073(rqname, trcode)
        elif rqname == 'opt10074_req':
            self.logger.debug("일자별 실현 손익을 가져옵니다.")
            # self._result_opt10074 = self._opt10074(rqname, trcode)
        elif rqname == "opt10081_req":
            self.logger.debug("종목의 일자별 거래데이터를 가져옵니다.")
            # self._result_opt10081 = self._opt10081(rqname, trcode)
        elif rqname == "opt10075_req":
            self.logger.debug("당일 체결/미체결 내역을 가져옵니다.")
            self._result_opt10075 = self._opt10075(rqname, trcode)
        elif rqname == "opt10085_req":
            self.logger.debug("잔고를 가져옵니다.")
            # self._result_opt10085 = self._opt10085(rqname, trcode)
        elif rqname == "관심종목조회":
            self._result_OPTKWFID = self._OPTKWFID(rqname, trcode)
        elif rqname == "send_order_req":
            pass

        # 이벤트 루프 종료
        # self.logger.debug(f"TR 수신 이벤트 루프를 종료합니다. 메서드: _receive_tr_data")
        self.tr_event_loop.exit()

    def _opw00001(self, rqname, trcode):
        d2_deposit_before_format = self.get_comm_data(trcode, rqname, 0, "d+2출금가능금액")
        d2_deposit = self._change_format(d2_deposit_before_format)
        return d2_deposit

    # 이번에는 opw00018 TR을 위한 코드를 추가하겠습니다. opw00018 TR은 싱글 데이터를 통해 계좌에 대한 평가 잔고 데이터를 제공하며 멀티 데이터를 통해 보유 종목별 평가 잔고 데이터를 제공합니다.
    # 먼저 총매입금액, 총평가금액, 총평가손익금액, 총수익률, 추정예탁자산을 get_comm_data 메서드를 통해 얻어옵니다. 얻어온 데이터는 change_format 메서드를 통해 포맷을 문자열로 변경합니다.
    def _opw00018(self, rqname, trcode):
        single_result = dict()
        # 전역변수로 사용하기 위해서 총매입금액은 self로 선언
        total_purchase_price = self._change_format(self.get_comm_data(trcode, rqname, 0, "총매입금액"))
        total_eval_price = self._change_format(self.get_comm_data(trcode, rqname, 0, "총평가금액"))
        total_eval_profit_loss_price = self._change_format(self.get_comm_data(trcode, rqname, 0, "총평가손익금액"))
        total_earning_rate = self._change_format2(self.get_comm_data(trcode, rqname, 0, "총수익률(%)"))
        estimated_deposit = self._change_format(self.get_comm_data(trcode, rqname, 0, "추정예탁자산"))

        single_result['total_purchase_price'] = total_purchase_price
        single_result['total_eval_price'] = total_eval_price
        single_result['total_eval_profit_loss_price'] = total_eval_profit_loss_price
        single_result['total_earning_rate'] = total_earning_rate
        single_result['estimated_deposit'] = estimated_deposit

        # 참고로 opw00018 TR을 사용하는 경우 한 번의 TR 요청으로 최대 20개의 보유 종목에 대한 데이터를 얻을 수 있습니다.
        columns = ['종목코드', '종목명', '수익률(%)', '평가손익', '보유량', '매입가', '현재가',
                   '매수금액', '평가금액', '총수수료', '세금', '매매가능수량', '매입수수료']
        multiple_result = {key: [] for key in columns}
        rows = self.get_repeat_cnt(trcode, rqname)
        code_pattern = re.compile(r'\d{6}')  # 이거 지우지마...
        for i in range(rows):
            multiple_result['종목코드'].append(code_pattern.search(self.get_comm_data(trcode, rqname, i, "종목번호")).group(0))
            multiple_result['종목명'].append(self.get_comm_data(trcode, rqname, i, "종목명"))
            multiple_result['평가손익'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "평가손익")))
            multiple_result['수익률(%)'].append(self._change_format2(self.get_comm_data(trcode, rqname, i, "수익률(%)")))
            multiple_result['보유량'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "보유수량")))
            multiple_result['평가금액'].append(self._change_format2(self.get_comm_data(trcode, rqname, i, "평가금액")))
            multiple_result['매매가능수량'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "매매가능수량")))
            multiple_result['매수금액'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "매입금액")))
            multiple_result['현재가'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "현재가")))
            multiple_result['총수수료'].append(self._change_format2(self.get_comm_data(trcode, rqname, i, "수수료합")))
            multiple_result['세금'].append(self._change_format2(self.get_comm_data(trcode, rqname, i, "세금")))
            multiple_result['매입수수료'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "매입수수료")))
            multiple_result['매입가'].append(self._change_format(self.get_comm_data(trcode, rqname, i, "매입가")))
        result_dict = {'single_result': single_result, 'multiple_result': multiple_result}
        return result_dict

    def _opt10073(self, rqname, trcode):
        columns = ['date', 'code', 'code_name', 'amount', 'today_profit', 'earning_rate']
        multiple_result = {key: [] for key in columns}
        rows = self.get_repeat_cnt(trcode, rqname)
        for i in range(rows):
            multiple_result['date'].append(self.get_comm_data(trcode, rqname, i, "일자"))
            multiple_result['code'].append(self.get_comm_data(trcode, rqname, i, "종목코드").lstrip('A'))
            multiple_result['code_name'].append(self.get_comm_data(trcode, rqname, i, "종목명"))
            multiple_result['amount'].append(self.get_comm_data(trcode, rqname, i, "체결량"))
            multiple_result['today_profit'].append(self.get_comm_data(trcode, rqname, i, "당일매도손익"))
            multiple_result['earning_rate'].append(self.get_comm_data(trcode, rqname, i, "손익율"))
        return multiple_result

    # 일별실현손익
    def _opt10074(self, rqname, trcode):
        single_result = dict()
        single_result['total_profit'] = self.get_comm_data(trcode, rqname, 0, "실현손익")  # total 실현손익
        single_result['today_profit'] = self.get_comm_data(trcode, rqname, 0, "당일매도손익")  # 오늘 실현손익
        return single_result

    def _opt10081(self, rqname, trcode):
        data_cnt = self.get_repeat_cnt(trcode, rqname)
        columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        multiple_result = {key: [] for key in columns}
        for i in range(0, data_cnt):
            multiple_result['date'].append(self.get_comm_data(trcode, rqname, i, "일자"))
            multiple_result['open'].append(int(self.get_comm_data(trcode, rqname, i, "시가")))
            multiple_result['high'].append(int(self.get_comm_data(trcode, rqname, i, "고가")))
            multiple_result['low'].append(int(self.get_comm_data(trcode, rqname, i, "저가")))
            multiple_result['close'].append(int(self.get_comm_data(trcode, rqname, i, "현재가")))
            multiple_result['volume'].append(int(self.get_comm_data(trcode, rqname, i, "거래량")))
        return multiple_result

    def _opw00007(self, rqname, trcode):
        columns = ['order_code', 'order_type', 'code', 'code_name', 'order_amount', 'chegyul_amount',
                   'non_chegyul_amount', 'chegyul_price']
        multiple_result = {key: [] for key in columns}
        rows = self.get_repeat_cnt(trcode, rqname)
        for i in range(rows):
            multiple_result['order_code'].append(self.get_comm_data(trcode, rqname, i, "주문번호"))
            multiple_result['order_type'].append(self.get_comm_data(trcode, rqname, i, "주문구분"))
            multiple_result['code'].append(self.get_comm_data(trcode, rqname, i, "종목번호").lstrip('A'))
            multiple_result['code_name'].append(self.get_comm_data(trcode, rqname, i, "종목명"))
            multiple_result['order_amount'].append(self.get_comm_data(trcode, rqname, i, "주문수량"))
            multiple_result['chegyul_amount'].append(self.get_comm_data(trcode, rqname, i, "체결수량"))
            multiple_result['non_chegyul_amount'].append(self.get_comm_data(trcode, rqname, i, "주문잔량"))
            multiple_result['chegyul_price'].append(self.get_comm_data(trcode, rqname, i, "체결단가"))
        return multiple_result

    def _opt10075(self, rqname, trcode):
        columns = ['order_time', 'order_code', 'original_order_code', 'order_type',
                   'code', 'code_name', 'chegyul_price', 'chegyul_amount', 'non_chegyul_amount']
        multiple_result = {key: [] for key in columns}
        rows = self.get_repeat_cnt(trcode, rqname)
        for i in range(rows):
            multiple_result['order_time'].append(self.get_comm_data(trcode, rqname, i, "시간"))
            multiple_result['order_code'].append(self.get_comm_data(trcode, rqname, i, "주문번호"))
            multiple_result['original_order_code'].append(self.get_comm_data(trcode, rqname, i, "원주문번호"))
            multiple_result['order_type'].append(self.get_comm_data(trcode, rqname, i, "주문구분").lstrip('+|-'))
            multiple_result['code'].append(self.get_comm_data(trcode, rqname, i, "종목코드").lstrip('A'))
            multiple_result['code_name'].append(self.get_comm_data(trcode, rqname, i, "종목명"))
            multiple_result['chegyul_price'].append(self.get_comm_data(trcode, rqname, i, "체결가"))
            multiple_result['chegyul_amount'].append(self.get_comm_data(trcode, rqname, i, "체결량"))
            multiple_result['non_chegyul_amount'].append(self.get_comm_data(trcode, rqname, i, "미체결수량"))
        return multiple_result

    def _OPTKWFID(self, rqname, trcode):
        """
        : 가져올 수 있는 항목

        종목코드, 종목명, 현재가, 기준가, 전일대비, 전일대비기호, 등락율, 거래량, 거래대금,
        체결량, 체결강도, 전일거래량대비, 매도호가, 매수호가, 매도1~5차호가, 매수1~5차호가,
        상한가, 하한가, 시가, 고가, 저가, 종가, 체결시간, 예상체결가, 예상체결량, 자본금,
        액면가, 시가총액, 주식수, 호가시간, 일자, 우선매도잔량, 우선매수잔량,우선매도건수,
        우선매수건수, 총매도잔량, 총매수잔량, 총매도건수, 총매수건수, 패리티, 기어링, 손익분기,
        잔본지지, ELW행사가, 전환비율, ELW만기일, 미결제약정, 미결제전일대비, 이론가,
        내재변동성, 델타, 감마, 쎄타, 베가, 로
        """
        self.logger.debug("_OPTKWFID 함수 안에 들어왔습니다.")
        columns = ['code', 'code_name', 'market_price', 'start_price', 'high_price', 'contract_strength', 'preferred_buy_remaining']
        multiple_result = {key: [] for key in columns}
        rows = self.get_repeat_cnt(trcode, rqname)
        for i in range(rows):
            multiple_result['code'].append(self.get_comm_data(trcode, rqname, i, "종목코드").lstrip('A'))
            multiple_result['code_name'].append(self.get_comm_data(trcode, rqname, i, "종목명"))
            multiple_result['market_price'].append(int(self.get_comm_data(trcode, rqname, i, "현재가").lstrip('+|-')))
            multiple_result['start_price'].append(int(self.get_comm_data(trcode, rqname, i, "시가").lstrip('+|-')))
            multiple_result['high_price'].append(int(self.get_comm_data(trcode, rqname, i, "고가").lstrip('+|-')))
            multiple_result['contract_strength'].append(self.get_comm_data(trcode, rqname, i, "체결강도"))
            multiple_result['preferred_buy_remaining'].append(int(self.get_comm_data(trcode, rqname, i, "우선매수잔량")))
        self.logger.debug(f"{multiple_result['code']}")
        return multiple_result

    @staticmethod
    def _change_format(data):
        strip_data = data.lstrip('0')
        if strip_data == '':
            strip_data = '0'
        return int(strip_data)

    # 수익률에 대한 포맷 변경은 change_format2라는 정적 메서드를 사용합니다.
    @staticmethod
    def _change_format2(data):
        """
        모의 투자는 mod_gubun = 1
        실전 투자는 mod_gubun = 100
        """
        # mod_gubun = 100
        mod_gubun = 1
        strip_data = data.lstrip('-0')  # 앞에 0 제거
        # 이렇게 추가해야 소수점으로 나온다.
        if strip_data == '':
            strip_data = '0'
        else:
            # 여기서 strip_data가 0이거나 " " 되니까 100 나눴을 때 갑자기 동작안함. 에러도 안뜸 그래서 원래는 if 위에 있었는데 else 아래로 내림
            strip_data = str(float(strip_data) / mod_gubun)
            if strip_data.startswith('.'):
                strip_data = '0' + strip_data
            #     strip 하면 -도 사라지나보네 여기서 else 하면 안된다. 바로 위에 소수 읻네 음수 인 경우가 있기 때문
            if data.startswith('-'):
                strip_data = '-' + strip_data
        return strip_data

    """ 실시간 이벤트 처리 메서드 """

    def _real_type_002(self, code):
        results = []
        for fid in self.real_type_002_df['fid']:
            if fid in [10, 17]:
                results.append(int(self.get_comm_real_data(code, fid).lstrip('-|+')))
            else:
                results.append(self.get_comm_real_data(code, fid))
        self.real_type_002_df[code] = results
        # self.logger.debug(f"실시간 002의 {code}에 {self.real_type_002_dict[code]}로 교체됐습니다.")



    """ 이상 확정 메서드 """

    # 편입된 종목에서 보유한 종목 제외
    # if len(code.split(';')) == 1:
    #     incorporated_list = code.split(';')
    # else:
    #     incorporated_list = code.split(';')[:-1]
    # possession_list = self.possession_list['code']
    # filtered_list = [elem for elem in incorporated_list if elem not in possession_list]
    #
    # self.logger.debug(f"편입된 {len(incorporated_list)}개 종목 중에서 보유하지 않은 종목 {len(filtered_list)} 개가 선정됐습니다.")
    #
    # # 다중 항목 현재가 조회
    # self.comm_kw_rq_data(filtered_list, len(filtered_list))
    # data_type = {'contract_strength': float, 'preferred_buy_remaining': int, 'market_price': int}
    # condition_init_df = pd.DataFrame(self._result_OPTKWFID).astype(data_type)
    #
    # # !@ 매수 우선순위 설정
    # condition_init_df = condition_init_df.sort_values(by='contract_strength', ascending=False).reset_index(
    #     drop=True)
    #
    # # 매수
    # self.operating_buy(condition_init_df, self._opw00001)
