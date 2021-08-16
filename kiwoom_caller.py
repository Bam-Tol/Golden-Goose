from kiwoom_handler import Handler
import pandas as pd
import numpy as np


class Caller(Handler):
    """
    Caller 클래스는 이벤트를 서버에 요청하기 위한 함수를 모아놓은 클래스 입니다.
    """
    def __init__(self):
        super().__init__()

    """ 호출 함수 """
    def login(self):
        self.comm_connect()
        account_number = self.get_login_info()
        return account_number

    # 출금가능금액(계좌 잔액) 호출 함수
    def comm_rq_opw00001(self):
        # 이번에는 예수금 데이터를 얻기 위해 opw00001 TR을 요청하는 코드를 구현해 봅시다. opw00001 TR은 연속적으로 데이터를 요청할 필요가 없으므로 상당히 간단합니다.
        # 비밀번호 입력매체 구분, 조회구분 다 작성해야 된다. 안그러면 0 으로 출력됨
        self.set_input_value("계좌번호", self.account_number)
        self.set_input_value("비밀번호", "")
        self.set_input_value("비밀번호입력매체구분", 00)
        self.set_input_value("조회구분", 3)  # 3: 추정조회, 2: 일반조회
        self.comm_rq_data("opw00001_req", "opw00001", 0, "2000")
        return self._result_opw00001

    # 잔고 및 보유 종목 현황 호출 함수
    def comm_rq_opw00018(self):
        self.set_input_value("계좌번호", self.account_number)
        self.comm_rq_data("opw00018_req", "opw00018", 0, "0391")  # 사용자구분명, tran명, 3째는 0은 조회, 2는 연속, 네번째 2000은 화면 번호
        result = self._result_opw00018

        while self.remained_data:
            # comm_rq_data 호출하기 전에 반드시 set_input_value 해야한다. 초기화 되기 때문
            self.set_input_value("계좌번호", self.account_number)
            self.comm_rq_data("opw00018_req", "opw00018", 2, "0391")
            for key in result['multiple_result'].keys():
                result['multiple_result'][key] += self._result_opw00018['multiple_result'][key]
        return result

    # 조건식 이름 호출 함수
    def call_condition_name_dict(self):
        self.get_condition_load()
        return self.condition_name_dict

    # 조건식 만족하는 종목 dictionary
    def call_send_condition(self, condition_name):
        self.send_condition(condition_name)
        return self.condition_tr_dict

    # 조건 구독 취소
    def call_stop_condition(self, condition_name):
        self.stop_condition(condition_name)
        condition_number = self.condition_name_dict[condition_name]
        screen_no = '0'+str(156+condition_number)
        self.set_real_remove(screen_no, 'ALL')
        del(self.condition_tr_dict[condition_name])

    # 조건식 만족하는 종목의 현재가 조회
    def comm_kw_rq_condition(self, list, screen_no):
        self.comm_kw_rq_data(list, len(list), screen_no)
        # comm_kw_rq_data로 호출하면 실시간에 자동 등록되기 때문에 실시간 종목에서 뺀다
        # self.set_real_remove(screen_no, 'ALL')
        # self.logger.debug(f"{screen_no} 화면의 모든 종목 실시간 시세를 [구독 취소] 합니다")
        return self._result_OPTKWFID

    # 실시간 등록
    def call_set_real_reg(self, screen_no, code_list):
        if len(code_list) > 0:
            code_list = ';'.join(code_list)
            fid_list = '10;11;12'  # (real_type 주식체결)
            option_type = 0  # replace
            self.logger.debug(f"현재 보유중인 {code_list}가 실시간 시세에 등록됩니다")
            self.set_real_reg(screen_no, code_list, fid_list, option_type)

    # 매수 함수
    def operating_buy(self, code_price_df, d2_deposit):
        """ 종목코드와 현재가가 들어있는 df를 input으로 넣으면 매수 """
        self.logger.debug(f"매수를 시작합니다...")

        # 한 종목 살 때 백만원 어치씩
        buy_unit = 1000000

        # 100만원 어치 구매 수량 계산
        code_price_df['구매수량'] = buy_unit // code_price_df['현재가']

        # !@ if len(code_price_df) > 0:
        if len(code_price_df) > 100:
            # 1. for 문으로 한 종목씩 산다. (주머니 사정을 고려)
            for i, elem in code_price_df.iterrows():
                # 2. 주머니 사정 확인 -> 잔고가 100만원 이하면 함수 나가기
                if d2_deposit > buy_unit:
                    # 3. 매수
                    # order_type: 1-신규매수, 2-신규매도, 3-매수취소, 4-매도취소
                    # hoga_gb: "00"-지정가, "03"-시장가
                    self.send_order(order_type=1, jongmok_code=elem['종목코드'], quantity=elem['구매수량'],
                                    price=0, hoga_gb="03", order_no="")
                    self.logger.debug(f"{elem['종목명']} 종목을 {elem['현재가']}원에 {elem['구매수량']}개 매수했습니다.")
                    d2_deposit -= (elem['현재가'] * elem['구매수량'])
                else:
                    self.logger.debug("돈을 다 써서 남은 종목을 매수하지 못했습니다...")
                    break
        else:
            self.logger.debug("매수할 종목이 없습니다...")
        self.logger.debug(f"매수를 완료했습니다...")



    """ 이상 확정 메서드 """

    """ 기존 함수 """
    # 체결 내역 조회
    def call_get_chejan_data(self):
        fid_list = "917;918"
        self.get_chejan_data(fid_list)

    # 종목명 가져오기
    def call_get_code_name(self, code_list):
        code_name_list = []
        for code in code_list:
            code_name_list += [self.get_master_code_name(code)]
        return code_name_list

    # 일자별 실현손익
    def daily_balance(self):
        # 일자별 실현손익 출력
        self.set_input_value("계좌번호", self.account_number)
        self.set_input_value("시작일자", "20170101")
        self.set_input_value("종료일자", self.today)
        self.comm_rq_data("opt10074_req", "opt10074", 0, "0329")
        return self._result_opt10074

    def chegyul_jongmok(self, code):
        self.set_input_value("종목코드", code)
        self.set_input_value("조회구분", 1)  # 조회구분 = 0:전체, 1:종목
        self.set_input_value("계좌번호", self.account_number)
        self.comm_rq_data("opt10076_req", "opt10076", 0, "0350")
        return self._result_opt10076

    # 특정 종목의 일별 데이터
    def get_total_data(self, jongmok_code, jongmok_code_name, start_date, is_exist):
        # 종목 테이블이 존재하면 1개만 가져오고
        self.set_input_value("종목코드", jongmok_code)
        self.set_input_value("기준일자", start_date)
        self.set_input_value("수정주가구분", 1)
        self.comm_rq_data("opt10081_req", "opt10081", 0, "0101")
        result = self._result_opt10081

        # 존재하지 않으면 과거 전체를 가져온다
        if is_exist is False:
            while self.remained_data is True:
                self.set_input_value("종목코드", jongmok_code)
                self.set_input_value("기준일자", start_date)
                self.set_input_value("수정주가구분", 1)
                self.comm_rq_data("opt10081_req", "opt10081", 2, "0101")
                for key in result.keys():
                    result[key] += self._result_opt10081[key]

        result['code'] = [jongmok_code] * len(result['date'])
        result['code_name'] = [jongmok_code_name] * len(result['date'])
        columns = ['code', 'code_name', 'date', 'open', 'high', 'low', 'close', 'volume']
        result_df = pd.DataFrame(result)[columns]
        return result_df

    # 당일 미체결 내역 요청
    def get_non_chegyul_jongmok(self):
        self.set_input_value("계좌번호", self.account_number)
        self.set_input_value("전체종목구분", 0)  # 0: 전체, 1: 종목
        self.set_input_value("매매구분", 0)  # 0: 전체, 1: 매도, 2: 매수
        self.set_input_value("종목코드", "")
        self.set_input_value("체결구분", 1)  # 0: 전체, 1: 미체결, 2: 체결
        self.comm_rq_data("opt10075_req", "opt10075", 0, "0504")
        result = self._result_opt10075

        while self.remained_data:
            # comm_rq_data 호출하기 전에 반드시 set_input_value 해야한다. 초기화 되기 때문
            self.set_input_value("계좌번호", self.account_number)
            self.set_input_value("전체종목구분", 0)  # 0: 전체, 1: 종목
            self.set_input_value("매매구분", 0)  # 0: 전체, 1: 매도, 2: 매수
            self.set_input_value("종목코드", "")
            self.set_input_value("체결구분", 1)  # 0: 전체, 1: 미체결, 2: 체결
            self.comm_rq_data("opt10075_req", "opt10075", 2, "0504")
            for key in result.keys():
                result[key] += self._result_opt10075[key]

        columns = ['order_type', 'code', 'code_name', 'chegyul_amount', 'non_chegyul_amount']
        result_df = pd.DataFrame(result)[columns]
        result_df.columns = ['매도수구분', '종목코드', '종목명', '체결량', '미체결수량']
        # result_df['original_order_code'] = np.where(result_df['original_order_code'].isin(['0000000', '']),
        #                                             result_df['order_code'], result_df['original_order_code'])
        return result_df

    # 당일 체결 내역 요청
    def get_chegyul_jongmok(self):
        self.set_input_value("계좌번호", self.account_number)
        self.set_input_value("전체종목구분", 0)  # 0: 전체, 1: 종목
        self.set_input_value("매매구분", 0)  # 0: 전체, 1: 매도, 2: 매수
        self.set_input_value("종목코드", "")
        self.set_input_value("체결구분", 2)  # 0: 전체, 1: 미체결, 2: 체결
        self.comm_rq_data("opt10075_req", "opt10075", 0, "0504")
        result = self._result_opt10075

        while self.remained_data:
            # comm_rq_data 호출하기 전에 반드시 set_input_value 해야한다. 초기화 되기 때문
            self.set_input_value("계좌번호", self.account_number)
            self.set_input_value("전체종목구분", 0)  # 0: 전체, 1: 종목
            self.set_input_value("매매구분", 0)  # 0: 전체, 1: 매도, 2: 매수
            self.set_input_value("종목코드", "")
            self.set_input_value("체결구분", 2)  # 0: 전체, 1: 미체결, 2: 체결
            self.comm_rq_data("opt10075_req", "opt10075", 2, "0505")
            for key in result.keys():
                result[key] += self._result_opt10075[key]

        result_df = pd.DataFrame(result)
        result_df['date'] = [self.today] * len(result_df)
        result_df['original_order_code'] = np.where(result_df['original_order_code'] == '0000000',
                                                    result_df['order_code'], result_df['original_order_code'])

        columns = ['date', 'order_time', 'order_code', 'original_order_code', 'order_type',
                   'code', 'code_name', 'chegyul_price', 'chegyul_amount', 'non_chegyul_amount']
        return result_df[columns]

    # 미체결 취소를 위한 함수
    def cancel_order(self, buy_or_sell_list):
        non_chegyul_df = self.get_non_chegyul_jongmok()
        cancel_df = non_chegyul_df[non_chegyul_df['order_type'].isin(buy_or_sell_list)]
        if len(cancel_df) > 0:
            for i, elem in cancel_df.iterrows():
                if elem['order_type'] == '매수':
                    # 매수 전량 취소
                    # order_type: 1-신규매수, 2-신규매도, 3-매수취소, 4-매도취소
                    # hoga_gb: "00"-지정가, "03"-시장가
                    self.send_order(order_type=3, jongmok_code=elem['code'],
                                    quantity="", price="", hoga_gb="", order_no=elem['original_order_code'])
                    self.logger.debug(f"{elem['code_name']} 종목을 전량 [매수 취소]했습니다.")
                elif elem['order_type'] == '매도':
                    # 매도 전량 취소
                    # order_type: 1-신규매수, 2-신규매도, 3-매수취소, 4-매도취소
                    # hoga_gb: "00"-지정가, "03"-시장가
                    self.send_order(order_type=4, jongmok_code=elem['code'],
                                    quantity="", price="", hoga_gb="", order_no=elem['original_order_code'])
                    self.logger.debug(f"{elem['code_name']} 종목을 전량 [매도 취소]했습니다.")





# 종목코드 및 이름 반환
# def get_code_df(self, market_code="0"):
#     # 0: 유가증권, 10: 코스닥
#     get_list = self.dynamicCall("GetCodeListByMarket(QString)", [market_code])
#     code_list = get_list.split(';')[:-1]
#     name_list = []
#     for code in code_list:
#         name_list.append(self.dynamicCall("GetMasterCodeName(QString)", [code]))
#     df = pd.DataFrame({'code_name': name_list, 'code': code_list})
#     return df

# 실시간 데이터 요청
# 화면이 없으면 계속 껐다 켜야해...
# def get_realtime_market(self):
#     screen_no = "0345"
#     code_list = "005930"
#     fid_list = "9201;9001;302;8019;10;930;"
#     real_type = 0
#     # OnReceiveRealData 켜놓기 -> 버튼 클릭 -> SetRealReg ->
#     self.dynamicCall("SetRealReg(QString, QString, QString, QString)", screen_no, code_list, fid_list, real_type)
#
#     # SetRealRemove 해주지 않으면 다음 조회시 최신 데이터를 받아올 수 없다.
#     # DisconnectRealData() 랑 똑같음
#     self.dynamicCall("SetRealRemove(QString, QString)", "ALL", "ALL")
