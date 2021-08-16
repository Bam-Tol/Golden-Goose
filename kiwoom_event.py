from kiwoom_caller import Caller
import pandas as pd
import copy
import numpy as np
import time


class KiwoomEvent(Caller):
    """
    KiwoomEvent 클래스는 window_setting 에서 호출하는 함수를 모아놓은 클래스
    : Caller를 이용해서 호출하고
    : Handler를 이용해서 이벤트를 처리한다
    """
    def __init__(self, logger_):
        super().__init__()
        self.logger = logger_
        self.account_number = self.login()
        self.d2_deposit = None
        self.possession_df = pd.DataFrame()
        self.condition_subscribed_df = pd.DataFrame(columns=['조건식'])

    """ 
    ####################
    [0345] 화면 구성 메서드
    #################### 
    """
    # 한 번만 요청하는 것으로 가정하고 만들자
    def real_time_registration(self):
        # self.logger.debug("실시간 잔고 구독")
        # 1. 보유 종목 list-up (10초에 한 번 조회 해보자)
        self.possession_df = pd.DataFrame(self.comm_rq_opw00018()['multiple_result'])

        # 2. 실시간 조회 요청 (replace)
        self.call_set_real_reg("0345", self.possession_df['종목코드'])

        # 3. 10초에 한 번 (d+2 출금가능 금액 조회) -> 실시간 추정자산 계산 시 사용
        self.d2_deposit = self.comm_rq_opw00001()

    # 실시간 수익률 계산(0.5초 또는 1초에 한 번)
    def real_time_profit_ratio(self):
        # 1. 현재 보유 종목 list-up
        possession_df = copy.deepcopy(self.possession_df)

        # 2. 실시간 업데이트 되는 df에서 현재가만 뽑아온다
        real_time_df = copy.deepcopy(self.real_type_002_df)
        map_dict = real_time_df[real_time_df['fid_encode'] == '현재가'].to_dict('records')[0]
        map_price = possession_df['종목코드'].map(map_dict)
        possession_df['현재가'] = np.where(map_price.isnull(), possession_df['현재가'], map_price)

        # 세금 제외 list
        etf_list = ['251340']

        # 계산
        possession_df['평가금액'] = possession_df['현재가'] * possession_df['보유량']
        # possession_df['수수료(추정)'] = ((possession_df['매입수수료'] + possession_df['평가금액'] * 0.00015) // 10 * 10).astype(np.int64)  # 0.015% (실전)
        possession_df['수수료(추정)'] = ((possession_df['매입수수료'] + possession_df['평가금액'] * 0.0035) // 10 * 10).astype(np.int64)  # 0.35% (모의)
        possession_df['세금(추정)'] = np.where(possession_df['종목코드'].isin(etf_list), 0,
                                           (possession_df['평가금액'] * 0.0023).astype(np.int64))  # 0.23%
        possession_df['손익분기(추정)'] = ((possession_df['매수금액'] + possession_df['수수료(추정)'] + possession_df['세금(추정)']) / possession_df['보유량']).astype(np.int64)
        possession_df['평가손익(추정)'] = (possession_df['현재가'] - possession_df['매입가']) * possession_df['보유량'] - possession_df['수수료(추정)'] - possession_df['세금(추정)']
        possession_df['수익률(추정)'] = np.round((possession_df['평가손익(추정)'] / possession_df['매수금액'] * 100).astype(np.float64), 2)

        # 합계
        summary_df = pd.DataFrame()
        summary_df['출금가능금액(d+2)'] = [self.d2_deposit]
        summary_df['총매입'] = [possession_df['매수금액'].sum()]
        summary_df['총평가'] = [possession_df['평가금액'].sum()]
        summary_df['총손익'] = [possession_df['평가손익(추정)'].sum()]

        summary_df['총수익률'] = np.round(summary_df['총손익'] / summary_df['총매입'] * 100, 2)
        summary_df['추정자산'] = summary_df['출금가능금액(d+2)'] + summary_df['총매입'] + summary_df['총손익']

        self.possession_df = possession_df
        return possession_df, summary_df

    """ 
    ####################
    [0156] 화면 구성 메서드
    ####################
    """
    # 조건식 목록 불러오기
    def get_condition_name(self):
        return self.call_condition_name_dict()

    # 조건식 만족하는 종목 dictionary
    def get_condition_tr_dict(self, condition_name):
        """
        :param condition_name: 조건식 이름
        :return : 조건식을 만족하는 종목의 dictionary
        """
        return self.call_send_condition(condition_name)

    def init_condition_tr_dict(self, tr_dict):
        condition_tr_dict = copy.deepcopy(tr_dict)
        # condition_tr_dict = {'거래량': ['001067', '006570', '032685', '036630',
        #                              '037070', '051980', '053030', '095500',
        #                              '179290', '191410', '192250', '219130', '371130'],
        #                      '성장주': ['051980', '191410', '005930'],
        #                      '쓰리 아웃사이드 다운': ['']}

        # dictionary 변환
        condition_name_list = []
        code_list = []

        # 구독 신청
        result = {}
        for condition_name in condition_tr_dict.keys():
            condition_code_list = condition_tr_dict[condition_name]
            condition_code_list = [elem for elem in condition_code_list if len(elem) == 6]
            condition_number = self.condition_name_dict[condition_name]
            screen_number = '0'+str(156+condition_number)

            code_list += condition_code_list
            condition_name_list += [condition_name] * len(condition_code_list)

            tmp = self.comm_kw_rq_condition(condition_code_list, screen_number)
            if len(result) == 0:
                result = tmp
                continue
            else:
                for key in tmp.keys():
                    result[key] += tmp[key]

        df = pd.DataFrame()
        df['조건식'] = condition_name_list
        df['종목코드'] = code_list
        df['종목명'] = result['code_name']
        df['현재가'] = result['market_price']
        df['시가'] = result['start_price']
        df['고가'] = result['high_price']
        df['시가 대비 등락률'] = list(np.round((df['현재가'] - df['시가']) / df['시가'] * 100, 2))
        df['고가 대비 현재가'] = list(np.round(df['현재가'] / df['고가'] * 100, 2))

        self.condition_subscribed_df = df
        return df

    # 조건식 구독 취소
    def stop_condition_tr_dict(self, condition_name, subscribed_df):
        self.call_stop_condition(condition_name)
        df = copy.deepcopy(subscribed_df)
        df = df[df['조건식'] != condition_name]
        self.condition_subscribed_df = df
        return df

    # 현재가, 시가대비 등락율 가져오기
    def update_real_time_price(self, subscribed_df):
        df = copy.deepcopy(subscribed_df)

        # 1. 실시간 업데이트 되는 dictionary에서 현재가만 뽑아온다
        real_time_df = copy.deepcopy(self.real_type_002_df)

        # 2. 실시간 업데이트 되는 df에서 현재가, 고가만 뽑아온다
        for elem in ['현재가', '고가']:
            map_dict = real_time_df[real_time_df['fid_encode'] == elem].to_dict('records')[0]
            map_price = df['종목코드'].map(map_dict)
            df[elem] = np.where(map_price.isnull(), df[elem], map_price)

        # 3. 파생 변수 계산
        df['시가 대비 등락률'] = np.round(((df['현재가'] - df['시가']) / df['시가'] * 100).astype(np.float64), 2)
        df['고가 대비 현재가'] = np.round((df['현재가'] / df['고가'] * 100).astype(np.float64), 2)
        self.condition_subscribed_df = df
        return df

    """ 
    ####################
    [4989] 화면 구성 메서드
    ####################
    """
    # 매수할 목록 불러오기
    def buy_available_list(self):
        condition_df = copy.deepcopy(self.condition_subscribed_df)
        possession_df = copy.deepcopy(self.possession_df)
        if len(condition_df) > 0:
            filtered_df = condition_df[~condition_df['종목코드'].isin(possession_df['종목코드'])]
            return filtered_df
        else:
            return condition_df

    # 조건식 매수
    def buy_condition_list(self, buy_df):
        if len(buy_df) > 0:
            self.operating_buy(buy_df, self.d2_deposit)

    # 미체결 내역 불러오기
    def get_non_traded_df(self, non_traded_df):
        """
        columns = ['계좌번호', '주문번호', '관리자사번', '종목코드', '주문업무분류', '주문상태',
                   '종목명', '주문수량', '주문가격', '미체결수량', '체결누계금액', '원주문번호',
                   '주문구분', '매매구분', '매도수구분', '주문/체결시간', '체결번호', '체결가', '체결량',
                   '현재가', '(최우선)매도호가', '(최우선)매수호가', '단위체결가', '단위체결량',
                   '당일매매수수료', '당일매매세금', '거부사유', '화면번호', '터미널번호', '신용구분', '대출일']
        """
        tmp = copy.deepcopy(self.chejan_df)
        tmp = tmp[['매도수구분', '종목코드', '종목명', '체결량', '미체결수량']]
        tmp = pd.concat([non_traded_df, tmp]).reset_index(drop=True).sort_values(by=['미체결수량'], ascending=False)
        tmp = tmp.drop_duplicates(subset=['매도수구분', '종목코드'], keep='last')
        # tmp = tmp.groupby(['매도수구분', '종목코드']).min(['미체결수량']).reset_index()
        non_traded_df = tmp[~(tmp['미체결수량'] == 0)]
        return non_traded_df

    """ #### 이상 확정 메서드 #### """

    # 보유하지 않은 종목 매수
    def buy_not_possession(self):
        """
        실시간으로 최신화 돼야 하는 변수들
        1. self.condition_tr_dict
        2. self.possession_list
        3. self.
        4. 종목의 현재가
        """
        # 조건 만족하는 모든 종목을 list로 변환
        tmp = copy.deepcopy(self.condition_tr_dict)
        possession_code_list = copy.deepcopy(self.possession_list['code'])

        condition_code_list = []
        for key in tmp.keys():
            condition_code_list += tmp[key]

        # 만족하는 조건 개수에 따라 sorting
        priority_dict = {}
        for elem in condition_code_list:
            priority_dict[elem] = condition_code_list.count(elem)
        sorted_condition_code_list = [elem[0] for elem in sorted(priority_dict.items(), key=lambda item: item[1])[::-1]]

        # priority_dict 중에서 보유하지 않은 종목 골라내기
        filtered_condition_list = [elem for elem in sorted_condition_code_list if elem not in possession_code_list]

        # 살 종목이 0보다 크면 보유하지 않은 종목 매수하기
        if len(filtered_condition_list) > 100:
            self.logger.debug("살 종목이 있습니다. 매수할래요")
            self.call_send_order(filtered_condition_list)

    # def get_condition_list(self):
    #     # 조건식 만족하는 종목 리스트
    #     satisfied_list = condition_tr_dict[condition_name]['code_list'].split(';')[:-1]
    #
    #     if len(satisfied_list) > 0:
    #         # 현재 보유중인 list
    #         possession_list = self.possession_list['code']
    #         filtered_list = [elem for elem in satisfied_list if elem not in possession_list]
    #         self.logger.debug(f"조건을 만족하는 {len(satisfied_list)}개 종목 중에서 보유하지 않은 종목 {len(filtered_list)} 개가 선정됐습니다.")
    #
    #         # 다중 항목 현재가 조회
    #         self.comm_kw_rq_condition(filtered_list, len(filtered_list))
    #         data_type = {'contract_strength': float, 'preferred_buy_remaining': int, 'market_price': int}
    #         condition_init_df = pd.DataFrame(self._result_OPTKWFID).astype(data_type)
    #         return condition_init_df
    #
    #     else:
    #         self.logger.debug(f"{self.condition_tr_dict[condition_name]['condition_name']} 조건에 만족하는 종목이 없습니다...")
    #         return None
    #
    # # 조건식에 만족하는 종목 매수
    # def buy_condition_df(self, condition_init_df):
    #     d2_deposit = self.comm_rq_opw00001()
    #     self.operating_buy(condition_init_df, d2_deposit)







