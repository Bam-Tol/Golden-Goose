ver = "### version 0.1.0 ###"
print(f"logger Version: {ver}")

import logging.handlers


def define_logger():
    # Logger 안에 Handler 가 있음 1개의 logger에 여러개의 handler 추가 가능
    logger = logging.getLogger("crumbs")  # crumbs라는 이름의 logger 정의
    logger.setLevel(logging.DEBUG)  # 로깅 레벨은 디버깅 레벨 이상

    # 핸들러정의
    stream_handler = logging.StreamHandler()  # StreamHandler는 console에, FileHandler는 file에 로깅
    formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s')
    stream_handler.setFormatter(formatter)

    # Logger에 Handler 추가하기
    logger.addHandler(stream_handler)
    return logger
