from enum import Enum
from datetime import datetime
import os


class LogType(Enum):
    UNKNOWN = -1
    DEBUG = 0
    INFO = 1
    WARNING = 2
    ERROR = 3


class Logger:
    def __init__(self):
        print("[INFO] [logger.py] Logger initialized!")

    def log(self, type: LogType, location: str, message: str):
        print(f'[{type.name}] [{os.path.basename(location)}] {message}')

    def logt(self, type: LogType, location: str, message: str):
        """logt logs with timestamp - possibly important when doing cocurrency

        Reason for keeping as seperate funciton is so the normal log stays fast
        for when benchmark times are important.
        """
        print(f'[{type.name}] [{datetime.now()}] [{os.path.basename(location)}] {message}')
