from enum import Enum
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
