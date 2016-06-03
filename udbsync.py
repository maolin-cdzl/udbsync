import time
import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler

from udbtype import *
from udb import *
from udbgenerator import *
from redispersist import *

def SetupLogger(path):
    FORMAT = "%(asctime)-15s %(levelname)-8s %(message)s"
    formatter = Formatter(fmt=FORMAT)

    logger = logging.getLogger()
    handler = TimedRotatingFileHandler(path,when="d",interval=1,backupCount=7)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

if __name__ == '__main__':
    SetupLogger('./udb.log')
    udb = GenerateUDB()
    PersistRedis(udb)
