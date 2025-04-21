from multiprocessing import Process
import logging
import common.config_init
from utils.utils import config_logger
from common.filter import Filter


class Controller:
    def __init__(self):
        self.all_filters = []
        self.config = common.config_init.initialize_config()

    def start(self):
        pass

0