import logging
import pickle
import traceback

from time import sleep
from pulsar import Message
from typing import Dict
from log_utils.log_types import *
from .base_processor import BaseMsgProcessor


class HDFSProcessor(BaseMsgProcessor):
    def process_msg(self, msg: Message):
        pass

    def flush_cache_to_db(self):
        pass

    def say_hello(self):
        pass
