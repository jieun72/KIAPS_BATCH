import configparser
import os
import sys
from abc import *


class MainBase(metaclass=ABCMeta):
    def __init__(self):
        fn = getattr(sys.modules['__main__'], '__file__')
        root_path = os.path.abspath(os.path.dirname(fn))
        self.config = configparser.ConfigParser(interpolation=configparser.BasicInterpolation())
        self.config.read(root_path + "/configs/kiaps_config.ini", encoding="UTF-8")
        if len(sys.argv) > 1:
            print(sys.argv[1])
            self.config.set("GLOBAL", "FILE_DATE", sys.argv[1])
        self._init()

    @abstractmethod
    def _init(self):
        pass
