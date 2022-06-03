import configparser
import logging
import os
import sys
from datetime import datetime


class KiapsLogging:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)

        self.__download_logger = logging.getLogger("DOWNLOAD")
        self.__upload_logger = logging.getLogger("UPLOAD")
        self.__file_handler = None

    # Initialization the handler for logger
    def init_file_handler(self, path, filename):
        if self.__file_handler is not None:
            self.__download_logger.removeHandler(self.__file_handler)
            self.__upload_logger.removeHandler(self.__file_handler)

        # File handler
        self.__file_handler = logging.FileHandler(filename="{}/{}.log".format(path, filename))
        self.__file_handler.setFormatter(logging.Formatter(
            fmt="[%(levelname)s] [%(name)s] [%(asctime)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"))

        self.__download_logger.addHandler(self.__file_handler)
        self.__upload_logger.addHandler(self.__file_handler)

    def download_info(self, msg):
        self.__download_logger.info(msg)

    def upload_info(self, msg):
        self.__upload_logger.info(msg)

    def download_warning(self, msg):
        self.__download_logger.warning(msg)

    def upload_warning(self, msg):
        self.__upload_logger.warning(msg)

    def download_error(self, msg):
        self.__download_logger.error(msg)

    def upload_error(self, msg):
        self.__upload_logger.error(msg)

    @classmethod
    def log_decorator(cls, function):
        def wrapper(*args, **kwargs):
            print('top')

            function(*args, **kwargs)

            fn = getattr(sys.modules['__main__'], '__file__')
            root_path = os.path.abspath(os.path.dirname(fn))
            config = configparser.ConfigParser(interpolation=configparser.BasicInterpolation())
            config.read(root_path + "/configs/kiaps_config.ini", encoding="UTF-8")

            KiapsLogging().upload_info("Successfully uploaded the file {}".format(args[1] + "_" + config.get("GLOBAL", "FILE_DATE") + ".txt"))
            with open(config.get("DIRECTORY", "LOG_PATH") % (datetime.now().strftime("%Y%m%d")), "a") as writer:
                writer.write("[INFO] [UPLOAD] [{}] Successfully uploaded the file {}\n".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"), args[1] + "_" + config.get("GLOBAL", "FILE_DATE") + ".txt"))

        return wrapper
