import logging


class Logger:
    logger = None

    @staticmethod
    def init_log(output=None):
        Logger.logger = logging.getLogger('crypto-trader')
        Logger.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s \n%(message)s\n')
        if output is None:
            slogger = logging.StreamHandler()
            slogger.setFormatter(formatter)
            Logger.logger.addHandler(slogger)
        else:
            flogger = logging.FileHandler(output)
            flogger.setFormatter(formatter)
            Logger.logger.addHandler(flogger)

    @staticmethod
    def info(str):
        Logger.logger.info(str)

    @staticmethod
    def error(str):
        Logger.logger.info(str)
