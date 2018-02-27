import logging


class Logger:
    logger = None

    @staticmethod
    def init_log(output=None):
        Logger.logger = logging.getLogger('BitcoinExchangeFH')
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
    def info(method, str):
        Logger.logger.info('[%s]\n%s\n' % (method, str))

    @staticmethod
    def error(method, str):
        Logger.logger.error('[%s]\n%s\n' % (method, str))
