
from datetime import datetime
from utils.pretty import Pretty

class Logger:
    def __init__(self, message, pretty=False):
        self.message = message

        if pretty:
            self.message = Pretty(message).print()

    def log_time(self):
        dateTimeObj = datetime.now()
        timestampStr = dateTimeObj.strftime('%d-%b-%Y (%H:%M:%S.%f)')
        print(f'\nTIMESTAMP: {self.message}: {timestampStr}')

        return dateTimeObj

    def log_exception(self, error):
        print(f'\nEXCEPTION: {self.message}')
        print(error)

    def log_info(self):
        print(f'\nINFO: {self.message}')
