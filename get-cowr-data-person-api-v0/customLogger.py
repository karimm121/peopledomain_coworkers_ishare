
import logging
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

def getlogger():
    # Instantiates a clien
    client = google.cloud.logging.Client()
    handler = CloudLoggingHandler(client, name='module_logger_log')
    module_logger = logging.getLogger('module_logger')
    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(handler)
    module_logger.info('log info: module_logger created')
    # # By default this captures all logs
    # # at INFO level and higher
    client.setup_logging()
    return module_logger
