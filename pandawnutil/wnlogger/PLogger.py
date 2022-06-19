import sys
import logging
try:
    from importlib import reload
except Exception:
    pass

rootLog = None

# set logger
def setLogger(tmpLog):
    global rootLog
    rootLog = tmpLog


# return logger
def getPandaLogger(log_file_name=None):
    # use root logger
    global rootLog
    if rootLog == None:
        rootLog = logging.getLogger('')
    # add StreamHandler if no handler
    if rootLog.handlers == [] or log_file_name is None:
        rootLog.setLevel(logging.DEBUG)
        if log_file_name is None:
            console = logging.StreamHandler(sys.stdout)
        else:
            console = logging.FileHandler(log_file_name)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console.setFormatter(formatter)
        rootLog.addHandler(console)
    # return
    return rootLog


# reset logger
def resetLogger():
    logging.shutdown()
    reload(logging)
    setLogger(None)
    return getPandaLogger()
