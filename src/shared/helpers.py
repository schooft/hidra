import os
import platform
import logging
import logging.handlers
import sys
import shutil
import zmq
import socket
from version import __version__

try:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.realpath ( __file__ ) )))
except:
    BASE_PATH = os.path.dirname ( os.path.dirname ( os.path.dirname ( os.path.abspath ( sys.argv[0] ) )))
SHARED_PATH = os.path.join(BASE_PATH,"src","shared")

if not SHARED_PATH in sys.path:
    sys.path.append ( SHARED_PATH )
del SHARED_PATH

from logutils.queue import QueueHandler, QueueListener


def isWindows():
    returnValue = False
    windowsName = "Windows"
    platformName = platform.system()

    if platformName == windowsName:
        returnValue = True
    # osRelease = platform.release()
    # supportedWindowsReleases = ["7"]
    # if osRelease in supportedWindowsReleases:
    #     returnValue = True

    return returnValue


def isLinux():
    returnValue = False
    linuxName = "Linux"
    platformName = platform.system()

    if platformName == linuxName:
        returnValue = True

    return returnValue


class globalObjects (object):
    controlFlag   = True


# This function is needed because configParser always needs a section name
# the used config file consists of key-value pairs only
# source: http://stackoverflow.com/questions/2819696/parsing-properties-file-in-python/2819788#2819788
class FakeSecHead (object):
    def __init__ (self, fp):
        self.fp = fp
        self.sechead = '[asection]\n'

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.fp.readline()


# http://code.activestate.com/recipes/541096-prompt-the-user-for-confirmation/
def confirm (prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.

    'resp' should be set to the default value assumed by the caller when
    user simply types ENTER.

    >>> confirm(prompt='Create Directory?', resp=True)
    Create Directory? [y]|n:
    True
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y:
    False
    >>> confirm(prompt='Create Directory?', resp=False)
    Create Directory? [n]|y: y
    True

    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        try:
            ans = raw_input(prompt)
        except KeyboardInterrupt:
            logging.error("Keyboard Interruption detected.")
        except Exception as e:
            logging.error("Something went wrong with the confirmation.")
            logging.debug("Error was: {err}".format(err=e))
            break

        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            logging.error("please enter y or n.")
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


def checkEventDetectorType (eDType, supportedTypes):

    eDType = eDType.lower()

    if eDType in supportedTypes:
        logging.debug("Event detector '{t}' is ok.".format(t=eDType))
    else:
        logging.error("Event detector '{t}' is not supported.".format(t=eDType))
        sys.exit(1)



def checkDirEmpty (dirPath):

    #check if directory is empty
    if os.listdir(dirPath):
        logging.debug("Directory '%s' is not empty." % str(dirPath))
        if confirm(prompt="Directory {p} is not empty.\nShould its content be removed?".format(d=dirPath),
                   resp = True):
            for element in os.listdir(dirPath):
                path = os.path.join(dirPath, element)
                if os.path.isdir(path):
                   try:
                        os.rmdir(path)
                   except OSError:
                        shutil.rmtree(path)
                else:
                    os.remove(path)
            logging.info("All elements of directory {p} were removed.".format(p=dirPath))


def checkAnySubDirExists (dirPath, subDirs):

    dirPath = os.path.normpath(dirPath)
    dirsToCheck = [os.path.join(dirPath, directory) for directory in subDirs]
    noSubdir = True

    for d in dirsToCheck:
        #check directory path for existance. exits if it does not exist
        if os.path.exists(d):
            noSubdir = False

    if noSubdir:
        logging.error("There are none of the specified subdirectories inside '%s'. Abort." % str(dirPath))
        logging.error("Checked paths: {d}".format(d=dirsToCheck))
        sys.exit(1)


def checkAllSubDirExist (dirPath, subDirs):

    dirPath = os.path.normpath(dirPath)
    dirsToCheck = [os.path.join(dirPath, directory) for directory in subDirs]

    for d in dirsToCheck:
        if not os.path.exists(d):
            logging.warning("Dir '%s' does not exist." % str(d))


def checkFileExistance (filePath):
    # Check file for existance.
    # Exits if it does not exist

    if not os.path.exists(filePath):
        logging.error("File '%s' does not exist. Abort." % str(filePath))
        sys.exit(1)


def checkDirExistance (dirPath):
    # Check directory path for existance.
    # Exits if it does not exist

    if not os.path.exists(dirPath):
        logging.error("Dir '%s' does not exist. Abort." % str(dirPath))
        sys.exit(1)


def checkLogFileWritable (filepath, filename):
    # Exits if logfile cannot be written
    try:
        logfullPath = os.path.join(filepath, filename)
        logFile = open(logfullPath, "a")
        logFile.close()
    except:
        logging.error("Unable to create the logfile {p}".format(p=logfullPath))
        logging.error("Please specify a new target by setting the following arguments:\n--logfileName\n--logfilePath")
        sys.exit(1)


def checkVersion (version, log):
    log.debug("remote version: {v}, local version: {v2}".format(v=version, v2=__version__))
    if version.rsplit(".", 1)[0] < __version__.rsplit(".", 1)[0]:
        log.info("Version of receiver is lower. Please update receiver.")
        return False
    elif version.rsplit(".", 1)[0] > __version__.rsplit(".", 1)[0]:
        log.info("Version of receiver is higher. Please update sender.")
        return False
    else:
        return True


def checkHost (hostname, whiteList, log):

    if hostname and whiteList:

        if type(hostname) == list:
            temp = True
            for host in hostname:
                if host.endswith(".desy.de"):
                    hostModified = host[:-8]
                else:
                    hostModified = host

                if host not in whiteList and hostModified not in whiteList:
                    log.info("Host {h} is not allowed to connect".format(h=host))
                    temp = False

            return temp


        else:
            if hostname.endswith(".desy.de"):
                hostnameModified = hostname[:-8]
            else:
                hostnameModified = hostname

            if hostname in whiteList or hostnameModified in whiteList:
                return True

    return False


def checkPing(host, log = logging):
    if isWindows():
        response = os.system("ping -n 1 -w 2 {h}".format(h=host))
    else:
        response = os.system("ping -c 1 -w 2 {h} > /dev/null 2>&1".format(h=host))

    if response != 0:
        log.error("{h} is not pingable.".format(h=host))
        sys.exit(1)


# IP and DNS name should be both in the whitelist
def extendWhitelist(whitelist, log):
    log.info("Configured whitelist: {w}".format(w=whitelist))
    extendedWhitelist = []

    for host in whitelist:

        if host == "localhost":
            elementToAdd = socket.gethostbyname(host)
        else:
            try:
                hostname, tmp, ip = socket.gethostbyaddr(host)

                if hostname.endswith(".desy.de"):
                    hostModified = hostname[:-8]
                else:
                    hostModified = hostname


                if hostModified not in whitelist:
                    extendedWhitelist.append(hostModified)

                if ip[0] not in whitelist:
                    extendedWhitelist.append(ip[0])
            except:
                pass

    for host in extendedWhitelist:
        whitelist.append(host)

    log.debug("Extended whitelist: {w}".format(w=whitelist))


#class forwarderThread(threading.Thread):
#    def __init__ (self, controlPubConId, controlSubConId, context):
#
#        threading.Thread.__init__(self)
#
#        self.controlPubConId = controlPubConId
#        self.controlSubConId = controlSubConId
#        self.context         = context
#
#        self.frontend        = None
#        self.backend         = None
#
#
#    def run (self):
#        # initiate XPUB/XSUB for control signals
#
#        # Socket facing clients
#        self.frontend = context.socket(zmq.SUB)
#        self.frontend.bind(controlPubConId)
#        logging.info("=== [forwarder] frontend bind to: '{id}'".format(id=controlPubConId))
#
#        self.frontend.setsockopt(zmq.SUBSCRIBE, "")
#
#        # Socket facing services
#        self.backend = context.socket(zmq.PUB)
#        self.backend.bind(controlSubConId)
#        logging.info("=== [forwarder] backend bind to: '{id}'".format(id=controlSubConId))
#
#        zmq.device(zmq.FORWARDER, self.frontend, self.backend)
#        logging.info("=== [forwarder] forwarder initiated")
#
#
#    def stop (self):
#        if self.frontend:
#            logging.info("=== [forwarder] close frontend")
#            self.frontend.close()
#            self.frontend = None
#        if self.backend:
#            logging.info("=== [forwarder] close backend")
#            self.backend.close()
#            self.backend = None
#
#
#    def __exit (self):
#        self.stop()
#
#
#    def __del__ (self):
#        self.stop()


# http://stackoverflow.com/questions/25585518/python-logging-logutils-with-queuehandler-and-queuelistener#25594270
class CustomQueueListener (QueueListener):
    def __init__ (self, queue, *handlers):
        super(CustomQueueListener, self).__init__(queue, *handlers)
        """
        Initialise an instance with the specified queue and
        handlers.
        """
        # Changing this to a list from tuple in the parent class
        self.handlers = list(handlers)

    def handle (self, record):
        """
        Override handle a record.

        This just loops through the handlers offering them the record
        to handle.

        :param record: The record to handle.
        """
        record = self.prepare(record)
        for handler in self.handlers:
            if record.levelno >= handler.level: # This check is not in the parent class
                handler.handle(record)

    def addHandler (self, hdlr):
        """
        Add the specified handler to this logger.
        """
        if not (hdlr in self.handlers):
            self.handlers.append(hdlr)

    def removeHandler (self, hdlr):
        """
        Remove the specified handler from this logger.
        """
        if hdlr in self.handlers:
            hdlr.close()
            self.handlers.remove(hdlr)


# Get the log Configuration for the lisener
def getLogHandlers (logfile, logsize, verbose, onScreenLogLevel = False):
    # Enable more detailed logging if verbose-option has been set
    logLevel = logging.INFO
    if verbose:
        logLevel = logging.DEBUG

    # Set format
    datef='%Y-%m-%d %H:%M:%S'
    f = '[%(asctime)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s'

    # Setup file handler to output to file
    # argument for RotatingFileHandler: filename, mode, maxBytes, backupCount)
    # 1048576 = 1MB
    if isWindows():
        h1 = logging.FileHandler(logfile, 'a')
    else:
        h1 = logging.handlers.RotatingFileHandler(logfile, 'a', logsize, 5)
    f1 = logging.Formatter(datefmt=datef,fmt=f)
    h1.setFormatter(f1)
    h1.setLevel(logLevel)

    # Setup stream handler to output to console
    if onScreenLogLevel:
        onScreenLogLevelLower = onScreenLogLevel.lower()
        if onScreenLogLevelLower in ["debug", "info", "warning", "error", "critical"]:

            f  = "[%(asctime)s] > %(message)s"

            if onScreenLogLevelLower == "debug":
                screenLogLevel = logging.DEBUG
                f = "[%(asctime)s] > [%(filename)s:%(lineno)d] %(message)s"

                if not verbose:
                    logging.error("Logging on Screen: Option DEBUG in only active when using verbose option as well (Fallback to INFO).")
            elif onScreenLogLevelLower == "info":
                screenLogLevel = logging.INFO
            elif onScreenLogLevelLower == "warning":
                screenLogLevel = logging.WARNING
            elif onScreenLogLevelLower == "error":
                screenLogLevel = logging.ERROR
            elif onScreenLogLevelLower == "critical":
                screenLogLevel = logging.CRITICAL

            h2 = logging.StreamHandler()
            f2 = logging.Formatter(datefmt=datef, fmt=f)
            h2.setFormatter(f2)
            h2.setLevel(screenLogLevel)

            return h1, h2
        else:
            logging.error("Logging on Screen: Option {l} is not supported.".format(l=onScreenLogLevel))
            exit(1)

    else:
        return h1


def initLogging (filenameFullPath, verbose, onScreenLogLevel = False):
    #@see https://docs.python.org/2/howto/logging-cookbook.html

    #more detailed logging if verbose-option has been set
    loggingLevel = logging.INFO
    if verbose:
        loggingLevel = logging.DEBUG

    #log everything to file
    logging.basicConfig(level=loggingLevel,
#                        format='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s:%(lineno)d] [%(name)s] [%(levelname)s] %(message)s',
                        format='%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d_%H:%M:%S',
                        filename=filenameFullPath,
                        filemode="a")

#        fileHandler = logging.FileHandler(filename=filenameFullPath,
#                                          mode="a")
#        fileHandlerFormat = logging.Formatter(datefmt='%Y-%m-%d_%H:%M:%S',
#                                              fmt='[%(asctime)s] [PID %(process)d] [%(filename)s] [%(module)s:%(funcName)s] [%(name)s] [%(levelname)s] %(message)s')
#        fileHandler.setFormatter(fileHandlerFormat)
#        fileHandler.setLevel(loggingLevel)
#        logging.getLogger("").addHandler(fileHandler)

    #log info to stdout, display messages with different format than the file output
    if onScreenLogLevel:
        onScreenLogLevelLower = onScreenLogLevel.lower()
        if onScreenLogLevelLower in ["debug", "info", "warning", "error", "critical"]:

            console = logging.StreamHandler()
            screenHandlerFormat = logging.Formatter(datefmt = "%Y-%m-%d_%H:%M:%S",
                                                    fmt     = "[%(asctime)s] > %(message)s")

            if onScreenLogLevelLower == "debug":
                screenLoggingLevel = logging.DEBUG
                console.setLevel(screenLoggingLevel)

                screenHandlerFormat = logging.Formatter(datefmt = "%Y-%m-%d_%H:%M:%S",
                                                        fmt     = "[%(asctime)s] > [%(filename)s:%(lineno)d] %(message)s")

                if not verbose:
                    logging.error("Logging on Screen: Option DEBUG in only active when using verbose option as well (Fallback to INFO).")
            elif onScreenLogLevelLower == "info":
                screenLoggingLevel = logging.INFO
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevelLower == "warning":
                screenLoggingLevel = logging.WARNING
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevelLower == "error":
                screenLoggingLevel = logging.ERROR
                console.setLevel(screenLoggingLevel)
            elif onScreenLogLevelLower == "critical":
                screenLoggingLevel = logging.CRITICAL
                console.setLevel(screenLoggingLevel)

            console.setFormatter(screenHandlerFormat)
            logging.getLogger("").addHandler(console)
        else:
            logging.error("Logging on Screen: Option {l} is not supported.".format(onScreenLogLevel))


