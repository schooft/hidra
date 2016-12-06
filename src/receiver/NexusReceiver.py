from __future__ import print_function

import sys
import argparse
import logging
import os
import json
import subprocess
import re
import errno
try:
    import ConfigParser
except:
    import configparser as ConfigParser


BASE_PATH = os.path.dirname(
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__))))
SHARED_PATH = os.path.join(BASE_PATH, "src", "shared")
API_PATH = os.path.join(BASE_PATH, "src", "APIs")
CONFIG_PATH = os.path.join(BASE_PATH, "conf")

if SHARED_PATH not in sys.path:
    sys.path.append(SHARED_PATH)
del SHARED_PATH
del BASE_PATH

import helpers

try:
    # search in global python modules first
    from hidra import Transfer
except:
    # then search in local modules
    if API_PATH not in sys.path:
        sys.path.append(API_PATH)
    del API_PATH

    from hidra import Transfer

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    defaultConfig = os.path.join(CONFIG_PATH, "nexusReiceiver.conf")

    ##################################
    #   Get command line arguments   #
    ##################################

    parser = argparse.ArgumentParser()

    parser.add_argument("--configFile",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--logfilePath",
                        type=str,
                        help="Path where logfile will be created")
    parser.add_argument("--logfileName",
                        type=str,
                        help="Filename used for logging")
    parser.add_argument("--logfileSize",
                        type=int,
                        help="File size before rollover in B (linux only)")
    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onScreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    parser.add_argument("--whitelist",
                        type=str,
                        help="List of hosts allowed to connect")
    parser.add_argument("--targetDir",
                        type=str,
                        help="Where incoming data will be stored to")
    parser.add_argument("--dataStreamIp",
                        type=str,
                        help="Ip of dataStream-socket to pull new files from")
    parser.add_argument("--dataStreamPort",
                        type=str,
                        help="Port number of dataStream-socket to pull new "
                             "files from")

    arguments = parser.parse_args()
    arguments.configFile = arguments.configFile \
        or defaultConfig

    # check if configFile exist
    helpers.check_file_existance(arguments.configFile)

    ##################################
    # Get arguments from config file #
    ##################################

    config = ConfigParser.RawConfigParser()
    config.readfp(helpers.FakeSecHead(open(arguments.configFile)))

    arguments.logfilePath = arguments.logfilePath \
        or config.get('asection', 'logfilePath')
    arguments.logfileName = arguments.logfileName \
        or config.get('asection', 'logfileName')

    if not helpers.is_windows():
        arguments.logfileSize = arguments.logfileSize \
            or config.get('asection', 'logfileSize')

    try:
        arguments.whitelist = arguments.whitelist \
            or json.loads(config.get('asection', 'whitelist'))
    except ValueError:
        ldap_cn = config.get('asection', 'whitelist')
        p = subprocess.Popen(
            ["ldapsearch",
             "-x",
             "-H ldap://it-ldap-slave.desy.de:1389",
             "cn=" + ldap_cn, "-LLL"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        lines = p.stdout.readlines()

        matchHost = re.compile(r'nisNetgroupTriple: [(]([\w|\S|.]+),.*,[)]',
                               re.M | re.I)
        arguments.whitelist = []

        for line in lines:

            if matchHost.match(line):
                if matchHost.match(line).group(1) not in arguments.whitelist:
                    arguments.whitelist.append(matchHost.match(line).group(1))
    except:
        arguments.whitelist = json.loads(
            config.get('asection', 'whitelist').replace("'", '"'))

    arguments.targetDir = arguments.targetDir \
        or config.get('asection', 'targetDir')

    arguments.dataStreamIp = arguments.dataStreamIp \
        or config.get('asection', 'dataStreamIp')
    arguments.dataStreamPort = arguments.dataStreamPort \
        or config.get('asection', 'dataStreamPort')

    ##################################
    #     Check given arguments      #
    ##################################

    logfile = os.path.join(arguments.logfilePath, arguments.logfileName)

    #enable logging
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handlers = helpers.get_log_handlers(logfile, arguments.logfileSize,
                                        arguments.verbose, arguments.onScreen)

    if type(handlers) == tuple:
        for h in handlers:
            root.addHandler(h)
    else:
        root.addHandler(handlers)

    # check target directory for existance
    helpers.check_dir_existance(arguments.targetDir)

    # check if logfile is writable
    helpers.check_log_file_writable(arguments.logfilePath,
                                    arguments.logfileName)

    return arguments


class NexusReceiver:
    def __init__(self):
        self.transfer = None

        try:
            arguments = argument_parsing()
        except:
            self.log = self.get_logger()
            raise

        self.log = self.get_logger()

        self.whitelist = arguments.whitelist

        self.log.info("Configured whitelist: {0}".format(self.whitelist))

        self.targetDir = os.path.normpath(arguments.targetDir)
        self.dataIp = arguments.dataStreamIp
        self.dataPort = arguments.dataStreamPort

        self.log.info("Writing to directory '{0}'".format(self.targetDir))

        self.transfer = Transfer("nexus", useLog=True)

        try:
            self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    def get_logger(self):
        logger = logging.getLogger("NexusReceiver")
        return logger

    def openCallback(self, params, filename):
        # TODO
        try:
            BASE_PATH = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.realpath(__file__))))
        except:
            BASE_PATH = os.path.dirname(
                os.path.dirname(
                    os.path.dirname(
                        os.path.abspath(sys.argv[0]))))
        print (BASE_PATH)

        targetFile = os.path.join(
            BASE_PATH, "data", "target", "local", filename)

        try:
            params["target_fp"] = open(targetFile, "wb")
        except IOError as e:
            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                try:
                    targetPath = os.path.split(targetFile)[0]
                    print ("targetPath", targetPath)
                    os.makedirs(targetPath)

                    params["target_fp"] = open(targetFile, "wb")
                    print ("New target directory created:", targetPath)
                except:
                    raise
            else:
                    raise
        print (params, filename)

    def readCallback(self, params, receivedData):
        metadata = receivedData[0]
        data = receivedData[1]
        print (params, metadata)

        params["target_fp"].write(data)

    def closeCallback(self, params, data):
        print (params, data)
        params["target_fp"].close()

    def run(self):
        callbackParams = {"target_fp": None}

        try:
            self.transfer.start([self.dataIp, self.dataPort], self.whitelist)
#            self.transfer.start(self.dataPort)
        except:
            self.log.error("Could not initiate stream", exc_info=True)
            raise

        #run loop, and wait for incoming messages
        while True:
            try:
                data = self.transfer.read(callbackParams,
                                          self.openCallback,
                                          self.readCallback,
                                          self.closeCallback)
                logging.debug("Retrieved: " + str(data)[:100])

#                if data == "CLOSE_FILE":
#                    break
            except KeyboardInterrupt:
                break
            except:
                self.log.error("Could not read")
                raise

    def stop(self):
        if self.transfer:
            self.log.info("Shutting down receiver...")
            self.transfer.stop()
            self.transfer = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == "__main__":
    #start file receiver
    receiver = NexusReceiver()
