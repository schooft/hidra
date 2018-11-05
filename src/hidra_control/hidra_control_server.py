#!/usr/bin/env python

# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This server configures and starts up hidra.
"""

# pylint: disable=broad-except

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import argparse
import copy
import glob
import json
import os
import sys
import socket
import subprocess
import time
from multiprocessing import Queue
# import tempfile
import zmq

import setproctitle

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
SHARED_DIR = os.path.join(BASE_DIR, "src", "shared")
CONFIG_DIR = os.path.join(BASE_DIR, "conf")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if SHARED_DIR not in sys.path:
    sys.path.insert(0, SHARED_DIR)
del SHARED_DIR
del CONFIG_DIR

# pylint: disable=wrong-import-position

import utils  # noqa E402
from parameter_utils import parse_parameters  # noqa E402

try:
    # search in local modules
    if API_DIR not in sys.path:
        sys.path.insert(0, API_DIR)
    del API_DIR

    import hidra
except ImportError:
    # search in global python modules
    import hidra

BASEDIR = "/opt/hidra"

CONFIG_DIR = "/opt/hidra/conf"
CONFIG_PREFIX = "datamanager_"

LOGDIR = os.path.join("/var", "log", "hidra")
# LOGDIR = os.path.join(tempfile.gettempdir(), "hidra", "logs")


class HidraController(object):
    """
    This class holds getter/setter for all parameters
    and function members that control the operation.
    """

    def __init__(self, beamline, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline

        self.procname = "hidra_{}".format(self.beamline)
        self.username = "{}user".format(self.beamline)

        # Set log handler
        self.log = log

        self.fix_subdirs = ["current/raw",
                            "current/scratch_bl",
                            "commissioning/raw",
                            "commissioning/scratch_bl",
                            "local"]
        self.local_target = os.path.join("/beamline", self.beamline)

        self.master_config = dict()

        self.__read_config()

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = dict()

        self.ctemplate = {
            "active": False,
            "beamline": self.beamline,
            "det_ip": None,
            "det_api_version": None,
            "history_size": None,
            "store_data": None,
            "remove_data": None,
            "whitelist": None,
            "ldapuri": None
        }

    def __read_config(self):
        # pylint: disable=global-variable-not-assigned
        global CONFIG_PREFIX
        global CONFIG_DIR

        # write configfile
        # /etc/hidra/P01.conf
        joined_path = os.path.join(CONFIG_DIR, CONFIG_PREFIX + self.beamline)
        config_files = glob.glob(joined_path + "_*.conf")
        self.log.info("Reading config files: {}".format(config_files))

        for cfile in config_files:
            # extract the detector id from the config file name (remove path,
            # prefix, beamline and ending)
            det_id = cfile.replace(joined_path + "_", "")[:-5]
            try:
                config = utils.read_config(cfile)
                self.master_config[det_id] = (
                    parse_parameters(config)["asection"])
            except IOError:
                self.log.debug("Configuration file not readable: {}"
                               .format(cfile))
        self.log.debug("master_config={0}".format(self.master_config))

    def exec_msg(self, msg):
        """
        [b"IS_ALIVE"]
            return "OK"
        [b"do", host_id, det_id, b"start"]
            return "DONE"
        [b"bye", host_id, detector]
        """
        if len(msg) == 0:
            return "ERROR"

        if msg[0] == b"IS_ALIVE":
            return b"OK"

        elif msg[0] == b"set":
            if len(msg) < 4:
                return "ERROR"

            return self.set(msg[1], msg[2], msg[3], json.loads(msg[4]))

        elif msg[0] == b"get":
            if len(msg) != 4:
                return "ERROR"

            reply = json.dumps(self.get(msg[1], msg[2], msg[3]))
            self.log.debug("reply is {0}".format(reply))

            if reply is None:
                self.log.debug("reply is None")
                reply = "None"

            return reply

        elif msg[0] == b"do":
            if len(msg) != 4:
                return "ERROR"

            return self.do(msg[1], msg[2], msg[3])

        elif msg[0] == b"bye":
            if len(msg) != 3:
                return "ERROR"

            self.log.debug("Received 'bye' from host {} for detector {}"
                           .format(msg[1], msg[2]))
            if msg[1] in self.all_configs:
                if msg[2] in self.all_configs[msg[1]]:
                    del self.all_configs[msg[1]][msg[2]]

                # no configs for this host left
                if not self.all_configs[msg[1]]:
                    del self.all_configs[msg[1]]

            return "DONE"
        else:
            return "ERROR"

    def set(self, host_id, det_id, param, value):
        """
        set a parameter
        """
        # identify the configuration for this connection
        if host_id not in self.all_configs:
            self.all_configs[host_id] = dict()
        if det_id not in self.all_configs[host_id]:
            self.all_configs[host_id][det_id] = copy.deepcopy(self.ctemplate)

        # This is a pointer
        current_config = self.all_configs[host_id][det_id]

        key = param.lower()

        supported_keys = [
            # IP of the detector
            "det_ip",
            # API version of the detector
            "det_api_version",
            # Number of events stored to look for doubles
            "history_size",
            # Flag describing if the data should be stored in local_target
            "store_data",
            # Flag describing if the files should be removed from the source
            "remove_data",
            # List of hosts allowed to connect to the data distribution
            "whitelist",
            # Ldap node and port
            "ldapuri"
        ]

        if key in supported_keys:
            current_config[key] = value
            return_val = "DONE"

        else:
            self.log.debug("key={}; value={}".format(key, value))
            return_val = "ERROR"

        if return_val != "ERROR":
            current_config["active"] = True

        return return_val

    def get(self, host_id, det_id, param):
        """
        return the value of a parameter
        """
        # if the requesting client has set parameters before but has not
        # executed start yet, the previously set parameters should be
        # displayed (not the ones with which hidra was started the last time)
        # on the other hand if it is a client coming up to check with which
        # parameters the current hidra instance is running, these should be
        # shown
        if host_id in self.all_configs \
                and det_id in self.all_configs[host_id] \
                and self.all_configs[host_id][det_id]["active"]:
            # This is a pointer
            current_config = self.all_configs[host_id][det_id]
        else:
            current_config = self.master_config[det_id]

        key = param.lower()

        supported_keys = ["det_ip",
                          "det_api_version",
                          "history_size",
                          "store_data",
                          "remove_data",
                          "whitelist",
                          "ldapuri"]

        print("key", key)
        if key == "fix_subdirs":
            return str(self.fix_subdirs)

        elif key in supported_keys:
            return current_config[key]

        else:
            return "ERROR"

    def do(self, host_id, det_id, cmd):  # pylint: disable=invalid-name
        """
        executes commands
        """
        key = cmd.lower()

        if key == "start":
            ret_val = self.start(host_id, det_id)
            return ret_val
        elif key == "stop":
            return self.stop(det_id)

        elif key == "restart":
            return self.restart(host_id, det_id)

        elif key == "status":
            return hidra_status(self.beamline, det_id, self.log)

        else:
            return "ERROR"

    def __write_config(self, host_id, det_id):
        # pylint: disable=global-variable-not-assigned
        global CONFIG_DIR
        global CONFIG_PREFIX

        # identify the configuration for this connection
        if host_id in self.all_configs and det_id in self.all_configs[host_id]:
            # This is a pointer
            current_config = self.all_configs[host_id][det_id]
        else:
            self.log.debug("No current configuration found")
            return

        # if the requesting client has set parameters before these should be
        # taken. If this was not the case use the one from the previous
        # executed start
        if not current_config["active"]:
            self.log.debug("Config parameters did not change since last start")
            self.log.debug("No need to write new config file")
            return

        #
        # see, if all required params are there.
        #
        if (current_config["det_ip"]
                and current_config["det_api_version"]
                and current_config["history_size"]
                and current_config["store_data"] is not None
                and current_config["remove_data"] is not None
                and current_config["whitelist"]
                and current_config["ldapuri"]):

            external_ip = hidra.CONNECTION_LIST[self.beamline]["host"]

            # TODO set p00 to http
            if self.beamline == "p00":
                eventdetector = "inotifyx_events"
                datafetcher = "file_fetcher"
            else:
                eventdetector = "http_events"
                datafetcher = "http_fetcher"

            # write configfile
            # /etc/hidra/P01_eiger01.conf
            config_file = os.path.join(CONFIG_DIR,
                                       CONFIG_PREFIX + "{}_{}.conf"
                                       .format(self.beamline, det_id))
            self.log.info("Writing config file: {}".format(config_file))

            with open(config_file, 'w') as f:  # pylint: disable=invalid-name
                f.write("log_path = {}\n".format(LOGDIR))
                f.write("log_name = datamanager_{}.log\n"
                        .format(self.beamline))
                f.write("log_size = 10485760\n")
                f.write("procname = {}\n".format(self.procname))
                f.write("username = {}\n".format(self.username))
                f.write("ext_ip = {}\n".format(external_ip))
                f.write("com_port = 50000\n")
                f.write("request_port = 50001\n")

                f.write("event_detector_type = {}\n".format(eventdetector))
                f.write("fix_subdirs = {}\n".format(self.fix_subdirs))

                if eventdetector == "inotifyx_events":
                    f.write("monitored_dir = {}/data/source\n".format(BASEDIR))
                    f.write('monitored_events = {"IN_CLOSE_WRITE" : '
                            '[".tif", ".cbf", ".nxs"]}\n')
                f.write("use_cleanup = False\n")
                f.write("action_time = 150\n")
                f.write("time_till_closed = 2\n")

                f.write("data_fetcher_type = {}\n".format(datafetcher))

                f.write("number_of_streams = 32\n")
                f.write("use_data_stream = False\n")
                f.write("chunksize = 10485760\n")

                f.write("local_target = {}\n".format(self.local_target))

                for key in current_config:
                    f.write(key + " = {}\n".format(current_config[key]))

                self.log.info("Started with ext_ip: {}, event detector: {},"
                              " data fetcher: {}".format(external_ip,
                                                         eventdetector,
                                                         datafetcher))

                # store the configuration parameters globally
                self.log.debug("config = {}".format(current_config))
                self.master_config[det_id] = dict()
                for key in current_config:
                    if key != "active":
                        self.master_config[det_id][key] = (
                            copy.deepcopy(current_config[key]))

                # mark local_config as inactive
                current_config["active"] = False

        else:
            for key in current_config:
                self.log.debug(key + ":" + current_config[key])
            raise Exception("Not all required parameters are specified")

    def start(self, host_id, det_id):
        """
        start ...
        """

        # check if service is running
        if hidra_status(self.beamline, det_id, self.log) == "RUNNING":
            return "ALREADY_RUNNING"

        try:
            self.__write_config(host_id, det_id)
        except Exception:
            self.log.error("Config file not written", exc_info=True)
            return "ERROR"

        # start service
        if call_hidra_service("start", self.beamline, det_id, self.log) != 0:
            self.log.error("Could not start the service.")
            return "ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) == "RUNNING":
            return "DONE"
        else:
            self.log.error("Service is not running after triggering start.")
            return "ERROR"

    def stop(self, det_id):
        """
        stop ...
        """
        # check if really running before return
        if hidra_status(self.beamline, det_id, self.log) != "RUNNING":
            return "ARLEADY_STOPPED"

        # stop service
        if call_hidra_service("stop", self.beamline, det_id, self.log) == 0:
            return "DONE"
        else:
            self.log.error("Could not stop the service.")
            return "ERROR"

    def restart(self, host_id, det_id):
        """
        restart ...
        """
        # stop service
        reval = self.stop(det_id)

        if reval == "DONE":
            # start service
            return self.start(host_id, det_id)
        else:
            return "ERROR"


def call_hidra_service(cmd, beamline, det_id, log):
    """Command hidra (e.g. start, stop, statu,...).

    Args:
        beamline: For which beamline to command hidra.
        det_id: Which detector to command hidra for.
        log: log handler.

    Returns:
        Return value of the systemd or service call.
    """

    systemd_prefix = "hidra@"
    service_name = "hidra"

#    sys_cmd = ["/home/kuhnm/Arbeit/projects/hidra/initscripts/hidra.sh",
#               "--beamline", "p00",
#               "--detector", "asap3-mon",
#               "--"+cmd]
#    return subprocess.call(sys_cmd)

    # systems using systemd
    if (os.path.exists("/usr/lib/systemd")
            and (os.path.exists("/usr/lib/systemd/{}.service"
                                .format(systemd_prefix))
                 or os.path.exists("/usr/lib/systemd/system/{}.service"
                                   .format(systemd_prefix))
                 or os.path.exists("/etc/systemd/system/{}.service"
                                   .format(systemd_prefix)))):

        svc = "{}{}_{}.service".format(systemd_prefix, beamline, det_id)
        log.debug("Call: systemctl {} {}".format(cmd, svc))
        if cmd == "status":
            return subprocess.call(["systemctl", "is-active", svc])
        else:
            return subprocess.call(["sudo", "-n", "systemctl", cmd, svc])

    # systems using init scripts
    elif os.path.exists("/etc/init.d") \
            and os.path.exists("/etc/init.d/" + service_name):
        log.debug("Call: service {} {}".format(cmd, svc))
        return subprocess.call(["service", service_name, cmd])
        # TODO implement beamline and det_id in hisdra.sh
        # return subprocess.call(["service", service_name, "status",
        #                         beamline, det_id])
    else:
        log.debug("Call: no service to call found")


def hidra_status(beamline, det_id, log):
    """Request hidra status.

    Args:
        beamline: For which beamline to command hidra.
        det_id: Which detector to command hidra for.
        log: log handler.

    Returns:
        A string describing the status:
            'RUNNING'
            'NOT RUNNING'
            'ERROR'

    """

    try:
        proc = call_hidra_service("status", beamline, det_id, log)
    except Exception:
        return "ERROR"

    if proc == 0:
        return "RUNNING"
    else:
        return "NOT RUNNING"


def argument_parsing():
    """Parsing of command line arguments.
    """

    parser = argparse.ArgumentParser()

    parser.add_argument("--beamline",
                        type=str,
                        help="Beamline for which the HiDRA Server "
                             "(detector mode) should be started",
                        default="p00")
    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onscreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    return parser.parse_args()


class ControlServer(object):
    """The main server class.
    """

    def __init__(self):

        self.beamline = None
        self.context = None
        self.socket = None

        self.master_config = None
        self.controller = None
        self.endpoint = None

        self.log_queue = None
        self.log_queue_listener = None

        self._setup()

    def _setup(self):

        arguments = argument_parsing()

        self.beamline = arguments.beamline

        # pylint: disable=no-member
        setproctitle.setproctitle("hidra-control-server_{}"
                                  .format(self.beamline))

        logfile = os.path.join(
            LOGDIR,
            "hidra-control-server_{}.log".format(self.beamline)
        )
        logsize = 10485760

        # Get queue
        self.log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        if arguments.onscreen:
            handler1, handler2 = utils.get_log_handlers(
                logfile,
                logsize,
                arguments.verbose,
                arguments.onscreen
            )

            # Start queue listener using the stream handler above.
            self.log_queue_listener = utils.CustomQueueListener(
                self.log_queue, handler1, handler2
            )
        else:
            handler1 = utils.get_log_handlers(
                logfile,
                logsize,
                arguments.verbose,
                arguments.onscreen
            )

            # Start queue listener using the stream handler above
            self.log_queue_listener = utils.CustomQueueListener(
                self.log_queue, handler1
            )

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("ControlServer", self.log_queue)

        self.log.info("Init")

        self.controller = HidraController(self.beamline, self.log)

        host = hidra.CONNECTION_LIST[self.beamline]["host"]
        host = socket.gethostbyaddr(host)[2][0]
        port = hidra.CONNECTION_LIST[self.beamline]["port"]
        self.endpoint = "tcp://{}:{}".format(host, port)

        self._create_sockets()

        self.run()

    def _create_sockets(self):

        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.endpoint)
            self.log.info("Start socket (bind): '{}'"
                          .format(self.endpoint))
        except zmq.error.ZMQError:
            self.log.error("Failed to start socket (bind) zmqerror: '{}'"
                           .format(self.endpoint), exc_info=True)
            raise
        except Exception:
            self.log.error("Failed to start socket (bind): '{}'"
                           .format(self.endpoint), exc_info=True)
            raise

    def run(self):
        """Waiting for new control commands and execute them.
        """

        while True:
            try:
                msg = self.socket.recv_multipart()
                self.log.debug("Recv {}".format(msg))
            except KeyboardInterrupt:
                break

            if len(msg) == 0:
                self.log.debug("Received empty msg")
                break

            elif msg[0] == b"exit":
                self.log.debug("Received 'exit'")
                self.stop()
                sys.exit(1)

            reply = self.controller.exec_msg(msg)

            self.socket.send(reply)

    def stop(self):
        """Clean up zmq sockets.
        """

        if self.socket:
            self.log.info("Closing Socket")
            self.socket.close()
            self.socket = None
        if self.context:
            self.log.info("Destroying Context")
            self.context.destroy()
            self.context = None

    def __exit__(self, exception_type, exception_value, traceback):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    ControlServer()
