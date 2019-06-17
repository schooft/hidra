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

import setproctitle
import zmq

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
BASE_DIR = os.path.dirname(os.path.dirname(CURRENT_DIR))
CONFIG_DIR = os.path.join(BASE_DIR, "conf")
API_DIR = os.path.join(BASE_DIR, "src", "APIs")

if API_DIR not in sys.path:
    sys.path.insert(0, API_DIR)
del API_DIR

# pylint: disable=wrong-import-position

import hidra  # noqa E402
import hidra.utils as utils  # noqa E402
from hidra.utils import FormatError  # noqa E402

CONFIG_PREFIX = "datamanager_"


class InstanceTracking(object):
    """Handles instance tracking.
    """

    def __init__(self, beamline, backup_file, log):
        self.beamline = beamline
        self.backup_file = backup_file
        self.log = log

        self.instances = None
        self._set_instances()

    def _set_instances(self):
        """Set all previously started instances.
        """

        try:
            with open(self.backup_file, 'r') as f:
                self.instances = json.loads(f.read())
        except IOError:
            # file does not exist
            self.instances = {}
        except Exception:
            # file content ist not as expected
            self.log.error("File containing instances existed but error "
                           "occured when reading it", exc_info=True)
            self.instances = {}

    def _update_instances(self):
        """Updates the instances file
        """

        try:
            with open(self.backup_file, "w") as f:
                f.write(json.dumps(self.instances, sort_keys=True, indent=4))
        except Exception:
            self.log.error("File containing instances could not be written",
                           exc_info=True)

    def get_instances(self):
        """Get all previously started instances

        Returns:
            A dictionary containing the instances of the form:
            { <beamline>: { <detector>: timestamp } }
        """

        return self.instances

    def add(self, det_id):
        """Mark instance as started.
        """

        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        if self.beamline in self.instances:
            self.instances[self.beamline][det_id] = timestamp
        else:
            self.instances[self.beamline] = {det_id: timestamp}

        self._update_instances()

    def remove(self, det_id):
        """Remove instance from tracking.
        """

        if self.beamline in self.instances:
            try:
                del self.instances[self.beamline][det_id]
            except KeyError:
                self.log.warning("detector %s was not found in instance "
                                 "list", det_id)
        else:
            self.log.warning("beamline %s was not found in instance list",
                             self.beamline)

        self._update_instances()

    def restart_instances(self):
        """Restarts instances if needed.
        """

        if self.beamline not in self.instances:
            return

        for det_id in self.instances[self.beamline]:
            # check if running
            if self.hidra_status(det_id) == "RUNNING":
                self.log.info("Started hidra for %s_%s, already running",
                              self.beamline, det_id)
                continue

            # restart
            if self.call_hidra_service("start", det_id) == 0:
                self.log.info("Started hidra for %s_%s",
                              self.beamline, det_id)
            else:
                self.log.error("Could not start hidra for %s_%s",
                               self.beamline, det_id)


class ConfigHandling(object):
    def __init__(self, beamline, config, log):
        self.beamline = beamline
        self.log = log

        self.config = config
        self.config_static = self.config["hidraconfig_static"]
        self.config_variable = self.config["hidraconfig_variable"]

        self.config_ending = ".yaml"
        self.master_config = {}

        # connection depending hidra configuration, master config one is
        # overwritten with these parameters when start is executed
        self.all_configs = dict()

        ed_type = "http_events"
        self.ctemplate = {
            "active": False,
            "beamline": self.beamline,
            "general": {
                "ldapuri": None,
                "whitelist": None,
            },
            "eventdetector": {
                "type": ed_type,
                ed_type: {
                    "det_ip": None,
                    "det_api_version": None,
                    "history_size": None,
                }
            },
            "datafetcher": {
                "store_data": None,
                "remove_data": None,
            }
        }

        ed_type = "http_events"
        self.required_params = {
            "general": ["ldapuri", "whitelist"],
            "eventdetector": [
                ["type", [ed_type]],
                {ed_type: ["det_ip", "det_api_version", "history_size"]}
            ],
            "datafetcher": ["store_data", "remove_data"]
        }

        self.__read_config()

    def set(self, host_id, det_id, param, value):

        # identify the configuration for this connection
        if host_id not in self.all_configs:
            self.all_configs[host_id] = dict()
        if det_id not in self.all_configs[host_id]:
            self.all_configs[host_id][det_id] = copy.deepcopy(self.ctemplate)

        utils.set_flat_param(
            param=param,
            param_value=value,
            config=self.all_configs[host_id][det_id],
            config_type="sender",
            log=self.log
        )

    def activate(self, host_id, det_id):
        self.all_configs[host_id][det_id]["active"] = True


    def get(self, host_id, det_id, param):
        # if the requesting client has set parameters before but has not
        # executed start yet, the previously set parameters should be
        # displayed (not the ones with which hidra was started the last time)
        # on the other hand if it is a client coming up to check with which
        # parameters the current hidra instance is running, these should be
        # shown
        try:
            if self.all_configs[host_id][det_id]["active"]:
                # This is a pointer
                current_config = self.all_configs[host_id][det_id]
            else:
                raise KeyError
        except KeyError:
            current_config = self.master_config[det_id]

        return utils.get_flat_param(param,
                                    current_config,
                                    "sender",
                                    log=self.log)


    def clear(self, host_id, det_id):

        if host_id in self.all_configs:
            try:
                del self.all_configs[host_id][det_id]
            except KeyError:
                pass

            # no configs for this host left
            if not self.all_configs[host_id]:
                del self.all_configs[host_id]

    def __read_config(self):

        # write configfile
        # /etc/hidra/P01.conf
        joined_path = os.path.join(CONFIG_DIR, CONFIG_PREFIX + self.beamline)
        config_files = glob.glob(joined_path + "_*" + self.config_ending)
        self.log.info("Reading config files: %s", config_files)

        for cfile in config_files:
            # extract the detector id from the config file name (remove path,
            # prefix, beamline and ending)
            det_id = cfile.replace(joined_path + "_", "")[:-5]
            try:
                self.master_config[det_id] = utils.load_config(cfile,
                                                               log=self.log)
            except IOError:
                self.log.debug("Configuration file not readable: %s", cfile)
            except Exception:
                self.log.debug("cfile=%s", cfile)
                self.log.error("Error when trying to load config file",
                               exc_info=True)
                raise
        self.log.debug("master_config=%s", json.dumps(self.master_config,
                                                      sort_keys=True,
                                                      indent=4))

    def _check_config_complete(self, host_id, det_id):
        """
         Check if all required params are there.
        """

        # identify the configuration for this connection
        if (host_id in self.all_configs
            and det_id in self.all_configs[host_id]):
            # This is a pointer
            current_config = self.all_configs[host_id][det_id]
        else:
            self.log.debug("No current configuration found")
            raise utils.NotFoundError()

        config_complete, _ = utils.check_config(self.required_params,
                                                current_config,
                                                self.log)

        if not config_complete:
            self.log.debug(
                json.dumps(current_config, sort_keys=True, indent=4)
            )
            raise utils.WrongConfiguration(
                "Not all required parameters are specified"
            )


    def write_config(self, host_id, det_id):
        # pylint: disable=global-variable-not-assigned
        global CONFIG_DIR
        global CONFIG_PREFIX

        try:
            self._check_config_complete(host_id, det_id)
        except utils.NotFoundError:
            return

        current_config = self.all_configs[host_id][det_id]

        # if the requesting client has set parameters before these should be
        # taken. If this was not the case use the one from the previous
        # executed start
        if not current_config["active"]:
            self.log.debug("Config parameters did not change since last start")
            self.log.debug("No need to write new config file")
            return

        # add variable config
        config_g = self.config_variable["general"]
        config_df = self.config_variable["datafetcher"]

        username = config_g["username"].format(bl=self.beamline)
        procname_prefix = config_g["procname"].format(bl=self.beamline)
        procname = "{}_{}".format(procname_prefix, det_id)
        log_name_prefix = config_g["log_name"].format(bl=self.beamline)
        log_name = "{}_{}.log".format(log_name_prefix, det_id)
        local_target = config_df["local_target"].format(bl=self.beamline)
        external_ip = hidra.CONNECTION_LIST[self.beamline]["host"]

        self.config_static["general"]["log_name"] = log_name
        self.config_static["general"]["procname"] = procname
        self.config_static["general"]["username"] = username
        self.config_static["general"]["ext_ip"] = external_ip
        df_type = self.config_static["datafetcher"]["type"]
        try:
            self.config_static["datafetcher"][df_type]["local_target"] = (
                local_target
            )
        except KeyError:
            self.config_static["datafetcher"][df_type] = {
                "local_target": local_target
            }

        # dynamic config
        utils.update_dict(current_config, self.config_static)

        # write configfile
        # /etc/hidra/P01_eiger01.conf
        config_file = os.path.join(
            CONFIG_DIR,
            self.config["controlserver"]["hidra_config_name"]
            .format(bl=self.beamline, det=det_id)
        )
        self.log.info("Writing config file: {}".format(config_file))
        utils.write_config(config_file, self.config_static, log=self.log)

        ed_type = self.config_static["eventdetector"]["type"]
        df_type = self.config_static["datafetcher"]["type"]
        self.log.info(
            "Started with ext_ip: %s, event detector: %s, "
            "data fetcher: %s",
            external_ip, ed_type, df_type
        )

        # store the dynamic config globally
        self.log.debug("config = {}", self.config_static)
        self.master_config[det_id] = copy.deepcopy(self.config_static)
        # this information shout not go into the master config
        del self.master_config[det_id]["active"]

        # mark local_config as inactive
        current_config["active"] = False


class HidraController(utils.Base):
    """
    This class holds getter/setter for all parameters
    and function members that control the operation.
    """

    def __init__(self, beamline, config, log):

        # Beamline is read-only, determined by portNo
        self.beamline = beamline
        self.config = config
        self.config_cs = None
        self.ldapuri = None
        self.netgroup_template = None

        # Set log handler
        self.log = log

        self.confighandling = None
        self.instances = None

        self.enable_hidra_connection = None
        self.supported_keys = []

        self.systemd_prefix = None
        self.service_name = None
        self.service_manager = None
        self.systemd_service_tmpl = None,

        self._setup()

    def _setup(self):
        self.config_cs = self.config["controlserver"]
        self.ldapuri = self.config_cs["ldapuri"]
        self.netgroup_template = self.config_cs["netgroup_template"]

        self.confighandling = ConfigHandling(self.beamline,
                                             self.config,
                                             self.log)

        backup_file = self.config_cs["backup_file"]
        self.instances = InstanceTracking(self.beamline, backup_file, self.log)
        self.instances.restart_instances()

        self.supported_keys = [
            "ldapuri",
            "whitelist",
            "det_ip",
            "det_api_version",
            "history_size",
            "store_data",
            "remove_data",
            "fix_subdirs"
        ]

#        self.supported_keys = [k for k in list(self.ctemplate.keys())
#                               if k not in ["active", "beamline"]]

        self.systemd_prefix = "hidra@"
        self.service_name = "hidra"

        self.service_manager = utils.get_service_manager(
            systemd_prefix=self.systemd_prefix,
            service_name=self.service_name
        )

        self.systemd_service_tmpl = ("{}{}".format(self.systemd_prefix,
                                                   self.beamline)
                                     + "_{}.service")

    def _decode_message(self, msg):
        """Decode the message
        """

        try:
            action = msg[0]
        except IndexError:
            raise FormatError

        if action == b"IS_ALIVE":
            return action, None, None, None, None

        try:
            action, host_id, det_id = msg[:3]
            host_id = host_id.decode()
            det_id = det_id.decode()
        except ValueError:
            self.log.error("No host_id and det_id defined")
            raise FormatError

        det_id = socket.getfqdn(det_id)

        if action == b"set":

            if len(msg) < 4:
                self.log.error("Not enough arguments")
                raise FormatError

            param, value = msg[3:]
            param = param.decode().lower()
            value = json.loads(value.decode())

        elif action in [b"get", b"do"]:
            if len(msg) != 4:
                self.log.error("Not enough arguments")
                raise FormatError

            param, value = msg[3].decode().lower(), None

        elif action == b"bye":
            param, value = None, None

        else:
            self.log.error("Unknown action")
            raise FormatError

        return action, host_id, det_id, param, value

    def exec_msg(self, msg):
        """
        [b"IS_ALIVE"]
            return "OK"
        [b"do", host_id, det_id, b"start"]
            return "DONE"
        [b"bye", host_id, detector]
        """

        try:
            action, host_id, det_id, param, value = self._decode_message(msg)
        except FormatError:
            self.log.error("Message of wrong format")
            return b"ERROR"

        if action == b"IS_ALIVE":
            return b"OK"

        try:
            # check if host is allowed to execute commands
            check_res = utils.check_netgroup(
                host_id,
                self.beamline,
                self.ldapuri,
                self.netgroup_template.format(bl=self.beamline),
                log=self.log,
                exit=False
            )
        except Exception:
            self.log.error("Error when checking netgroup", exc_info=True)
            self.log.debug("msg=%s", msg)
            return b"ERROR"

        if not check_res:
            return b"ERROR"

        if action == b"set":
            return self.set(host_id, det_id, param, value)

        elif action == b"get":
            reply = json.dumps(
                self.get(host_id, det_id, param)
            ).encode()
            self.log.debug("reply is %s", reply)

            if reply is None:
                self.log.debug("reply is None")
                reply = b"None"

            return reply

        elif action == b"do":
            return self.do(host_id, det_id, param)

        elif action == b"bye":
            return self.bye(host_id, det_id)
        else:
            return b"ERROR"

    def set(self, host_id, det_id, param, value):
        """
        set a parameter
        """

        if param in self.supported_keys:
            self.confighandling.set(host_id, det_id, param, value)
            self.confighandling.activate(host_id, det_id)
            return_val = b"DONE"
        else:
            self.log.debug("param=%s; value=%s", param, value)
            return_val = b"ERROR"

        return return_val

    def get(self, host_id, det_id, param):
        """
        return the value of a parameter
        """

        if param in self.supported_keys:
            value = self.confighandling.get(host_id, det_id, param)

            if isinstance(value, list):
                return str(value)
            else:
                return value

        else:
            self.log.debug("param=%s", param)
            return b"ERROR"

    def do(self, host_id, det_id, cmd):  # pylint: disable=invalid-name
        """
        executes commands
        """
        if cmd == "start":
            ret_val = self.start(host_id, det_id)
            return ret_val
        elif cmd == "stop":
            return self.stop(det_id)

        elif cmd == "restart":
            return self.restart(host_id, det_id)

        elif cmd == "status":
            return self.hidra_status(det_id)

        elif cmd == "get_instances":
            return self.get_instances()

        else:
            return b"ERROR"

    def bye(self, host_id, det_id):
        self.log.debug("Received 'bye' from host %s for detector %s",
                       host_id, det_id)

        self.confighandling.clear(host_id, det_id)

        return b"DONE"

    def call_hidra_service(self, cmd, det_id):
        """Command hidra (e.g. start, stop, status,...).

        Args:
            cmd: The command to call the service with
                 (e.g. start, stop, status,...).
            det_id: Which detector to command hidra for.

        Returns:
            Return value of the systemd or service call.
        """

    #    return 0

    #    sys_cmd = ["/home/kuhnm/Arbeit/projects/hidra/initscripts/hidra.sh",
    #               "--beamline", "p00",
    #               "--detector", "asap3-mon",
    #               "--"+cmd]
    #    return subprocess.call(sys_cmd)

        # system using systemd
        if self.service_manager == "systemd":
            svc = self.systemd_service_tmpl.format(det_id)
            self.log.debug("Call: systemctl %s %s", cmd, svc)

            if cmd == "status":
                return subprocess.call(["systemctl", "is-active", svc])
            else:
                return subprocess.call(["sudo", "-n", "systemctl", cmd, svc])

        # system using init scripts
        elif self.service_manager == "init":
            self.log.debug("Call: service %s %s", self.service_name, cmd)
            return subprocess.call(["service", self.service_name, cmd])
            # TODO implement beamline and det_id in hisdra.sh
            # return subprocess.call(["service", self.service_name, "status",
            #                         beamline, det_id])

        else:
            self.log.debug("Call: no service to call found")

    def hidra_status(self, det_id):
        """Request hidra status.

        Args:
            det_id: Which detector to command hidra for.

        Returns:
            A string describing the status:
                'RUNNING'
                'NOT RUNNING'
                'ERROR'
        """

        try:
            proc = self.call_hidra_service("status", det_id)
        except Exception:
            return b"ERROR"

        if proc == 0:
            return b"RUNNING"
        else:
            return b"NOT RUNNING"

    def start(self, host_id, det_id):
        """
        start ...
        """

        # check if service is running
        if self.hidra_status(det_id) == b"RUNNING":
            return b"ALREADY_RUNNING"

        try:
            self.confighandling.write_config(host_id, det_id)
        except Exception:
            self.log.error("Config file not written", exc_info=True)
            return b"ERROR"

        # start service
        if self.call_hidra_service("start", det_id) != 0:
            self.log.error("Could not start the service.")
            return b"ERROR"

        # Needed because status always returns "RUNNING" in the first second
        time.sleep(1)

        # check if really running before return
        if self.hidra_status(det_id) != b"RUNNING":
            self.log.error("Service is not running after triggering start.")

            if self.service_manager == "systemd":
                status = utils.read_status(
                    service=self.systemd_service_tmpl.format(det_id),
                    log=self.log
                )["info"]
                self.log.debug("systemctl status: \n%s", status)

            return b"ERROR"

        # remember that the instance was started
        self.instances.add(det_id)
        return b"DONE"

    def get_instances(self):
        """Get the started hidra instances

        Returns:
            List of detectors started for this beamline as json dump.
        """

        try:
            bl_instances = self.instances.get_instances()[self.beamline]
        except KeyError:
            # something went wrong when trying to start the instance
            bl_instances = {}

        return json.dumps(list(bl_instances.keys())).encode()

    def stop(self, det_id):
        """
        stop ...
        """
        # check if really running before return
        if self.hidra_status(det_id) != b"RUNNING":
            return b"ARLEADY_STOPPED"

        # stop service
        if self.call_hidra_service("stop", det_id) != 0:
            self.log.error("Could not stop the service.")
            return b"ERROR"

        self.instances.remove(det_id)
        return b"DONE"

    def restart(self, host_id, det_id):
        """
        restart ...
        """
        # stop service
        reval = self.stop(det_id)

        if reval == b"DONE":
            # start service
            return self.start(host_id, det_id)
        else:
            return b"ERROR"

    def _stop(self):
        """
        Clean up.
        """
        pass

    def __del__(self):
        self._stop()

    def __exit__(self):
        self._stop()


def argument_parsing():
    """Parsing of command line arguments.
    """

    config_file = os.path.join(CONFIG_DIR, "control_server.yaml")

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

    arguments = parser.parse_args()

    # convert to dict and map to config section
    arguments = {"controlserver": vars(arguments)}

    # ------------------------------------------------------------------------
    # Get arguments from config file and comand line
    # ------------------------------------------------------------------------
    utils.check_existance(config_file)

    config = utils.load_config(config_file)
    utils.update_dict(arguments, config)

    # the configuration is now of the form:
    # {
    #   "controlserver": {...}
    #   "hidraconfig_static" : {...}
    #   "hidraconfig_variable" : {...}
    # }

    # TODO check config for required params

    return config


class ControlServer(object):
    """The main server class.
    """

    def __init__(self):

        self.beamline = None
        self.context = None
        self.socket = None

        self.controller = None
        self.endpoint = None

        self.log_queue = None
        self.log_queue_listener = None

        self._setup()

        self.run()

    def _setup(self):

        config = argument_parsing()

        # shortcut for simpler use
        config_ctrl = config["controlserver"]

        self.beamline = config_ctrl["beamline"]

        setproctitle.setproctitle(config_ctrl["procname"]
                                  .format(bl=self.beamline))

        logfile = os.path.join(
            config_ctrl["log_path"],
            config_ctrl["log_name"].format(bl=self.beamline)
        )

        # Get queue
        self.log_queue = Queue(-1)

        handler = utils.get_log_handlers(
            logfile,
            config_ctrl["log_size"],
            config_ctrl["verbose"],
            config_ctrl["onscreen"]
        )

        # Start queue listener using the stream handler above
        self.log_queue_listener = utils.CustomQueueListener(
            self.log_queue, *handler
        )

        self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = utils.get_logger("ControlServer", self.log_queue)
        self.log.info("Init")

        self.controller = HidraController(self.beamline, config, self.log)

        host = hidra.CONNECTION_LIST[self.beamline]["host"]
        host = socket.gethostbyaddr(host)[2][0]
        port = hidra.CONNECTION_LIST[self.beamline]["port"]
        self.endpoint = "tcp://{}:{}".format(host, port)

        self._create_sockets()

    def _create_sockets(self):

        # Create ZeroMQ context
        self.log.info("Registering ZMQ context")
        self.context = zmq.Context()

        # socket to get requests
        try:
            self.socket = self.context.socket(zmq.REP)
            self.socket.bind(self.endpoint)
            self.log.info("Start socket (bind): '%s'", self.endpoint)
        except zmq.error.ZMQError:
            self.log.error("Failed to start socket (bind) zmqerror: '%s'",
                           self.endpoint, exc_info=True)
            raise
        except Exception:
            self.log.error("Failed to start socket (bind): '%s'",
                           self.endpoint, exc_info=True)
            raise

    def run(self):
        """Waiting for new control commands and execute them.
        """

        while True:
            try:
                msg = self.socket.recv_multipart()
                self.log.debug("Recv %s", msg)
            except KeyboardInterrupt:
                break

            if not msg:
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
