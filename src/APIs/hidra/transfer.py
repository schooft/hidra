# API to communicate with a data transfer unit

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import copy
import errno
import json
import logging
import os
import re
import socket
import sys
import tempfile
import time
import zmq
from multiprocessing import Queue
from zmq.auth.thread import ThreadAuthenticator

from ._version import __version__
from ._shared_utils import LoggingFunction


class NotSupported(Exception):
    pass


class UsageError(Exception):
    pass


class FormatError(Exception):
    pass


class ConnectionFailed(Exception):
    pass


class VersionError(Exception):
    pass


class AuthenticationFailed(Exception):
    pass


class CommunicationFailed(Exception):
    pass


class DataSavingError(Exception):
    pass


def get_logger(logger_name,
               queue=False,
               log_level="debug"):
    """Send all logs to the main process.

    The worker configuration is done at the start of the worker process run.
    Note that on Windows you can't rely on fork semantics, so each process
    will run the logging configuration code when it starts.
    """

    log_level_lower = log_level.lower()

    if queue:
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger(logger_name)
        logger.propagate = False
        logger.addHandler(h)

        if log_level_lower == "debug":
            logger.setLevel(logging.DEBUG)
        elif log_level_lower == "info":
            logger.setLevel(logging.INFO)
        elif log_level_lower == "warning":
            logger.setLevel(logging.WARNING)
        elif log_level_lower == "error":
            logger.setLevel(logging.ERROR)
        elif log_level_lower == "critical":
            logger.setLevel(logging.CRITICAL)
    else:
        logger = LoggingFunction(log_level_lower)

    return logger


def generate_filepath(base_path, config_dict, add_filename=True):
    """
    Generates full path (including file name) where file will be saved to

    """
    if not config_dict or base_path is None:
        return None

    if (config_dict["relative_path"] == ""
            or config_dict["relative_path"] is None):
        target_path = base_path

    # if the relative path starts with a slash path.join will consider it
    # as absolute path
    elif config_dict["relative_path"].startswith("/"):
        target_path = (os.path.normpath(
            os.path.join(base_path, config_dict["relative_path"][1:])))

    else:
        target_path = (os.path.normpath(
            os.path.join(base_path, config_dict["relative_path"])))

    if add_filename:
        filepath = os.path.join(target_path, config_dict["filename"])

        return filepath
    else:
        return target_path


def generate_file_identifier(config_dict):
    """
    Generates file identifier assempled out of relative path and file name

    """
    if not config_dict:
        return None

    if (config_dict["relative_path"] == ""
            or config_dict["relative_path"] is None):
        file_id = config_dict["filename"]

    # if the relative path starts with a slash path.join will consider it
    # as absolute path
    elif config_dict["relative_path"].startswith("/"):
        file_id = os.path.join(config_dict["relative_path"][1:],
                               config_dict["filename"])
    else:
        file_id = os.path.join(config_dict["relative_path"],
                               config_dict["filename"])
    return file_id


def convert_suffix_list_to_regex(pattern,
                                 suffix=True,
                                 compile_regex=False,
                                 log=None):
    """
    Takes a list of suffixes and converts it into a corresponding regex
    If input is a string nothing is done

    Args:
        pattern (list or string): a list of suffixes, regexes or a string
        suffix (boolean): if a list of regexes is given which should be merged
        compile_regex (boolean): if the regex should be compiled
        log: logging handler (optional)

    Returns:
        regex (regex object): compiled regular expression of the style
                              ".*(<suffix>|...)$ resp. (<regex>|<regex>)"
    """
    # Convert list into regex
    if type(pattern) == list:

        if suffix:
            regex = ".*"
        else:
            regex = ""

        file_suffix = ""
        for s in pattern:
            if s:
                # not an empty string
                file_suffix += s
            file_suffix += "|"

        # remove the last "|"
        file_suffix = file_suffix[:-1]

        if file_suffix:
            regex += "({})$".format(file_suffix)

    # a regex was given
    else:
        regex = pattern

    if log:
        log.debug("converted regex={}".format(regex))

    if compile_regex:
        return re.compile(regex)
    else:
        return regex


class Transfer():
    def __init__(self,
                 connection_type,
                 signal_host=None,
                 use_log=False,
                 context=None,
                 dirs_not_to_create=None):

        # print messages of certain level to screen
        if use_log in ["debug", "info", "warning", "error", "critical"]:
            self.log = LoggingFunction(use_log)
        # use logutils queue
        elif type(use_log) == Queue:
            self.log = get_logger("Transfer", use_log)
        # use logging
        elif use_log:
            self.log = logging.getLogger("Transfer")
        # use no logging at all
        elif use_log is None:
            self.log = LoggingFunction(None)
        # print everything to screen
        else:
            self.log = LoggingFunction("debug")

        # ZMQ applications always start by creating a context,
        # and then using that for creating sockets
        # (source: ZeroMQ, Messaging for Many Applications by Pieter Hintjens)
        if context is not None:
            self.context = context
            self.ext_context = True
        else:
            self.context = zmq.Context()
            self.ext_context = False

        self.current_pid = os.getpid()

        if signal_host is not None:
            self.signal_host = socket.getfqdn(signal_host)
        else:
            self.signal_host = None
        self.signal_port = "50000"
        self.request_port = "50001"
        self.file_op_port = "50050"
        self.status_check_port = "50050"
        self.confirmation_port = "50053"
        self.ipc_dir = os.path.join(tempfile.gettempdir(), "hidra")

        self.is_ipv6 = False

        self.data_con_style = "bind"

        self.signal_socket = None
        self.request_socket = None
        self.file_op_socket = None
        self.status_check_socket = None
        self.data_socket = None
        self.confirmation_socket = None
        self.control_socket = None

        self.poller = zmq.Poller()

        self.auth = None

        self.targets = None

        self.supported_connections = ["STREAM", "STREAM_METADATA",
                                      "QUERY_NEXT", "QUERY_METADATA",
                                      "NEXUS"]

        if dirs_not_to_create is None:
            self.dirs_not_to_create = dirs_not_to_create
        else:
            self.dirs_not_to_create = tuple(dirs_not_to_create)

        self.signal_exchanged = None

        self.started_connections = dict()
        self.status = [b"OK"]

        self.socket_response_timeout = 1000

        self.number_of_streams = None
        self.recvd_close_from = []
        self.reply_to_signal = False
        self.all_close_recvd = False

        self.file_descriptors = dict()

        self.file_opened = False
        self.callback_params = None
        self.open_callback = None
        self.read_callback = None
        self.close_callback = None

        self.stopped_everything = False

        # In older api versions this was a class method
        # (further support for users)
        self.generate_target_filepath = generate_filepath

        if connection_type in self.supported_connections:
            self.connection_type = connection_type
        else:
            raise NotSupported("Chosen type of connection is not supported.")

    def get_remote_version(self):
        self.__create_signal_socket(self.signal_port)
        if self.targets is None:
            self.targets = []

        signal = b"GET_VERSION"
        message = self.__send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received signal confirmation ...")
            return message[1]
        else:
            self.log.error("Invalid confirmation received...")
            self.log.debug("message={}".format(message))
            return None

    # targets: [host, port, prio] or [[host, port, prio], ...]
    def initiate(self, targets):
        if self.connection_type == "NEXUS":
            self.log.info("There is no need for a signal exchange for "
                          "connection type 'NEXUS'")
            return

        if type(targets) != list:
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context = zmq.Context()
            self.ext_context = False

        if self.poller is None:
            self.poller = zmq.Poller()

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal_port = self.signal_port
            signal = b"START_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal_port = self.signal_port
            signal = b"START_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal_port = self.signal_port
            signal = b"START_QUERY_NEXT"
        elif self.connection_type == "QUERY_METADATA":
            signal_port = self.signal_port
            signal = b"START_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")

        if self.signal_host:
            self.__create_signal_socket(signal_port)
        else:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified.")

        self.__set_targets(targets)

        message = self.__send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received signal confirmation ...")
            self.signal_exchanged = signal

        else:
            raise CommunicationFailed("Sending start signal ...failed.")

    def __create_signal_socket(self, signal_port):

        # To send a notification that a Displayer is up and running, a
        # communication socket is needed
        # create socket to exchange signals with Sender
        self.signal_socket = self.context.socket(zmq.REQ)

        # time to wait for the sender to give a confirmation of the signal
        # self.signal_socket.RCVTIMEO = self.socket_response_timeout
        connection_str = "tcp://{}:{}".format(self.signal_host, signal_port)
        try:
            self.signal_socket.connect(connection_str)
            self.log.info("signal_socket started (connect) for '{}'"
                          .format(connection_str))
        except:
            self.log.error("Failed to start signal_socket (connect): '{}'"
                           .format(connection_str))
            raise

        # using a Poller to implement the signal_socket timeout
        self.poller.register(self.signal_socket, zmq.POLLIN)

    def __set_targets(self, targets):
        self.targets = []

        # [host, port, prio]
        if (len(targets) == 3
                and type(targets[0]) != list
                and type(targets[1]) != list
                and type(targets[2]) != list):
            host, port, prio = targets
            self.targets = [["{}:{}".format(socket.getfqdn(host), port),
                             prio,
                             ".*"]]

        # [host, port, prio, suffixes]
        elif (len(targets) == 4
                and type(targets[0]) != list
                and type(targets[1]) != list
                and type(targets[2]) != list
                and type(targets[3]) == list):
            host, port, prio, suffixes = targets

            regex = convert_suffix_list_to_regex(suffixes,
                                                 log=self.log)

            self.targets = [["{}:{}".format(socket.getfqdn(host), port),
                             prio,
                             regex]]

        # [[host, port, prio], ...] or [[host, port, prio, suffixes], ...]
        else:
            for t in targets:
                len_t = len(t)
                if (type(t) == list
                        and (len_t == 3 or len_t == 4)
                        and type(t[0]) != list
                        and type(t[1]) != list
                        and type(t[2]) != list):

                    if len_t == 3:
                        host, port, prio = t
                        suffixes = [""]
                    else:
                        host, port, prio, suffixes = t

                    regex = convert_suffix_list_to_regex(suffixes,
                                                         log=self.log)

                    self.targets.append(
                        ["{}:{}".format(socket.getfqdn(host), port),
                         prio,
                         regex])
                else:
                    self.stop()
                    self.log.debug("targets={}".format(targets))
                    raise FormatError("Argument 'targets' is of wrong format.")

    def __send_signal(self, signal):

        if not signal:
            return

        # Send the signal that the communication infrastructure should be
        # established
        self.log.info("Sending Signal")

        send_message = [__version__, signal]

        trg = json.dumps(self.targets).encode('utf-8')
        send_message.append(trg)

        self.log.debug("Signal: {}".format(send_message))
        try:
            self.signal_socket.send_multipart(send_message)
        except:
            self.log.error("Could not send signal")
            raise

        message = None
        try:
            socks = dict(self.poller.poll(self.socket_response_timeout))
        except:
            self.log.error("Could not poll for new message")
            raise

        # if there was a response
        if (self.signal_socket in socks
                and socks[self.signal_socket] == zmq.POLLIN):
            try:
                #  Get the reply.
                message = self.signal_socket.recv_multipart()
                self.log.info("Received answer to signal: {}"
                              .format(message))

            except:
                self.log.error("Could not receive answer to signal")
                raise

        # check correctness of message
        if message and message[0] == b"VERSION_CONFLICT":
            self.stop()
            raise VersionError("Versions are conflicting. Sender version: {0},"
                               " API version: {}"
                               .format(message[1], __version__))

        elif message and message[0] == b"NO_VALID_HOST":
            self.stop()
            raise AuthenticationFailed("Host is not allowed to connect.")

        elif message and message[0] == b"CONNECTION_ALREADY_OPEN":
            self.stop()
            raise CommunicationFailed("Connection is already open.")

        elif message and message[0] == b"STORING_DISABLED":
            self.stop()
            raise CommunicationFailed("Data storing is disabled on sender "
                                      "side")

        elif message and message[0] == b"NO_VALID_SIGNAL":
            self.stop()
            raise CommunicationFailed("Connection type is not supported for "
                                      "this kind of sender.")

        return message

    def __get_data_socked_id(self, data_socket_prop):
        """ Determines the local ip, DNS name and socket IDs

        Args:
            data_socket_prop: information about the socket to receive incoming
                              data

                - can be a list of the form [<host>, <port>]
                - can be a <port>, then the local host is used as <host>

        Return:
            socket_id (str): identifier for the address ID (using the DNS name)
                             where it is bind to for data receiving
            socket_bind_id (str): the address ID to bind to for data receiving
                                  (this has to use the ip)
        """

        self.ip = "0.0.0.0"           # TODO use IP of hostname?

        host = ""
        port = ""

        # determine host (may be DNS name) and port
        if data_socket_prop:
            self.log.debug("Specified data_socket_prop: {}"
                           .format(data_socket_prop))

            if type(data_socket_prop) == list:
                if len(data_socket_prop) == 2:
                    host = data_socket_prop[0]
                    port = data_socket_prop[1]
                else:
                    self.log.debug("data_socket_prop={}"
                                   .format(data_socket_prop))
                    raise FormatError("Socket information have to be of the "
                                      "form [<host>, <port>].")
            else:
                host = socket.getfqdn()
                port = str(data_socket_prop)

        elif self.targets:
            if len(self.targets) == 1:
                host, port = self.targets[0][0].split(":")
            else:
                raise FormatError("Multiple possible ports. "
                                  "Please choose which one to use.")
        else:
                raise FormatError("No target specified.")

        # ZMQ transport protocol IPC has a different syntax than TCP
        if self.zmq_protocol == "ipc":
            socket_id = "{}/{}".format(host, port).encode("utf-8")
            socket_bind_id = "{}/{}".format(host, port)

            return socket_id, socket_bind_id

        # determine IP to bind to
        ip_from_host = socket.gethostbyaddr(host)[2]
        if len(ip_from_host) == 1:
            self.ip = ip_from_host[0]

        # determine socket identifier (might use DNS name)
        socket_id = "{}:{}".format(host, port).encode("utf-8")

        # Distinguish between IPv4 and IPv6 addresses
        try:
            socket.inet_aton(self.ip)
            self.log.info("IPv4 address detected: {}.".format(self.ip))
            self.is_ipv6 = False
        except socket.error:
            self.log.info("Address '{}' is not an IPv4 address, "
                          "asume it is an IPv6 address.".format(self.ip))
            self.is_ipv6 = True

        # determine socket identifier to bind to (uses IP)
        socket_bind_id = self.__get_socket_id(self.ip, port)

        return socket_id, socket_bind_id

    def __get_socket_id(self, ip, port):
        """ Determines socket ID for the given port

        If the IP is an IPV6 address the appropriate zeromq syntax is used
        """
        if self.is_ipv6:
            return "[{}]:{}".format(ip, port)
        else:
            return "{}:{}".format(ip, port)

    def start(self,
              data_socket_id=False,
              whitelist=None,
              protocol="tcp",
              data_con_style="bind"):

        if protocol in ["tcp", "ipc"]:
            self.zmq_protocol = protocol
        else:
            raise NotSupported("Protocol {} is not supported."
                               .format(protocol))

        self.data_con_style = data_con_style

        # Receive data only from whitelisted nodes
        if whitelist:
            if type(whitelist) == list:
                self.whitelist = whitelist

                self.auth = ThreadAuthenticator(self.context)
                self.auth.start()
                for host in whitelist:
                    try:
                        if host == "localhost":
                            ip = [socket.gethostbyname(host)]
                        else:
                            hostname, tmp, ip = socket.gethostbyaddr(host)

                        self.log.debug("Allowing host {} ({})"
                                       .format(host, ip[0]))
                        self.auth.allow(ip[0])
                    except socket.gaierror:
                        self.log.error("Could not get IP of host {}. Proceed."
                                       .format(host))
                    except:
                        self.log.error("Error was: ", exc_info=True)
                        raise AuthenticationFailed(
                            "Could not get IP of host {}".format(host))
            else:
                raise FormatError("Whitelist has to be a list of "
                                  "IPs/DNS names")

        socket_bind_id = False
        for t in ["STEAM", "QUERY_NEXT", "NEXUS"]:
            if t in self.started_connections:
                socket_bind_id = self.started_connections[t]["bind_id"]

        if socket_bind_id:
            self.log.info("Reopening already started connection.")
        else:
            socket_id, socket_bind_id = (
                self.__get_data_socked_id(data_socket_id))

        socket_bind_str = "{}://{}".format(self.zmq_protocol, socket_bind_id)

        self.log.debug("socket_id_bind_str={}".format(socket_bind_str))

        ######## data socket ############
        # Socket to retrieve data
        self.data_socket = self.context.socket(zmq.PULL)
        # remember the bind string for reestablishment of the connection
        self.data_socket_con_str = socket_bind_str

        if whitelist:
            self.data_socket.zap_domain = b'global'

        if self.is_ipv6:
            self.data_socket.ipv6 = True
            self.log.debug("Enabling IPv6 socket")

        try:
            if self.data_con_style == "bind":
                self.data_socket.bind(self.data_socket_con_str)
            elif self.data_con_style == "connect":
                self.data_socket.connect(self.data_socket_con_str)
            else:
                raise NotSupported("Connection style '{}' is not supported.")

            self.log.info("Data socket of type {} started ({}) for '{}'"
                          .format(self.connection_type,
                                  data_con_style,
                                  self.data_socket_con_str))
        except:
            self.log.error("Failed to start Socket of type {} ({}): '{}'"
                           .format(self.connection_type,
                                   data_con_style,
                                   self.data_socket_con_str),
                           exc_info=True)
            raise

        self.poller.register(self.data_socket, zmq.POLLIN)
        #####################################

        if self.connection_type in ["QUERY_NEXT", "QUERY_METADATA"]:

            ########## request socket ###########
            # An additional socket is needed to establish the data retriving
            # mechanism
            self.request_socket = self.context.socket(zmq.PUSH)
            con_str = "tcp://{}:{}".format(self.signal_host,
                                           self.request_port)
            try:
                self.request_socket.connect(con_str)
                self.log.info("Request socket started (connect) for '{}'"
                              .format(con_str))
            except:
                self.log.error("Failed to start request socket (connect):"
                               " '{}'".format(con_str), exc_info=True)
                raise
            #####################################

            self.started_connections["QUERY_NEXT"] = {
                "id": socket_id,
                "bind_id": socket_bind_id
            }

        elif self.connection_type in ["NEXUS"]:

            ####### file operation socket #######
            # Reuse status check socket to get signals to open and close
            # nexus files
            self.setopt("status_check")
            #####################################

            ######### control socket ###########
            # Socket to retrieve control signals from control API
            if not os.path.exists(self.ipc_dir):
                os.makedirs(self.ipc_dir)

            self.control_socket = self.context.socket(zmq.PULL)
            control_con_str = (
                "ipc://{}/{}_{}".format(self.ipc_dir,
                                        self.current_pid,
                                        "control_API")
            )
            try:
                self.control_socket.bind(control_con_str)
                self.log.info("Internal controlling socket started (bind) for"
                              " '{}'".format(control_con_str))
            except:
                self.log.error("Failed to start internal controlling socket "
                               "(bind): '{}'".format(control_con_str),
                               exc_info=True)

            self.poller.register(self.control_socket, zmq.POLLIN)
            #####################################

            self.started_connections["NEXUS"] = {
                "id": socket_id,
                "bind_id": socket_bind_id
            }
        else:
            self.started_connections["STREAM"] = {
                "id": socket_id,
                "bind_id": socket_bind_id
            }

    def setopt(self, option, value=None):
        """
        Args
            option (str):
                "file_op": enable and configure socket to use for receiving
                           file operation commands for NEXUS use case
                "status_check": enable and configure socket to use for status
                                check requests
                "confirmation": enable and configure socket to use for
                                confirmation that individual data packets where
                                handled without problems
            value (optional):
                - int: port to be used for setup of specified option
                - list of len 2: ip and port to be used for setup of specified
                                 option [<ip>, <port>]
                - list of len 3: ip and port to be used for setup of specified
                                 option [<protocol>, <ip>, <port>]
        """
        if option == "status_check":
            # TODO create Thread which handles this asynchroniously
            if self.status_check_socket is not None:
                self.log.error("Status check is already enabled (used port: "
                               "{})".format(self.status_check_port))
                return

            self.status_check_protocol = "tcp"
            self.status_check_ip = self.ip

            if value is not None:
                if type(value) == list:
                    if len(value) == 2:
                        self.status_check_ip = value[0]
                        self.status_check_port = value[1]
                    elif len(value) == 3:
                        self.status_check_protocol = value[0]
                        self.status_check_ip = value[1]
                        self.status_check_port = value[2]
                    else:
                        self.log.debug("value={}".format(value))
                        msg = ("Socket information have to be of the form"
                               "[<host>, <port>].")
                        raise FormatError(msg)
                else:
                    self.status_check_port = value

            bind_str = "{}://{}".format(
                self.status_check_protocol,
                self.__get_socket_id(self.status_check_ip,
                                     self.status_check_port))

            ######## status check socket ########
            # socket to get signals to get status check requests. this socket
            # is also used to get signals to open and close nexus files
            self.status_check_socket = self.context.socket(zmq.REP)

            if self.is_ipv6:
                self.status_check_socket.ipv6 = True
                self.log.debug("enabling ipv6 socket for status_check_socket")
            try:
                self.status_check_socket.bind(bind_str)
                self.log.info("Start status check socket (bind) for '{}'"
                              .format(bind_str))
            except:
                self.log.error("Failed to start status check socket (bind) "
                               "for '{}'".format(bind_str), exc_info=True)

            self.poller.register(self.status_check_socket, zmq.POLLIN)
            #####################################

        elif option == "file_op":
            if self.file_op_socket is not None:
                self.log.error("File operation is already enabled (used port: "
                               "{})".format(self.file_op_port))
                return

            self.file_op_protocol = "tcp"
            self.file_op_ip = self.ip

            if value is not None:
                if type(value) == list:
                    if len(value) == 2:
                        self.file_op_ip = value[0]
                        self.file_op_port = value[1]
                    elif len(value) == 3:
                        self.file_op_protocol = value[0]
                        self.file_op_ip = value[1]
                        self.file_op_port = value[2]
                    else:
                        self.log.debug("value={}".format(value))
                        msg = ("Socket information have to be of the form"
                               "[<host>, <port>].")
                        raise FormatError(msg)
                else:
                    self.file_op_port = value

            bind_str = "{}://{}".format(
                self.file_op_protocol,
                self.__get_socket_id(self.file_op_ip,
                                     self.file_op_port))

            ######## status check socket ########
            # socket to get signals to get status check requests. this socket
            # is also used to get signals to open and close nexus files
            self.file_op_socket = self.context.socket(zmq.REP)

            if self.is_ipv6:
                self.file_op_socket.ipv6 = True
                self.log.debug("enabling ipv6 socket for file_op_socket")
            try:
                self.file_op_socket.bind(bind_str)
                self.log.info("Start status check socket (bind) for '{}'"
                              .format(bind_str))
            except:
                self.log.error("Failed to start status check socket (bind) "
                               "for '{}'".format(bind_str), exc_info=True)

            self.poller.register(self.file_op_socket, zmq.POLLIN)
            #####################################

        elif option == "confirmation":
            if self.confirmation_socket is not None:
                self.log.error(
                    "Confirmation is already enabled (used port: {})"
                    .format(self.confirmation_port)
                )
                return

            self.confirmation_protocol = "tcp"
            self.confirmation_ip = self.ip

            if value is not None:
                if type(value) == list:
                    if len(value) == 2:
                        self.confirmation_ip = value[0]
                        self.confirmation_port = value[1]
                    elif len(value) == 3:
                        self.confirmation_protocol = value[0]
                        self.confirmation_ip = value[1]
                        self.confirmation_port = value[2]
                    else:
                        self.log.debug("value={}".format(value))
                        msg = ("Socket information have to be of the form"
                               "[<host>, <port>].")
                        raise FormatError(msg)
                else:
                    self.confirmation_port = value

            con_str = "{}://{}".format(
                self.confirmation_protocol,
                self.__get_socket_id(self.confirmation_ip,
                                     self.confirmation_port))

            ######## confirmation socket ########
            # to send the a confirmation to the sender that the data packages
            # was stored successfully
            self.confirmation_socket = self.context.socket(zmq.PUB)

            try:
                self.confirmation_socket.bind(con_str)
                self.log.info("Start confirmation_socket (bind) for '{}'"
                              .format(con_str))
            except:
                self.log.error(
                    "Failed to start confirmation socket (bind) for '{}'"
                    .format(con_str),
                    exc_info=True
                )
            #####################################
        else:
            raise NotSupported("Option {} is not supported".format(option))

    def register(self, whitelist):

        # to add hosts to the whitelist the authentcation thread has to be
        # stopped and the socket closed
        self.log.debug("Shutting down auth thread and data_socket")
        self.auth.stop()
        self.poller.unregister(self.data_socket)
        self.data_socket.close()

        self.log.debug("Starting down auth thread")
        self.auth = ThreadAuthenticator(self.context)
        self.auth.start()

        for host in whitelist:
            try:
                # convert DNS names to IPs
                if host == "localhost":
                    ip = [socket.gethostbyname(host)]
                else:
                    # returns (hostname, aliaslist, ipaddrlist)
                    ip = socket.gethostbyaddr(host)[2]

                self.log.debug("Allowing host {} ({})".format(host, ip[0]))
                self.auth.allow(ip[0])
            except socket.gaierror:
                self.log.error("Could not get IP of host {}. Proceed."
                               .format(host))
            except:
                self.log.error("Error was: ", exc_info=True)
                msg = "Could not get IP of host {}".format(host)
                raise AuthenticationFailed(msg)

        # Recreate the socket (not with the new whitelist enables)
        self.log.debug("Starting down data_socket")

        ########## data socket ###########
        self.data_socket = self.context.socket(zmq.PULL)
        self.data_socket.zap_domain = b'global'

        if self.is_ipv6:
            self.data_socket.ipv6 = True
            self.log.debug("Enabling IPv6 socket")

        try:
            self.data_socket.bind(self.data_socket_con_str)
            self.log.info(
                "Data socket of type {} started (bind) for '{}'"
                .format(self.connection_type, self.data_socket_con_str)
            )
        except:
            self.log.error(
                "Failed to start Socket of type {} (bind): '{}'"
                .format(self.connection_type, self.data_socket_con_str),
                exc_info=True
            )
            raise

        self.poller.register(self.data_socket, zmq.POLLIN)
        #####################################

    def read(self, callback_params, open_callback, read_callback,
             close_callback):

        if (not self.connection_type == "NEXUS"
                or "NEXUS" not in self.started_connections):
            msg = ("Wrong connection type (current: {}) or session"
                   " not started.".format(self.connection_type))
            raise UsageError(msg)

        self.callback_params = callback_params
        self.open_callback = open_callback
        self.read_callback = read_callback
        self.close_callback = close_callback

        run_loop = True

        while run_loop:
            self.log.debug("polling")
            try:
                socks = dict(self.poller.poll())
            except:
                self.log.error("Could not poll for new message")
                raise

            # received signal from status check socket
            # (socket is also used for nexus signals)
            if (self.status_check_socket in socks
                    and socks[self.status_check_socket] == zmq.POLLIN):
                self.log.debug("status_check_socket is polling")

                message = self.status_check_socket.recv_multipart()
                self.log.debug("status_check_socket recv: {}".format(message))

                # request to close the open file
                if message[0] == b"CLOSE_FILE":
                    if self.all_close_recvd:
                        self.status_check_socket.send_multipart(message)
                        logging.debug("status_check_socket (file operation) "
                                      "send: {}".format(message))
                        self.all_close_recvd = False

                        self.close_callback(self.callback_params,
                                            message)
                        break
                    else:
                        self.reply_to_signal = message

                # request to open a new file
                elif message[0] == b"OPEN_FILE":
                    self.status_check_socket.send_multipart(message)
                    self.log.debug("status_check_socket (file operation) "
                                   "send: {}".format(message))

                    try:
                        self.open_callback(self.callback_params, message[1])
                        self.file_opened = True
                    except:
                        self.status_check_socket.send_multipart([b"ERROR"])
                        self.log.error("Not supported message received")

                # received not supported signal
                else:
                    self.status_check_op_socket.send_multipart([b"ERROR"])
                    self.log.error("Not supported message received")

            # received data
            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):
                self.log.debug("data_socket is polling")

                try:
                    multipart_message = self.data_socket.recv_multipart()
#                    self.log.debug("multipart_message={}"
#                                    .format(multipart_message[:100]))
                except:
                    self.log.error("Could not receive data due to unknown "
                                   "error.", exc_info=True)

                if multipart_message[0] == b"ALIVE_TEST":
                    continue

                if len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
#                    self.log.debug("multipart_message={}"
#                                   .format(multipart_message[:100]))
                    # TODO return errorcode

                try:
                    run_loop = self.__react_on_message(multipart_message)
                except KeyboardInterrupt:
                    self.log.debug("Keyboard interrupt detected. "
                                   "Stopping to receive.")
                    raise
                except:
                    self.log.error("Unknown error while receiving files. "
                                   "Need to abort.", exc_info=True)
#                    raise Exception("Unknown error while receiving files. "
#                                   "Need to abort.")

            # received control signal
            if (self.control_socket in socks
                    and socks[self.control_socket] == zmq.POLLIN):
                self.log.debug("control_socket is polling")
                self.control_socket.recv()
#                self.log.debug("Control signal received. Stopping.")
                raise Exception("Control signal received. Stopping.")

    def __react_on_message(self, multipart_message):

        if multipart_message[0] == b"CLOSE_FILE":
            try:
                # filename = multipart_message[1]
                id = multipart_message[2]
            except:
                self.log.error("Could not extract id from the "
                               "multipart-message", exc_info=True)
                self.log.debug("multipart-message: {}"
                               .format(multipart_message),
                               exc_info=True)
                raise

            self.recvd_close_from.append(id)
            self.log.debug("Received close-file signal from "
                           "DataDispatcher-{}".format(id))

            # get number of signals to wait for
            if not self.number_of_streams:
                self.number_of_streams = int(id.split("/")[1])

            # have all signals arrived?
            self.log.debug("self.recvd_close_from={}, "
                           "self.number_of_streams={}"
                           .format(self.recvd_close_from,
                                   self.number_of_streams))
            if len(self.recvd_close_from) == self.number_of_streams:
                self.log.info("All close-file-signals arrived")
                if self.reply_to_signal:
                    self.status_check_socket.send_multipart(
                        self.reply_to_signal)
                    self.log.debug("status_check_socket (file operation) "
                                   "send: {}".format(self.reply_to_signal))

                    self.reply_to_signal = False
                    self.recvd_close_from = []

                    self.close_callback(self.callback_params,
                                        multipart_message)
                    return False
                else:
                    self.all_close_recvd = True

            else:
                self.log.info("self.recvd_close_from={}, "
                              "self.number_of_streams={}"
                              .format(self.recvd_close_from,
                                      self.number_of_streams))

        else:
            # extract multipart message
            try:
                metadata = json.loads(multipart_message[0].decode("utf-8"))
            except:
                # json.dumps of None results in 'null'
                if multipart_message[0] != 'null':
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    self.log.debug("multipartmessage[0] = {}"
                                   .format(multipart_message[0]),
                                   exc_info=True)
                metadata = None

            # TODO validate multipart_message
            # (like correct dict-values for metadata)

            try:
                payload = multipart_message[1]
            except:
                self.log.warning("An empty file was received within the "
                                 "multipart-message", exc_info=True)
                payload = None

            self.read_callback(self.callback_params, [metadata, payload])

        return True

    def get_chunk(self, timeout=None):
        """
        Receives or queries for chunks of the new files depending on the
        connection initialized.

        Args:
            timout (optional): The time (in ms) to wait for new messages to
                               come before aborting.

        Returns:
            Either
            the newest data chunk
                (if connection type "QUERY_NEXT" or "STREAM" was choosen)
            the metadata of the newest data chunk
                (if connection type "QUERY_METADATA" or "STREAM_METADATA" was
                choosen)

        """

        # query_metadata and stream_metadata are covered with this as well
        if ("STREAM" not in self.started_connections
                and "QUERY_NEXT" not in self.started_connections):
            self.log.error("Could not communicate, no connection was "
                           "initialized.")
            return None, None

        if "QUERY_NEXT" in self.started_connections:

            send_message = [b"NEXT",
                            self.started_connections["QUERY_NEXT"]["id"]]
            try:
                self.request_socket.send_multipart(send_message)
            except Exception:
                self.log.error("Could not send request to request_socket",
                               exc_info=True)
                return None, None

        timestamp = time.time()

        while True:
            # receive data
            if timeout:
                try:
                    socks = dict(self.poller.poll(timeout))
                except:
                    if self.stopped_everything:
                        raise KeyboardInterrupt
                    else:
                        self.log.error("Could not poll for new message")
                        raise
            else:
                try:
                    socks = dict(self.poller.poll())
                except:
                    if self.stopped_everything:
                        self.log.debug("Stopping poller")
                        raise KeyboardInterrupt
                    else:
                        self.log.error("Could not poll for new message")
                        raise

            # received signal from status check socket
            if (self.status_check_socket in socks
                    and socks[self.status_check_socket] == zmq.POLLIN):

                message = self.status_check_socket.recv_multipart()
#                self.log.debug("status_check_socket recv: {0}"
#                               .format(message))

                # request to close the open file
                if message[0] == b"STATUS_CHECK":
                    self.status_check_socket.send_multipart(self.status)
#                    logging.debug("status_check_op_socket send: {0}"
#                                  .format(self.status))
                elif message[0] == b"RESET_STATUS":
                    self.status = [b"OK"]
                    self.log.debug("Reset request received. Status changed "
                                   "to: {}".format(self.status))
                    self.status_check_socket.send_multipart(self.status)
                # received not supported signal
                else:
                    self.status_check_socket.send_multipart([b"ERROR"])
                    self.log.error("Not supported message received")

            # if there was a response
            if (self.data_socket in socks
                    and socks[self.data_socket] == zmq.POLLIN):

                try:
                    multipart_message = self.data_socket.recv_multipart()
                except:
                    self.log.error("Receiving data..failed.", exc_info=True)
                    return [None, None]

                if multipart_message[0] == b"ALIVE_TEST":
                    if timeout:
                        # measure how much time is left from the timeout value
                        # timeout is in ms, timestamp in s
                        timeout -= (time.time() - timestamp) * 1000
                    if timeout < 0:
                        return [None, None]
                    else:
                        continue
                elif len(multipart_message) < 2:
                    self.log.error("Received mutipart-message is too short. "
                                   "Either config or file content is missing.")
                    self.log.debug("multipart_message={}"
                                   .format(multipart_message[:100]))
                    return [None, None]

                # extract multipart message
                try:
                    metadata = json.loads(multipart_message[0].decode("utf-8"))
                except:
                    self.log.error("Could not extract metadata from the "
                                   "multipart-message.", exc_info=True)
                    metadata = None

                # TODO validate multipart_message
                # (like correct dict-values for metadata)

                try:
                    payload = multipart_message[1]
                except:
                    self.log.warning("An empty file was received within the "
                                     "multipart-message", exc_info=True)
                    payload = None

                return [metadata, payload]

            # no responce was received
            else:
                # self.log.warning("Could not receive data in the given time.")

                if "QUERY_NEXT" in self.started_connections:
                    try:
                        self.request_socket.send_multipart(
                            [b"CANCEL",
                             self.started_connections["QUERY_NEXT"]["id"]])
                    except Exception:
                        self.log.error("Could not cancel the next query",
                                       exc_info=True)

                return [None, None]

    def check_file_closed(self, payload, m):
        # Either the message is smaller than than expected (last chunk)
        # or the size of the origin file was a multiple of the
        # chunksize and this is the last expected chunk (chunk_number
        # starts with 0)
        if len(payload) < m["chunksize"] \
            or (m["filesize"] % m["chunksize"] == 0
                and m["filesize"] / m["chunksize"] == m["chunk_number"] + 1):
            return True
        else:
            return False

    def get(self, timeout=None):
        """
        Receives or queries for new files depending on the connection
        initialized.

        Args:
            timout (optional): The time (in ms) to wait for new messages to
                               come before stop waiting.

        Returns:
            Either
            the newest file
                (if connection type "QUERY_NEXT" or "STREAM" was choosen)
            the metadata of the newest file
                (if connection type "QUERY_METADATA" or "STREAM_METADATA" was
                choosen)

        """
        run_loop = True

        all_data = []
        all_metadata = None

        # save all chunks to file
        while run_loop:

            try:
                # timeout (in ms) to be able to react on system signals
                [metadata, payload] = self.get_chunk(timeout=timeout)
            except KeyboardInterrupt:
                raise
            except:
                if self.stopped_everything:
                    break
                else:
                    self.log.error("Getting data failed.", exc_info=True)
                    raise

            # the metadata is the same for all chunks (only exception chunk_number)
            if all_metadata is None:
                all_metadata = metadata

            all_data.append(copy.deepcopy(payload))

            if self.check_file_closed(payload, metadata):
                # indicates end of file. Leave loop
                self.log.info("New file with modification time {} received "
                              .format(metadata["file_mod_time"]))

                try:
                    # highlight that the metadata does not correspond to only one
                    # chunk anymore
                    all_metadata["chunk_number"] = None

                    # merge the data again
                    all_data = b"".join(all_data)
                except:
                    self.log.error("Something went wrong when merging chunks",
                                   exc_info=True)
                    raise

                return all_metadata, all_data

    def store_data_chunk(self,
                         descriptors,
                         filepath,
                         payload,
                         base_path,
                         metadata):
        """Writes the data chunk into a file.

        Args:
            descriptors: All open file descriptors
            filepath: The path of the file to which the chunk should be stored.
            payload: The data to store.
            base_path: The base path under which the file should be stored
            metadata: The metadata recceived together with the data.
        """
        # append payload to file
        try:
            descriptors[filepath].write(payload)
        # file was not open
        except KeyError:
            try:
                descriptors[filepath] = open(filepath, "wb")
                # write data
                descriptors[filepath].write(payload)
            except IOError as e:
                # errno.ENOENT == "No such file or directory"
                if e.errno == errno.ENOENT:
                    try:
                        rel_path = metadata["relative_path"]

                        # do not create directories defined as immutable,
                        # e.g.commissioning, current and local
                        if (self.dirs_not_to_create is not None
                                and rel_path.startswith(self.dirs_not_to_create)):
                            self.log.error("Unable to write file '{}': "
                                           "Directory {} is not available"
                                           .format(filepath,
                                                   metadata["relative_path"]))
                            target_path = None
                            raise

                        target_path = generate_filepath(base_path,
                                                        metadata,
                                                        add_filename=False)
                        os.makedirs(target_path)

                        descriptors[filepath] = open(filepath, "wb")
                        self.log.info("New target directory created: {}"
                                      .format(target_path))
                        # write data
                        descriptors[filepath].write(payload)
                    except:
                        self.log.error("Unable to save payload to file: '{}'"
                                       .format(filepath), exc_info=True)
                        self.log.debug("target_path:{}".format(target_path))
                        raise
                else:
                    self.log.error("Failed to append payload to file: '{}'"
                                   .format(filepath), exc_info=True)
                    raise
            except:
                self.log.error("Failed to append payload to file: '{}'"
                               .format(filepath), exc_info=True)
                raise

            if ("confirmation_required" in metadata
                    and metadata["confirmation_required"]):
                file_id = generate_file_identifier(metadata)
                # send confirmation
                try:
                    topic = metadata["confirmation_required"].encode()
                    #topic = b"test"
                    message = [topic, file_id.encode("utf-8")]
                    self.confirmation_socket.send_multipart(message)

                    self.log.debug("Sending confirmation for chunk {} of "
                                   "file '{}' to {}"
                                   .format(metadata["chunk_number"],
                                           file_id,
                                           topic))
                except:
                    if self.confirmation_socket is None:
                        self.log.error("Correct data handling is requested to "
                                       "be confirmed. Please enable option "
                                       "'confirmation'")
                        raise UsageError("Option 'confirmation' is not "
                                         "enabled")
                    else:
                        raise

        except KeyboardInterrupt:
            # save the data in the file before quitting
            self.log.debug("KeyboardInterrupt received while writing data")
            raise
        except:
            self.log.error("Failed to append payload to file: '{0}'"
                           .format(filepath), exc_info=True)
            raise

        if self.check_file_closed(payload, metadata):
            # indicates end of file. Leave loop
            try:
                descriptors[filepath].close()
                del descriptors[filepath]

                self.log.info("New file with modification time {} received "
                              "and saved: {}"
                              .format(metadata["file_mod_time"], filepath))
            except:
                self.log.error("File could not be closed: {}"
                               .format(filepath), exc_info=True)
                raise
            return False
        else:
            return True

    def store(self, target_base_path, timeout=None):
        """Writes all data belonging to one file to disc.

        Args:
            target_base_path: The base path under which the file possible
                              subdirectories should be created.
            timout (optional): The time (in ms) to wait for new messages to
                               come before aborting.
        """

        run_loop = True

        # save all chunks to file
        while run_loop:

            try:
                # timeout (in ms) to be able to react on system signals
                [payload_metadata, payload] = self.get_chunk(timeout)
            except KeyboardInterrupt:
                raise
            except:
                if self.stopped_everything:
                    break
                else:
                    self.log.error("Getting data failed.", exc_info=True)
                    raise

            if payload_metadata is not None and payload is not None:

                # generate target filepath
                target_filepath = generate_filepath(target_base_path,
                                                    payload_metadata)
                self.log.debug("New chunk for file {} received."
                               .format(target_filepath))

                # TODO: save message to file using a thread (avoids blocking)
                try:
                    run_loop = self.store_data_chunk(
                        descriptors=self.file_descriptors,
                        filepath=target_filepath,
                        payload=payload,
                        base_path=target_base_path,
                        metadata=payload_metadata
                    )
                    # for testing
#                    try:
#                        a = 5/0
#                    except:
#                        # returns a tuple (type, value, traceback)
#                        exc_type, exc_value, _ = sys.exc_info()
#
#                        self.status = [b"ERROR",
#                                       str(exc_type).encode("utf-8"),
#                                       str(exc_value).encode("utf-8")]
#                        self.log.debug("Status changed to: {}"
#                                       .format(self.status))
                except:
                    self.log.debug("Stopping data storing loop")

                    # returns a tuple (type, value, traceback)
                    exc_type, exc_value, _ = sys.exc_info()

                    self.log.error(exc_value)

                    self.status = [b"ERROR",
                                   str(exc_type).encode("utf-8"),
                                   str(exc_value).encode("utf-8")]
                    self.log.debug("Status changed to: {}"
                                   .format(self.status))

                    break
            else:
                # self.log.debug("No data received. Break loop")
                break

    def stop(self):
        """
        * Close open file handler to prevent file corruption
        * Send signal that the application is quitting
        * Close ZMQ connections
        * Destroying context

        """

        # Close open file handler to prevent file corruption
        for target_filepath in list(self.file_descriptors.keys()):
            self.file_descriptors[target_filepath].close()
            del self.file_descriptors[target_filepath]

        # Send signal that the application is quitting
        if self.signal_socket and self.signal_exchanged:
            self.log.info("Sending close signal")
            signal = None
            if ("STREAM" in self.started_connections
                    or (b"STREAM" in self.signal_exchanged)):
                signal = b"STOP_STREAM"
            elif ("QUERY_NEXT" in self.started_connections
                    or (b"QUERY" in self.signal_exchanged)):
                signal = b"STOP_QUERY_NEXT"

            self.__send_signal(signal)
            # TODO: need to check correctness of signal?
#            message = self.__send_signal(signal)

            try:
                del self.started_connections["STREAM"]
            except KeyError:
                pass
            try:
                del self.started_connections["QUERY_NEXT"]
            except KeyError:
                pass

        # unregister sockets from poller
        self.poller = None


        # Close ZMQ connections
        try:
            if self.signal_socket is not None:
                self.log.info("closing signal_socket...")
                self.signal_socket.close(linger=0)
                self.signal_socket = None
            if self.data_socket is not None:
                self.log.info("closing data_socket...")
                self.data_socket.close()
                self.data_socket = None
            if self.request_socket is not None:
                self.log.info("closing request_socket...")
                self.request_socket.close(linger=0)
                self.request_socket = None
            if self.status_check_socket is not None:
                self.log.info("closing status_check_socket...")
                self.status_check_socket.close(linger=0)
                self.status_check_socket = None
            if self.confirmation_socket is not None:
                self.log.info("closing confirmation_socket...")
                self.confirmation_socket.close(linger=0)
                self.confirmation_socket = None
            if self.control_socket is not None:
                self.log.info("closing control_socket...")
                self.control_socket.close(linger=0)
                self.control_socket = None

                control_con_str = ("{}/{}_{}"
                                   .format(self.ipc_dir,
                                           self.current_pid,
                                           "control_API"))
                try:
                    os.remove(control_con_str)
                    self.log.debug("Removed ipc socket: {}"
                                   .format(control_con_str))
                except OSError:
                    self.log.warning("Could not remove ipc socket: {}"
                                     .format(control_con_str))
                except:
                    self.log.warning("Could not remove ipc socket: {}"
                                     .format(control_con_str), exc_info=True)
        except:
            self.log.error("closing ZMQ Sockets...failed.", exc_info=True)

        if self.auth:
            try:
                self.auth.stop()
                self.auth = None
                self.log.info("Stopping authentication thread...done.")
            except:
                self.log.error("Stopping authentication thread...done.",
                               exc_info=True)

        for target in self.file_descriptors:
            self.file_descriptors[target].close()
            self.log.warning("Not all chunks were received for file {}"
                             .format(target))

        if self.file_descriptors:
            self.file_descriptors = dict()

        # if the context was created inside this class,
        # it has to be destroyed also within the class
        if not self.ext_context and self.context:
            try:
                self.log.info("Closing ZMQ context...")
                self.context.destroy(0)
                self.context = None
                self.log.info("Closing ZMQ context...done.")
            except:
                self.log.error("Closing ZMQ context...failed.", exc_info=True)

        self.stopped_everything = True

    def force_stop(self, targets):

        if type(targets) != list:
            self.stop()
            raise FormatError("Argument 'targets' must be list.")

        if not self.context:
            self.context = zmq.Context()
            self.ext_context = False

        signal = None
        # Signal exchange
        if self.connection_type == "STREAM":
            signal_port = self.signal_port
            signal = b"STOP_STREAM"
        elif self.connection_type == "STREAM_METADATA":
            signal_port = self.signal_port
            signal = b"STOP_STREAM_METADATA"
        elif self.connection_type == "QUERY_NEXT":
            signal_port = self.signal_port
            signal = b"STOP_QUERY_NEXT"
        elif self.connection_type == "QUERY_METADATA":
            signal_port = self.signal_port
            signal = b"STOP_QUERY_METADATA"

        self.log.debug("Create socket for signal exchange...")

        if self.signal_host and not self.signal_socket:
            self.__create_signal_socket(signal_port)
        elif not self.signal_host:
            self.stop()
            raise ConnectionFailed("No host to send signal to specified.")

        self.__set_targets(targets)

        message = self.__send_signal(signal)

        # if there was no response or the response was of the wrong format,
        # the receiver should be shut down
        if message and message[0].startswith(signal):
            self.log.info("Received confirmation ...")

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
