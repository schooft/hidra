#!/usr/bin/env python

from __future__ import unicode_literals

import argparse
import zmq
import zmq.devices
import os
import logging
import json
import time
from multiprocessing import Process, freeze_support, Queue
import threading
import signal
import setproctitle
import tempfile
import socket

from signalhandler import SignalHandler
from taskprovider import TaskProvider
from datadispatcher import DataDispatcher

from __init__ import BASE_PATH
from logutils.queue import QueueHandler
import helpers
from _version import __version__

CONFIG_PATH = os.path.join(BASE_PATH, "conf")

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def argument_parsing():
    default_config = os.path.join(CONFIG_PATH, "datamanager.conf")

    supported_ed_types = ["inotifyx_events",
                          "watchdog_events",
                          "zmq_events",
                          "http_events"]

    supported_df_types = ["file_fetcher",
                          "zmq_fetcher",
                          "http_fetcher"]

    ##################################
    #   Get command line arguments   #
    ##################################

    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        help="Location of the configuration file")

    parser.add_argument("--log_path",
                        type=str,
                        help="Path where the logfile will be created")
    parser.add_argument("--log_name",
                        type=str,
                        help="Filename used for logging")
    parser.add_argument("--log_size",
                        type=int,
                        help="File size before rollover in B (linux only)")
    parser.add_argument("--verbose",
                        help="More verbose output",
                        action="store_true")
    parser.add_argument("--onscreen",
                        type=str,
                        help="Display logging on screen "
                             "(options are CRITICAL, ERROR, WARNING, "
                             "INFO, DEBUG)",
                        default=False)

    parser.add_argument("--procname",
                        type=str,
                        help="Name with which the service should be running")

    parser.add_argument("--ext_ip",
                        type=str,
                        help="IP of the interface to bind to for external "
                             "communication")

    # SignalHandler config

    parser.add_argument("--com_port",
                        type=str,
                        help="Port number to receive signals")
    parser.add_argument("--whitelist",
                        nargs='+',
                        help="List of hosts allowed to connect")

    parser.add_argument("--request_port",
                        type=str,
                        help="ZMQ port to get new requests")
    parser.add_argument("--request_fw_port",
                        type=str,
                        help="ZMQ port to forward requests")
    parser.add_argument("--control_pub_port",
                        type=str,
                        help="Port number to publish control signals")
    parser.add_argument("--control_sub_port",
                        type=str,
                        help="Port number to receive control signals")

    # EventDetector config

    parser.add_argument("--event_detector_type",
                        type=str,
                        help="Type of event detector to use")
    parser.add_argument("--fix_subdirs",
                        type=str,
                        help="Subdirectories to be monitored and to store the "
                             "data to (only needed if event detector is "
                             "inotifyx_events or watchdog_events "
                             "and data fetcher is file_fetcher)")

    parser.add_argument("--monitored_dir",
                        type=str,
                        help="Directory to be monitor for changes; inside "
                             "this directory only the specified "
                             "subdirectories are monitred (only needed if "
                             "event detector is inotifyx_events or "
                             "watchdog_events)")
    parser.add_argument("--monitored_events",
                        type=str,
                        help="Event type of files (options are: "
                             "IN_CLOSE_WRITE, IN_MOVED_TO, ...) and the "
                             "formats to be monitored, files in an other "
                             "format will be be neglected (needed if "
                             "event detector is inotifyx_events or "
                             "watchdog_events)")

    parser.add_argument("--history_size",
                        type=int,
                        help="Number of events stored to look for doubles "
                             "(needed if event detector is "
                             "inotifyx_events)")

    parser.add_argument("--use_cleanup",
                        help="Flag describing if a clean up thread which "
                             "regularly checks if some files were missed "
                             "should be activated (needed if event detector "
                             "is inotifyx_events)",
                        choices=["True", "False"])

    parser.add_argument("--action_time",
                        type=float,
                        help="Intervall time (in seconds) used for clea nup "
                             "(only needed if event_detector_type is "
                             "inotifyx_events)")

    parser.add_argument("--time_till_closed",
                        type=float,
                        help="Time (in seconds) since last modification after "
                             "which a file will be seen as closed (only "
                             "needed if event_detector_type is "
                             "inotifyx_events (for clean up) or "
                             "watchdog_events)")

    parser.add_argument("--event_det_port",
                        type=str,
                        help="ZMQ port to get events from (only needed if "
                             "event_detector_type is zmq_events)")

    parser.add_argument("--eiger_ip",
                        type=str,
                        help="IP of the Eiger detector (only needed if "
                             "event_detector_type is http_events)")
    parser.add_argument("--eiger_api_version",
                        type=str,
                        help="API version of the Eiger detector (only needed "
                             "if event_detector_type is http_events)")

    # DataFetcher config

    parser.add_argument("--data_fetcher_type",
                        type=str,
                        help="Module with methods specifying how to get the "
                             "data)")
    parser.add_argument("--data_fetcher_port",
                        type=str,
                        help="If 'zmq_fetcher' is specified as "
                             "data_fetcher_type it needs a port to listen to)")

    parser.add_argument("--use_data_stream",
                        help="Enable ZMQ pipe into storage system (if set to "
                             "false: the file is moved into the "
                             "local_target)",
                        choices=["True", "False"])
    parser.add_argument("--data_stream_target",
                        type=str,
                        help="Fixed host and port to send the data to with "
                             "highest priority (only active if "
                              "use_data_stream is set)")
    parser.add_argument("--number_of_streams",
                        type=int,
                        help="Number of parallel data streams)")
    parser.add_argument("--chunksize",
                        type=int,
                        help="Chunk size of file-parts getting send via ZMQ)")

    parser.add_argument("--router_port",
                        type=str,
                        help="ZMQ-router port which coordinates the "
                             "load-balancing to the worker-processes)")

    parser.add_argument("--local_target",
                        type=str,
                        help="Target to move the files into")

    parser.add_argument("--store_data",
                        help="Flag describing if the data should be stored in "
                             "local_target (needed if data_fetcher_type is "
                             "file_fetcher or http_fetcher)",
                        choices=["True", "False"])
    parser.add_argument("--remove_data",
                        help="Flag describing if the files should be removed "
                             "from the source (needed if data_fetcher_type is "
                             "http_fetcher)",
                        choices=["True", "False", "with_confirmation"])

    arguments = parser.parse_args()
    arguments.config_file = arguments.config_file or default_config

    # check if config_file exist
    helpers.check_existance(arguments.config_file)

    ##################################################
    # Get arguments from config file and comand line #
    ##################################################

    params = helpers.set_parameters(arguments.config_file, arguments)

    ##################################
    #     Check given arguments      #
    ##################################

    # check if logfile is writable
    params["log_file"] = os.path.join(params["log_path"], params["log_name"])
    helpers.check_writable(params["log_file"])

    # check if the event_detector_type is supported
    helpers.check_type(
        params["event_detector_type"],
        supported_ed_types,
        "Event detector")

    # check if the data_fetcher_type is supported
    helpers.check_type(
        params["data_fetcher_type"],
        supported_df_types,
        "Data fetcher")

    # check if directories exist
    helpers.check_existance(params["log_path"])
    if params["monitored_dir"]:
        helpers.check_existance(params["monitored_dir"])
        helpers.check_all_sub_dir_exist(
            params["monitored_dir"],
            params["fix_subdirs"])
    if params["store_data"]:
        helpers.check_existance(params["local_target"])
        helpers.check_all_sub_dir_exist(
            params["local_target"],
            params["fix_subdirs"])

    if params["use_data_stream"]:
        helpers.check_ping(params["data_stream_targets"][0][0])

    return params


class DataManager():
    def __init__(self, log_queue=None):
        self.device = None
        self.control_pub_socket = None
        self.test_socket = None
        self.context = None
        self.ext_log_queue = True
        self.log = None
        self.log_queue_listener = None

        self.localhost = "127.0.0.1"

        self.current_pid = os.getpid()

        try:
            self.params = argument_parsing()
        except:
            self.log = logging
            self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
            raise

        if log_queue:
            self.log_queue = log_queue
            self.ext_log_queue = True
        else:
            self.ext_log_queue = False

            # Get queue
            self.log_queue = Queue(-1)

            # Get the log Configuration for the lisener
            if self.params["onscreen"]:
                h1, h2 = helpers.get_log_handlers(self.params["log_file"],
                                                  self.params["log_size"],
                                                  self.params["verbose"],
                                                  self.params["onscreen"])

                # Start queue listener using the stream handler above.
                self.log_queue_listener = helpers.CustomQueueListener(
                    self.log_queue, h1, h2)
            else:
                h1 = helpers.get_log_handlers(self.params["log_file"],
                                              self.params["log_size"],
                                              self.params["verbose"],
                                              self.params["onscreen"])

                # Start queue listener using the stream handler above
                self.log_queue_listener = (
                    helpers.CustomQueueListener(self.log_queue, h1))

            self.log_queue_listener.start()

        # Create log and set handler to queue handle
        self.log = self.get_logger(self.log_queue)

        self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
        self.log.info("Configured ipc_path: {0}".format(self.ipc_path))

        # Make ipc_path accessible for modules
        self.params["ipc_path"] = self.ipc_path

        # set process name
        check_passed, _ = helpers.check_config(["procname"],
                                               self.params,
                                               self.log)
        if not check_passed:
            raise Exception("Configuration check failed")
        setproctitle.setproctitle(self.params["procname"])
        self.log.info("Running as {0}".format(self.params["procname"]))

        self.log.info("DataManager started (PID {0})."
                      .format(self.current_pid))

        signal.signal(signal.SIGTERM, self.signal_term_handler)

        if not os.path.exists(self.ipc_path):
            os.mkdir(self.ipc_path)
            # the permission have to changed explicitly because
            # on some platform they are ignored when called within mkdir
            os.chmod(self.ipc_path, 0o777)
            self.log.info("Creating directory for IPC communication: {0}"
                          .format(self.ipc_path))

        # Enable specification via IP and DNS name
        # TODO make this IPv6 compatible
        if self.params["ext_ip"] == "0.0.0.0":
            self.ext_ip = self.params["ext_ip"]
        else:
            self.ext_ip = socket.gethostbyaddr(self.params["ext_ip"])[2][0]

        # Make ipc_path accessible for modules
        self.params["ext_ip"] = self.ext_ip
        # TODO this should not be set here (it belong to the moduls)
        self.params["context"] = None
        self.params["session"] = None

        self.com_port = self.params["com_port"]
        self.request_port = self.params["request_port"]

        self.com_con_id = ("tcp://{0}:{1}"
                           .format(self.ext_ip, self.params["com_port"]))
        self.request_con_id = ("tcp://{0}:{1}"
                               .format(self.ext_ip,
                                       self.params["request_port"]))

        if helpers.is_windows():
            self.log.info("Using tcp for internal communication.")
            self.control_pub_con_id = (
                "tcp://{0}:{1}".format(self.localhost,
                                       self.params["control_pub_port"]))
            self.control_sub_con_id = (
                "tcp://{0}:{1}".format(self.localhost,
                                       self.params["control_sub_port"]))
            self.request_fw_con_id = (
                "tcp://{0}:{1}".format(self.localhost,
                                       self.params["request_fw_port"]))
            self.router_con_id = (
                "tcp://{0}:{1}".format(self.localhost,
                                       self.params["router_port"]))

        else:
            self.log.info("Using ipc for internal communication.")
            self.control_pub_con_id = ("ipc://{0}/{1}_{2}"
                                       .format(self.ipc_path,
                                               self.current_pid,
                                               "controlPub"))
            self.control_sub_con_id = ("ipc://{0}/{1}_{2}"
                                       .format(self.ipc_path,
                                               self.current_pid,
                                               "controlSub"))
            self.request_fw_con_id = ("ipc://{0}/{1}_{2}"
                                      .format(self.ipc_path,
                                              self.current_pid,
                                              "requestFw"))
            self.router_con_id = ("ipc://{0}/{1}_{2}"
                                  .format(self.ipc_path,
                                          self.current_pid,
                                          "router"))

        self.whitelist = self.params["whitelist"]

        self.use_data_stream = self.params["use_data_stream"]
        self.log.info("Usage of data stream set to '{0}'"
                      .format(self.use_data_stream))

        if self.use_data_stream:
            if len(self.params["data_stream_targets"]) > 1:
                self.log.error("Targets to send data stream to have more than one entry which is not supported")
                self.log.debug("data_stream_targets: {0}".format(self.params["data_stream_targets"]))
                sys.exit(1)

            self.fixed_stream_id = ("{0}:{1}"
                                    .format(self.params["data_stream_targets"][0][0],
                                            self.params["data_stream_targets"][0][1]))
        else:
            self.fixed_stream_id = None

        self.number_of_streams = self.params["number_of_streams"]
        self.chunksize = self.params["chunksize"]

        try:
            self.local_target = self.params["local_target"]
            self.log.info("Configured local_target: {0}"
                          .format(self.local_target))
        except KeyError:
            self.local_target = None

        self.signalhandler_pr = None
        self.taskprovider_pr = None
        self.datadispatcher_pr = []

        self.log.info("Version: {0}".format(__version__))

        # IP and DNS name should be both in the whitelist
        helpers.extend_whitelist(self.whitelist, self.log)

        # Create zmq context
        # there should be only one context in one process
#        self.context = zmq.Context.instance()
        self.context = zmq.Context()
        self.log.debug("Registering global ZMQ context")

        try:
            if self.test_fixed_streaming_host(enable_logging=True):
                self.create_sockets()

                self.run()
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping due to unknown error condition",
                           exc_info=True)
        finally:
            self.stop()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("DataManager")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def create_sockets(self):

        # initiate forwarder for control signals (multiple pub, multiple sub)
        try:
            self.device = zmq.devices.ThreadDevice(
                zmq.FORWARDER, zmq.SUB, zmq.PUB)
            self.device.bind_in(self.control_pub_con_id)
            self.device.bind_out(self.control_sub_con_id)
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
            self.device.start()
            self.log.info("Start thead device forwarding messages "
                          "from '{0}' to '{1}'"
                          .format(self.control_pub_con_id,
                                  self.control_sub_con_id))
        except:
            self.log.error("Failed to start thead device forwarding messages "
                           "from '{0}' to '{1}'"
                           .format(self.control_pub_con_id,
                                   self.control_sub_con_id), exc_info=True)
            raise

        # socket for control signals
        try:
            self.control_pub_socket = self.context.socket(zmq.PUB)
            self.control_pub_socket.connect(self.control_pub_con_id)
            self.log.info("Start control_pub_socket (connect): '{0}'"
                          .format(self.control_pub_con_id))
        except:
            self.log.error("Failed to start control_pub_socket (connect): "
                           "'{0}'".format(self.control_pub_con_id),
                           exc_info=True)
            raise

    def test_fixed_streaming_host(self, enable_logging=False):
        if self.use_data_stream:
            if self.test_socket is None:
                try:
                    self.test_socket = self.context.socket(zmq.PUSH)
                    connection_str = "tcp://{0}".format(self.fixed_stream_id)

                    self.test_socket.connect(connection_str)
                    self.log.info("Start test_socket (connect): '{0}'"
                                  .format(connection_str))
                except:
                    self.log.error("Failed to start test_socket (connect): "
                                   "'{0}'".format(connection_str),
                                   exc_info=True)
                    return False

            try:
                if enable_logging:
                    self.log.debug("ZMQ version used: {0}"
                                   .format(zmq.__version__))

                # With older ZMQ versions the tracker results in an ZMQError in
                # the DataDispatchers when an event is processed
                # (ZMQError: Address already in use)
                if zmq.__version__ <= "14.5.0":

                    self.test_socket.send_multipart([b"ALIVE_TEST"])
                    if enable_logging:
                        self.log.info("Sending test message to fixed streaming"
                                      " host {0} ... success"
                                      .format(self.fixed_stream_id))

                else:
                    tracker = self.test_socket.send_multipart(
                        [b"ALIVE_TEST"], copy=False, track=True)
                    if not tracker.done:
                        tracker.wait(2)
#                    self.log.debug("tracker.done = {0}".format(tracker.done))
                    if not tracker.done:
                        self.log.error("Failed to send test message to fixed "
                                       "streaming host {0}"
                                       .format(self.fixed_stream_id),
                                       exc_info=True)
                        return False
                    elif enable_logging:
                        self.log.info("Sending test message to fixed "
                                      "streaming host {0} ... success"
                                      .format(self.fixed_stream_id))
            except:
                self.log.error("Failed to send test message to fixed "
                               "streaming host {0}"
                               .format(self.fixed_stream_id), exc_info=True)
                return False
        return True

    def run(self):
        self.signalhandler_pr = threading.Thread(target=SignalHandler,
                                                 args=(
                                                     self.control_pub_con_id,
                                                     self.control_sub_con_id,
                                                     self.whitelist,
                                                     self.com_con_id,
                                                     self.request_fw_con_id,
                                                     self.request_con_id,
                                                     self.log_queue,
                                                     self.context)
                                                 )
        self.signalhandler_pr.start()

        # needed, because otherwise the requests for the first files are not
        # forwarded properly
        time.sleep(0.5)

        if not self.signalhandler_pr.is_alive():
            return

        self.taskprovider_pr = Process(target=TaskProvider,
                                       args=(
                                           self.params,
                                           self.control_sub_con_id,
                                           self.request_fw_con_id,
                                           self.router_con_id,
                                           self.log_queue)
                                       )
        self.taskprovider_pr.start()

        self.log.info("Configured Type of data fetcher: {0}"
                      .format(self.params["data_fetcher_type"]))

        for i in range(self.number_of_streams):
            id = b"{0}/{1}".format(i, self.number_of_streams)
            pr = Process(target=DataDispatcher,
                         args=(
                             id,
                             self.control_sub_con_id,
                             self.router_con_id,
                             self.chunksize,
                             self.fixed_stream_id,
                             self.params,
                             self.log_queue,
                             self.local_target)
                         )
            pr.start()
            self.datadispatcher_pr.append(pr)

        # indicates if the processed are sent to waiting mode
        sleep_was_sent = False

        while self.signalhandler_pr.is_alive() and \
            self.taskprovider_pr.is_alive() and \
            all(datadispatcher.is_alive()
                for datadispatcher in self.datadispatcher_pr):

            if self.test_fixed_streaming_host():
                if sleep_was_sent:
                    self.log.info("Sending 'WAKEUP' signal")
                    self.control_pub_socket.send_multipart([b"control",
                                                            b"WAKEUP"])
                    sleep_was_sent = False

            else:
                # Due to an unforseeable event there is no active receiver on
                # the other side. Thus the processes should enter a waiting
                # mode and no data should be send.
                self.log.warning("Sending 'SLEEP' signal")
                self.control_pub_socket.send_multipart([b"control", b"SLEEP"])
                sleep_was_sent = True

            time.sleep(1)

        # notify which subprocess terminated
        if not self.signalhandler_pr.is_alive():
            self.log.info("SignalHandler terminated.")
        if not self.taskprovider_pr.is_alive():
            self.log.info("TaskProvider terminated.")
        if not any(datadispatcher.is_alive()
                   for datadispatcher in self.datadispatcher_pr):
            self.log.info("One DataDispatcher terminated.")

    def stop(self):

        if self.log is None:
            self.log = logging

        if self.control_pub_socket:
            self.log.info("Sending 'Exit' signal")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

        # waiting till the other processes are finished
        time.sleep(0.5)

        if self.control_pub_socket:
            self.log.info("Closing control_pub_socket")
            self.control_pub_socket.close(0)
            self.control_pub_socket = None

        if self.test_socket:
            self.log.debug("Stopping test_socket")
            self.test_socket.close(0)
            self.test_socket = None

        if self.context:
            self.log.info("Destroying context")
            self.context.destroy(0)
            self.context = None

        control_pub_path = ("{0}/{1}_{2}"
                            .format(self.ipc_path,
                                    self.current_pid,
                                    "controlPub"))
        control_sub_path = ("{0}/{1}_{2}"
                            .format(self.ipc_path,
                                    self.current_pid,
                                    "controlSub"))

        # Clean up ipc communication files
        try:
            os.remove(control_pub_path)
            self.log.debug("Removed ipc socket: {0}".format(control_pub_path))
        except OSError:
            self.log.debug("Could not remove ipc socket: {0}"
                           .format(control_pub_path))
        except:
            self.log.warning("Could not remove ipc socket: {0}"
                             .format(control_pub_path), exc_info=True)

        try:
            os.remove(control_sub_path)
            self.log.debug("Removed ipc socket: {0}".format(control_sub_path))
        except OSError:
            self.log.debug("Could not remove ipc socket: {0}"
                           .format(control_sub_path))
        except:
            self.log.warning("Could not remove ipc socket: {0}"
                             .format(control_sub_path), exc_info=True)

        # Remove temp directory (if empty)
        try:
            os.rmdir(self.ipc_path)
            self.log.debug("Removed IPC direcory: {0}".format(self.ipc_path))
        except OSError:
            self.log.debug("Could not remove IPC directory: {0}"
                           .format(self.ipc_path))
        except:
            self.log.warning("Could not remove IPC directory: {0}"
                             .format(self.ipc_path), exc_info=True)

        if not self.ext_log_queue and self.log_queue_listener:
            self.log.info("Stopping log_queue")
            self.log_queue.put_nowait(None)
            self.log_queue_listener.stop()
            self.log_queue_listener = None

    def signal_term_handler(self, signal, frame):
        self.log.debug('got SIGTERM')
        self.stop()

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


# cannot be defined in "if __name__ == '__main__'" because then it is unbound
# see https://docs.python.org/2/library/multiprocessing.html#windows
class TestReceiverStream():
    def __init__(self, com_port, fixed_recv_port, receiving_port,
                 receiving_port2, log_queue):

        self.log = self.get_logger(log_queue)

        context = zmq.Context.instance()

        self.com_socket = context.socket(zmq.REQ)
        connection_str = "tcp://localhost:{0}".format(com_port)
        self.com_socket.connect(connection_str)
        self.log.info("=== com_socket connected to {0}".format(connection_str))

        self.fixedRecvSocket = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{0}".format(fixed_recv_port)
        self.fixedRecvSocket.bind(connection_str)
        self.log.info("=== fixedRecvSocket connected to {0}"
                      .format(connection_str))

        self.receiving_socket = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{0}".format(receiving_port)
        self.receiving_socket.bind(connection_str)
        self.log.info("=== receiving_socket connected to {0}"
                      .format(connection_str))

        self.receiving_socket2 = context.socket(zmq.PULL)
        connection_str = "tcp://0.0.0.0:{0}".format(receiving_port2)
        self.receiving_socket2.bind(connection_str)
        self.log.info("=== receiving_socket2 connected to {0}"
                      .format(connection_str))

        self.send_signal("START_STREAM", receiving_port, 1)
        self.send_signal("START_STREAM", receiving_port2, 0)

        self.run()

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("TestReceiverStream")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def send_signal(self, signal, ports, prio=None):
        self.log.info("=== send_signal : {0}, {1}".format(signal, ports))
        send_message = [__version__, signal]
        targets = []
        if type(ports) == list:
            for port in ports:
                targets.append(["localhost:{0}".format(port), prio])
        else:
            targets.append(["localhost:{0}".format(ports), prio])

        targets = json.dumps(targets).encode("utf-8")
        send_message.append(targets)
        self.com_socket.send_multipart(send_message)
        received_message = self.com_socket.recv()
        self.log.info("=== Responce : {0}".format(received_message))

    def run(self):
        try:
            while True:
                recv_message = self.fixedRecvSocket.recv_multipart()
                self.log.info("=== received fixed: {0}"
                              .format(json.loads(recv_message[0])))
                recv_message = self.receiving_socket.recv_multipart()
                self.log.info("=== received: {0}"
                              .format(json.loads(recv_message[0])))
                recv_message = self.receiving_socket2.recv_multipart()
                self.log.info("=== received 2: {0}"
                              .format(json.loads(recv_message[0])))
        except KeyboardInterrupt:
            pass

    def __exit__(self):
        self.receiving_socket.close(0)
        self.receiving_socket2.close(0)
        self.context.destroy()


if __name__ == '__main__':
    # see https://docs.python.org/2/library/multiprocessing.html#windows
    freeze_support()

    test = False

    if test:
        from shutil import copyfile

        logfile = os.path.join(BASE_PATH, "logs", "datamanager_test.log")
        logsize = 10485760

        log_queue = Queue(-1)

        # Get the log Configuration for the lisener
        h1, h2 = helpers.get_log_handlers(logfile, logsize,
                                          verbose=True,
                                          onscreen_log_level="debug")

        # Start queue listener using the stream handler above
        log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
        log_queue_listener.start()

        # Create log and set handler to queue handle
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)  # Log level = DEBUG
        qh = QueueHandler(log_queue)
        root.addHandler(qh)

        com_port = "50000"
        fixed_recv_port = "50100"
        receiving_port = "50101"
        receiving_port2 = "50102"

        testPr = Process(target=TestReceiverStream,
                         args=(
                             com_port,
                             fixed_recv_port,
                             receiving_port,
                             receiving_port2,
                             log_queue))
        testPr.start()
        logging.debug("test receiver started")

        source_file = os.path.join(BASE_PATH, "test_file.cbf")
        target_file_base = os.path.join(
            BASE_PATH, "data", "source", "local", "raw") + os.sep

        try:
            sender = DataManager(log_queue)
        except:
            sender = None

        if sender:
            time.sleep(0.5)
            i = 100
            try:
                while i <= 105:
                    target_file = "{0}{1}.cbf".format(target_file_base, i)
                    logging.debug("copy to {0}".format(target_file))
                    copyfile(source_file, target_file)
                    i += 1

                    time.sleep(1)
            except Exception as e:
                logging.error("Exception detected: {0}".format(e),
                              exc_info=True)
            finally:
                time.sleep(3)
                testPr.terminate()

                for number in range(100, i):
                    target_file = "{0}{1}.cbf".format(target_file_base, number)
                    try:
                        os.remove(target_file)
                        logging.debug("remove {0}".format(target_file))
                    except:
                        pass

                sender.stop()
                log_queue.put_nowait(None)
                log_queue_listener.stop()

    else:
        sender = None
        try:
            sender = DataManager()
        finally:
            if sender is not None:
                sender.stop()
