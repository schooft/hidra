"""Testing the file_fetcher data fetcher.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import time
import zmq
from shutil import copyfile
from multiprocessing import Process


from .__init__ import BASE_DIR
from .datafetcher_test_base import DataFetcherTestBase
from file_fetcher import DataFetcher, Cleaner

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class TestDataFetcher(DataFetcherTestBase):
    """Specification of tests to be performed for the loaded DataFetcher.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestDataFetcher, self).setUp()

        # Set up config
        self.data_fetcher_config = {
            "fix_subdirs": ["commissioning", "current", "local"],
            "store_data": False,
            "remove_data": False,
            "chunksize": 10485760,  # = 1024*1024*10 = 10 MiB
            "local_target": None,
            # "local_target": os.path.join(BASE_DIR, "data", "target"),
            "main_pid": self.config["main_pid"],
            "endpoints": self.config["endpoints"]
        }

        self.cleaner_config = {
            "main_pid": self.config["main_pid"]
        }

        self.receiving_ports = ["6005", "6006"]

        self.datafetcher = None
        self.receiving_sockets = None
        self.control_pub_socket = None

    def test_no_confirmation(self):
        """Simulate file fetching without taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(config=self.data_fetcher_config,
                                       log_queue=self.log_queue,
                                       fetcher_id=0,
                                       context=self.context,
                                       lock=self.lock)

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        # Test file fetcher
        source_dir = os.path.join(BASE_DIR, "data", "source")
        prework_source_file = os.path.join(BASE_DIR,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")
        prework_target_file = os.path.join(source_dir, "local", "100.cbf")

        copyfile(prework_source_file, prework_target_file)
        time.sleep(0.5)

        metadata = {
            "source_path": os.path.join(BASE_DIR, "data", "source"),
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        self.datafetcher.get_metadata(targets, metadata)

        self.datafetcher.send_data(targets, metadata, open_connections)

        self.datafetcher.finish(targets, metadata, open_connections)

        self.log.debug("open_connections after function call: {}"
                       .format(open_connections))

        try:
            for sckt in self.receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: {}".format(recv_message))
        except KeyboardInterrupt:
            pass

    def test_with_confirmation(self):
        """Simulate file fetching while taking care of confirmation signals.
        """

        self.datafetcher = DataFetcher(config=self.data_fetcher_config,
                                       log_queue=self.log_queue,
                                       fetcher_id=0,
                                       context=self.context,
                                       lock=self.lock)

        self.config["remove_data"] = "with_confirmation"
        endpoints = self.config["endpoints"]

        # Set up cleaner
        kwargs = dict(
            config=self.cleaner_config,
            log_queue=self.log_queue,
            endpoints=endpoints,
            context=self.context
        )
        cleaner_pr = Process(target=Cleaner, kwargs=kwargs)
        cleaner_pr.start()

        # Set up receiver simulator
        self.receiving_sockets = []
        for port in self.receiving_ports:
            self.receiving_sockets.append(self.set_up_recv_socket(port))

        confirmation_socket = self.start_socket(
            name="confirmation_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.confirm_bind
        )

        # create control socket
        # control messages are not send over an forwarder, thus the
        # control_sub endpoint is used directly
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="bind",
            endpoint=endpoints.control_sub_bind
        )

        # Test file fetcher
        source_dir = os.path.join(BASE_DIR, "data", "source")
        prework_source_file = os.path.join(BASE_DIR,
                                           "test",
                                           "test_files",
                                           "test_file.cbf")
        prework_target_file = os.path.join(source_dir, "local", "100.cbf")

        copyfile(prework_source_file, prework_target_file)
        time.sleep(0.5)

        metadata = {
            "source_path": source_dir,
            "relative_path": os.sep + "local",
            "filename": "100.cbf"
        }

        targets = [
            ["{}:{}".format(self.con_ip, self.receiving_ports[0]), 1, "data"],
            ["{}:{}".format(self.con_ip, self.receiving_ports[1]), 0, "data"]
        ]

        open_connections = dict()

        self.log.debug("open_connections before function call: {}"
                       .format(open_connections))

        self.datafetcher.get_metadata(targets, metadata)

        self.datafetcher.send_data(targets, metadata, open_connections)

        self.datafetcher.finish(targets, metadata, open_connections)

        # generate file identifier
        if metadata["relative_path"].startswith("/"):
            metadata["relative_path"] = metadata["relative_path"][1:]

        file_id = os.path.join(metadata["relative_path"],
                               metadata["filename"])

        # send file identifier to cleaner
        confirmation_socket.send(file_id.encode("utf-8"))
        self.log.debug("confirmation sent {}".format(file_id))

        self.log.debug("open_connections after function call: {}"
                       .format(open_connections))

        try:
            for sckt in self.receiving_sockets:
                recv_message = sckt.recv_multipart()
                recv_message = json.loads(recv_message[0].decode("utf-8"))
                self.log.info("received: {}".format(recv_message))
        except KeyboardInterrupt:
            pass

        self.stop_socket(name="confirmation_socket",
                         socket=confirmation_socket)

    def tearDown(self):
        if self.control_pub_socket is not None:
            self.log.debug("Sending control signal: EXIT")
            self.control_pub_socket.send_multipart([b"control", b"EXIT"])

            # give signal time to arrive
            time.sleep(1)

        if self.receiving_sockets is not None:
            for i, sckt in enumerate(self.receiving_sockets):
                self.stop_socket(name="receiving_socket{}".format(i),
                                 socket=sckt)
            self.receiving_sockets = None

        if self.datafetcher is not None:
            self.log.debug("Stopping datafetcher")
            self.datafetcher.stop()
            self.datafetcher = None

        self.stop_socket(name="control_pub_socket")

        super(TestDataFetcher, self).tearDown()