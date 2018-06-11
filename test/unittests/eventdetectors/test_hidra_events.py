"""Testing the hidra_events event detector.
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import json
import os
import tempfile
import zmq

from .__init__ import BASE_DIR
from .test_eventdetector_base import TestEventDetectorBase, create_dir
from hidra_events import EventDetector


class TestEventDetector(TestEventDetectorBase):
    """Specification of tests to be performed for the loaded EventDetecor.
    """

    # pylint: disable=too-many-instance-attributes
    # Is reasonable in this case.

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestEventDetector, self)._init_logging

        self._init_logging()

        self.main_pid = os.getpid()

        self.ipc_path = os.path.join(tempfile.gettempdir(), "hidra")
        create_dir(self.ipc_path)
#        if not os.path.exists(self.ipc_path):
#            os.mkdir(ipc_path)
#            os.chmod(self.ipc_path, 0o777)
#            self.log.info("Creating directory for IPC communication: {}"
#                          .format(self.ipc_path))

        self._event_det_con_str = "ipc://{}/{}_{}".format(self.ipc_path,
                                                          self.main_pid,
                                                          "eventDet")
        print("self.event_det_con_str", self._event_det_con_str)

        self.config = {
            "context": None,
            "ext_ip": "0.0.0.0",
            "ipc_path": self.ipc_path,
            "main_pid": self.main_pid,
            "ext_data_port": "50100"
        }

        self.start = 100
        self.stop = 101

        target_base_path = os.path.join(BASE_DIR, "data", "source")
        target_relative_path = os.path.join("local", "raw")
        self.target_path = os.path.join(target_base_path,
                                        target_relative_path)

        self.eventdetector = EventDetector(self.config, self.log_queue)

    def test_eventdetector(self):
        """Simulate incoming data and check if received events are correct.
        """

        context = zmq.Context.instance()

        in_con_str = "tcp://{}:{}".format(self.config["ext_ip"],
                                          self.config["ext_data_port"])
        out_con_str = "ipc://{}/{}_{}".format(self.ipc_path,
                                              self.main_pid,
                                              "out")

        local_in = True
        local_out = True

        if local_in:
            # create zmq socket to send events
            data_in_socket = context.socket(zmq.PUSH)
            data_in_socket.connect(in_con_str)
            self.log.info("Start data_in_socket (connect): '{}'"
                          .format(in_con_str))

        if local_out:
            data_out_socket = context.socket(zmq.PULL)
            data_out_socket.connect(out_con_str)
            self.log.info("Start data_out_socket (connect): '{}'"
                          .format(out_con_str))

        try:
            for i in range(self.start, self.stop):
                self.log.debug("generate event")
                target_file = "{}{}.cbf".format(self.target_path, i)
                message = {
                    "filename": target_file,
                    "filepart": 0,
                    "chunksize": 10
                }

                if local_in:
                    data_in_socket.send_multipart(
                        [json.dumps(message).encode("utf-8"), b"incoming_data"]
                    )

                event_list = self.eventdetector.get_new_event()
                if event_list:
                    self.log.debug("event_list: {}".format(event_list))

                self.assertIn(message, event_list)
                self.assertEqual(len(event_list), 1)
                self.assertDictEqual(event_list[0], message)

                if local_out:
                    recv_message = data_out_socket.recv_multipart()
                    self.log.debug("Received - {}".format(recv_message))

        except KeyboardInterrupt:
            pass
        finally:
            if local_in:
                data_in_socket.close()
            if local_out:
                data_out_socket.close()

    def tearDown(self):
        self.eventdetector.stop()

        super(TestEventDetector, self).tearDown()