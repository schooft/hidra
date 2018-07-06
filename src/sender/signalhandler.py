from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import zmq
import zmq.devices
import os
import copy
import json
import re

from base_class import Base

from __init__ import BASE_PATH  # noqa F401
from _version import __version__
import utils
from hidra import convert_suffix_list_to_regex

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class SignalHandler(Base):

    def __init__(self,
                 config,
                 endpoints,
                 whitelist,
                 ldapuri,
                 log_queue,
                 context=None):

        self.config = config
        self.endpoints = endpoints

        self.log = None

        self.open_requ_vari = []
        self.open_requ_perm = []
        self.allowed_queries = []
        # to rotate through the open permanent requests
        self.next_requ_node = []

        self.whitelist = None
        self.open_connections = []

        self.context = None
        self.ext_context = None
        self.socket = None
        self.poller = None

        self.control_pub_socket = None
        self.control_sub_socket = None
        self.com_socket = None
        self.request_fw_socket = None
        self.request_socket = None

        self.setup(log_queue, context, whitelist, ldapuri)

        self.exec_run()

    def setup(self, log_queue, context, whitelist, ldapuri):

        # Send all logs to the main process
        self.log = utils.get_logger("SignalHandler", log_queue)
        self.log.debug("SignalHandler started (PID {}).".format(os.getpid()))

        self.whitelist = utils.extend_whitelist(whitelist, ldapuri, self.log)

        # remember if the context was created outside this class or not
        if context:
            self.context = context
            self.ext_context = True
        else:
            self.log.info("Registering ZMQ context")
            self.context = zmq.Context()
            self.ext_context = False

        try:
            self.create_sockets()
        except:
            self.log.error("Cannot create sockets", exc_info=True)
            self.stop()

    def create_sockets(self):

        # socket to send control signals to
        self.control_pub_socket = self.start_socket(
            name="control_pub_socket",
            sock_type=zmq.PUB,
            sock_con="connect",
            endpoint=self.endpoints.control_pub_con
        )

        # socket to get control signals from
        self.control_sub_socket = self.start_socket(
            name="control_sub_socket",
            sock_type=zmq.SUB,
            sock_con="connect",
            endpoint=self.endpoints.control_sub_con
        )

        self.control_sub_socket.setsockopt_string(zmq.SUBSCRIBE, u"control")

        # socket to forward requests
        self.request_fw_socket = self.start_socket(
            name="request_fw_socket",
            sock_type=zmq.REP,
            sock_con="bind",
            endpoint=self.endpoints.request_fw_bind
        )

        if self.whitelist != []:
            # create zmq socket for signal communication with receiver
            self.com_socket = self.start_socket(
                name="com_socket",
                sock_type=zmq.REP,
                sock_con="bind",
                endpoint=self.endpoints.com_bind
            )

            # create socket to receive requests
            self.request_socket = self.start_socket(
                name="request_socket",
                sock_type=zmq.PULL,
                sock_con="bind",
                endpoint=self.endpoints.request_bind
            )
        else:
            self.log.info("Socket com_socket and request_socket not started "
                          "since there is no host allowed to connect")

        # Poller to distinguish between start/stop signals and queries for the
        # next set of signals
        self.poller = zmq.Poller()
        self.poller.register(self.control_sub_socket, zmq.POLLIN)
        self.poller.register(self.request_fw_socket, zmq.POLLIN)
        if self.whitelist != []:
            self.poller.register(self.com_socket, zmq.POLLIN)
            self.poller.register(self.request_socket, zmq.POLLIN)

    def exec_run(self):
        try:
            self.run()
        except zmq.ZMQError:
            self.log.error("Stopping signalHandler due to ZMQError.",
                           exc_info=True)
        except KeyboardInterrupt:
            pass
        except:
            self.log.error("Stopping SignalHandler due to unknown error "
                           "condition.", exc_info=True)
        finally:
            self.stop()

    def run(self):
        """React to incoming signals.

        Possible incomming signals:
        com_socket
            (start/stop command from external)
            START_STREAM: Add request for all incoming data packets
                          (no  further requests needed)
            STOP_STREAM: Remove assignment for all incoming data packets
            START_STREAM_METADATA: Add request for metadata only of all
                                   incoming data packets
                                   (no  further requests needed)
            STOP_STREAM_METADATA: Remove assignment for metadata of all
                                  incoming data packets
            START_QUERY_NEXT: Enable requests for individual data packets
            STOP_QUERY_NEXT: Disable requests for individual data packets
            START_QUERY_METADATA: Enable requests for metadata of individual
                                  data packets
            STOP_QUERY_METADATA: Disable requests for metadata of individual
                                 data packets

        request_socket
            (requests from external)
            NEXT: Request for the next incoming data packet
            CANCEL: Cancel the previous request

        request_fw_socket
            (internal forwarding of requests which came fromexternal)
            GET_REQUESTS: TaskProvider asks to get the next set of open
                          requests

        control_sub_socket
            (internal control messages)
            SLEEP: receiver is currently not available
                   -> this does not affect this class
            WAKEUP: receiver is back online
                    -> this does not affect this class
            EXIT: shutdown everything
        """

        # run loop, and wait for incoming messages
        self.log.debug("Waiting for new signals or requests.")
        while True:
            socks = dict(self.poller.poll())

            #---------------------------------------------------------------------
            # incoming request from TaskProvider
            #---------------------------------------------------------------------
            if (self.request_fw_socket in socks
                    and socks[self.request_fw_socket] == zmq.POLLIN):

                try:
                    in_message = self.request_fw_socket.recv_multipart()
                    if in_message[0] == b"GET_REQUESTS":
                        self.log.debug("New request for signals received.")
                        filename = json.loads(in_message[1].decode("utf-8"))
                        open_requests = []

                        for request_set in self.open_requ_perm:
                            if request_set:
                                index = self.open_requ_perm.index(request_set)

                                # [<host:port>, <prio>, <suffix_list>,
                                #  <metadata|data>]
                                socket_id, prio, pattern, send_type = (
                                    request_set[self.next_requ_node[index]])

                                # Check if filename matches requested
                                # regex
                                if pattern.match(filename) is not None:
                                    # do not send pattern
                                    open_requests.append([socket_id,
                                                          prio,
                                                          send_type])

                                    # distribute in round-robin order
                                    self.next_requ_node[index] = (
                                        (self.next_requ_node[index] + 1)
                                        % len(request_set)
                                    )

                        for request_set in self.open_requ_vari:
                            # Check if filename suffix matches requested suffix
                            if (request_set
                                    and (request_set[0][2].match(filename)
                                         is not None)):
                                socket_id, prio, pattern, send_type = (
                                    request_set.pop(0))
                                # do not send pattern
                                open_requests.append([socket_id,
                                                      prio,
                                                      send_type])

                        if open_requests:
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests))
                            self.log.debug("Answered to request: {}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {}"
                                           .format(self.allowed_queries))
                        else:
                            open_requests = ["None"]
                            self.request_fw_socket.send_string(
                                json.dumps(open_requests)
                            )
                            self.log.debug("Answered to request: {}"
                                           .format(open_requests))
                            self.log.debug("open_requ_vari: {}"
                                           .format(self.open_requ_vari))
                            self.log.debug("allowed_queries: {}"
                                           .format(self.allowed_queries))

                except:
                    self.log.error("Failed to receive/answer new signal "
                                   "requests", exc_info=True)

            #---------------------------------------------------------------------
            # start/stop command from external
            #---------------------------------------------------------------------
            if (self.com_socket in socks
                    and socks[self.com_socket] == zmq.POLLIN):

                in_message = self.com_socket.recv_multipart()
                self.log.debug("Received signal: {}".format(in_message))

                check_failed, signal, target = (
                    self.check_signal_inverted(in_message)
                )
                if not check_failed:
                    self.react_to_signal(signal, target)
                else:
                    self.send_response(check_failed)

            #---------------------------------------------------------------------
            # request from external
            #---------------------------------------------------------------------
            if (self.request_socket in socks
                    and socks[self.request_socket] == zmq.POLLIN):

                in_message = self.request_socket.recv_multipart()
                self.log.debug("Received request: {}".format(in_message))

                if in_message[0] == b"NEXT":
                    incoming_socket_id = utils.convert_socket_to_fqdn(
                        in_message[1].decode("utf-8"), self.log)

                    for index in range(len(self.allowed_queries)):
                        for i in range(len(self.allowed_queries[index])):
                            if (incoming_socket_id
                                    == self.allowed_queries[index][i][0]):
                                self.open_requ_vari[index].append(
                                    self.allowed_queries[index][i])
                                self.log.info("Add to open requests: {}"
                                              .format(self.allowed_queries[
                                                  index][i]))

                elif in_message[0] == b"CANCEL":
                    incoming_socket_id = utils.convert_socket_to_fqdn(
                        in_message[1].decode("utf-8"), self.log
                    )

                    still_requested = []
                    for a in range(len(self.open_requ_vari)):
                        vari_per_group = []
                        for b in self.open_requ_vari[a]:
                            if incoming_socket_id != b[0]:
                                vari_per_group.append(b)

                        still_requested.append(vari_per_group)

                    self.open_requ_vari = still_requested

                    self.log.info("Remove all occurences from {} from "
                                  "variable request list."
                                  .format(incoming_socket_id))

                else:
                    self.log.info("Request not supported.")

            #---------------------------------------------------------------------
            # control commands from internal
            #---------------------------------------------------------------------
            if (self.control_sub_socket in socks
                    and socks[self.control_sub_socket] == zmq.POLLIN):

                try:
                    message = self.control_sub_socket.recv_multipart()
                    # self.log.debug("Control signal received.")
                except:
                    self.log.error("Waiting for control signal...failed",
                                   exc_info=True)
                    continue

                # remove subscription topic
                del message[0]

                if message[0] == b"EXIT":
                    self.log.info("Requested to shutdown.")
                    break
                elif message[0] == b"SLEEP":
                    # self.log.debug("Received sleep signal. Do nothing.")
                    continue
                elif message[0] == b"WAKEUP":
                    self.log.debug("Received wakeup signal. Do nothing.")
                    continue
                else:
                    self.log.error("Unhandled control signal received: {}"
                                   .format(message[0]))

    def check_signal_inverted(self, in_message):
        """Unpack and check incoming message.
        """

        if len(in_message) != 3:
            self.log.warning("Received signal is of the wrong format")
            self.log.debug("Received signal is too short or too long: {}"
                           .format(in_message))
            return [b"NO_VALID_SIGNAL"], None, None

        try:
            version, signal, targets = (
                in_message[0].decode("utf-8"),
                in_message[1],
                in_message[2].decode("utf-8")
            )
            targets = json.loads(targets)

            targets = utils.convert_socket_to_fqdn(targets, self.log)

            host = [t[0].split(":")[0] for t in targets]
            self.log.debug("host {}".format(host))
        except:
            return [b"NO_VALID_SIGNAL"], None, None

        if version:
            if utils.check_version(version, self.log):
                self.log.info("Versions are compatible")
            else:
                self.log.warning("Versions are not compatible")
                return [b"VERSION_CONFLICT", __version__], None, None

        if signal and host:
            # Checking signal sending host
            self.log.debug("Check if host to send data to are in whitelist...")
            if utils.check_host(host, self.whitelist, self.log):
                self.log.info("Hosts are allowed to connect.")
                self.log.debug("hosts: {}".format(host))
            else:
                self.log.warning("One of the hosts is not allowed to connect.")
                self.log.debug("hosts: {}".format(host))
                self.log.debug("whitelist: {}".format(self.whitelist))
                return [b"NO_VALID_HOST"], None, None

        return False, signal, targets

    def send_response(self, signal):
        """Send response back.
        """

        if type(signal) != list:
            signal = [signal]

        self.log.debug("Send response back: {}".format(signal))
        self.com_socket.send_multipart(signal, zmq.NOBLOCK)

    def _start_signal(self,
                      signal,
                      send_type,
                      socket_ids,
                      registered_ids,
                      vari_requests,
                      perm_requests):
        """Register socket ids and updated related lists accordingly.

        Updated registered_ids, vari_requests and perm_requests in place and send
        confirmation back.

        Args:
            signal: Signal to send after finishing
            send_type: The type of data the socket ids should get.
            socket_ids: Socket ids to be registered.
            registered_ids: Already registered socket ids.
            vari_requests: List of open requests (query mode).
            perm_requests: List of next node number to serve (stream mode).
        """

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

        # Convert suffixes to regex
        # for compatibility with API versions 3.1.2 or older
        # socket_ids is of the format [[<host>, <prio>, <suffix>], ...]
        for socket_conf in socket_ids:
            self.log.debug("suffix={}".format(socket_conf[2]))
            socket_conf[2] = convert_suffix_list_to_regex(socket_conf[2],
                                                          suffix=True,
                                                          compile_regex=False,
                                                          log=self.log)

        overwrite_index = None

        # the registerd disjoint socket ids for each node set
        # [set(<host>:<port>, <host>:<port>, ...), set(...), ...]
        registered_socketids_flatlist = [set([j[0] for j in sublist])
                                         for sublist in registered_ids]

        # the disjoint socket_ids to be register
        # "set" is used to eliminated duplications
        # set(<host>:<port>, <host>:<port>, ...) created from target list (=socket_ids)
        socket_ids_flatlist = set([socket_conf[0]
                                   for socket_conf in socket_ids])

        # If the socket_ids of the node set to be register are either a subset
        # or a superset of an already registered node set overwrite the old one
        # with it
        for i in registered_socketids_flatlist:
            # Check if socket_ids is sublist of one entry of registered_ids
            # -> overwrite existing entry
            if socket_ids_flatlist.issubset(i):
                self.log.debug("socket_ids already contained, override")
                overwrite_index = registered_socketids_flatlist.index(i)
            # Check if one entry of registered_ids is sublist in socket_ids
            # -> overwrite existing entry
            elif i.issubset(socket_ids_flatlist):
                self.log.debug("socket_ids is superset of already contained "
                               "set, override")
                overwrite_index = registered_socketids_flatlist.index(i)
            # TODO Mixture ?
            elif not socket_ids_flatlist.isdisjoint(i):
                self.log.error("socket_ids is neither a subset nor superset "
                               "of already contained set")
                self.log.debug("Currently: no idea what to do with this.")
                self.log.debug("socket_ids={}".format(socket_ids_flatlist))
                self.log.debug("registered_socketids={}".format(i))

        if overwrite_index is not None:
            # overriding is necessary because the new request may contain
            # different parameters like monitored file suffix, priority or
            # connection type also this means the old socket_id set should be
            # replaced in total and not only partially
            self.log.debug("overwrite_index={}".format(overwrite_index))

            registered_ids[overwrite_index] = copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids])
            )

            # compile regex
            # This cannot be done before because deepcopy does not support it
            # for python versions < 3.7, see http://bugs.python.org/issue10076
            for socket_conf in registered_ids[overwrite_index]:
                socket_conf[2] = re.compile(socket_conf[2])

            if perm_requests is not None:
                perm_requests[overwrite_index] = 0

            if vari_requests is not None:
                vari_requests[overwrite_index] = []
        else:
            registered_ids.append(copy.deepcopy(
                sorted([i + [send_type] for i in socket_ids]))
            )

            # compile regex
            # This cannot be done before because deepcopy does not support it
            # for python versions < 3.7, see http://bugs.python.org/issue10076
            for socket_conf in registered_ids[-1]:
                socket_conf[2] = re.compile(socket_conf[2])

            if perm_requests is not None:
                perm_requests.append(0)

            if vari_requests is not None:
                vari_requests.append([])

        self.log.debug("after start handling: registered_ids={}"
                       .format(registered_ids))

        # send signal back to receiver
        self.send_response([signal])

#        connection_found = False
#        tmp_allowed = []
#        flatlist = [i[0] for i in
#                    [j for sublist in registered_ids for j in sublist]]
#        self.log.debug("flatlist: {0}".format(flatlist))

#        for socket_conf in socket_ids:
#
#            socket_conf[0] = socket.getfqdn(socket_conf[0])
#
#            socket_id = socket_conf[0]
#            self.log.debug("socket_id: {0}".format(socket_id))
#
#            if socket_id in flatlist:
#                connection_found = True
#                self.log.info("Connection to {0} is already open"
#                              .format(socket_id))
#            elif socket_id not in [ i[0] for i in tmp_allowed]:
#                tmp_socket_conf = socket_conf + [send_type]
#                tmp_allowed.append(tmp_socket_conf)
#            else:
#                # TODO send notification back?
#                # (double entries in START_QUERY_NEXT)
#                pass

#        if not connection_found:
#            # send signal back to receiver
#            self.send_response([signal])
#            registered_ids.append(copy.deepcopy(sorted(tmp_allowed)))
#            if perm_requests != None:
#                perm_requests.append(0)
#            del tmp_allowed
#
#            if vari_requests != None:
#                vari_requests.append([])
#        else:
#            # send error back to receiver
# #           self.send_response(["CONNECTION_ALREADY_OPEN"])
#            # "reopen" the connection and confirm to receiver
#            self.send_response([signal])

    def _stop_signal(self,
                     signal,
                     socket_ids,
                     registered_ids,
                     vari_requests,
                     perm_requests):
        """Unregister socket ids and updated related lists accordingly.

        Updated registered_ids, vari_requests and perm_requests in place and send
        confirmation back.

        Args:
            signal: Signal to send after finishing
            socket_ids: Socket ids to be deregistered.
            registered_ids: Currently registered socket ids.
            vari_requests: List of open requests (query mode).
            perm_requests: List of next node number to serve (stream mode).
        """

        socket_ids = utils.convert_socket_to_fqdn(socket_ids,
                                                  self.log)

#        connection_not_found = False
#        found = False
#        for socket_conf in socket_ids:
#            socket_id = socket_conf[0]
#
#            for sublist in registered_ids:
#                for element in sublist:
#                    if socket_id == element[0]:
#                        to_remove.append(element)
#                        found = True
#            if not found:
#                connection_not_found = True

        # list of socket configurations to remove (in format how they are
        # registered:
        # [[[<host>:<port>, <prio>, <regex>, <end_type>],...],...]
        # this is needed because socket_ids only contain partial information:
        # [[<host>:<port>, <prio>, <regex uncompiled>]]
        to_remove = [reg_id
                     for socket_conf in socket_ids
                     for sublist in registered_ids
                     for reg_id in sublist
                     if socket_conf[0] == reg_id[0]]

        if not to_remove:
            self.send_response([b"NO_OPEN_CONNECTION_FOUND"])
            self.log.info("No connection to close was found for {}"
                          .format(socket_conf))
        else:
            # send signal back to receiver
            self.send_response([signal])

            for element in to_remove:
                socket_id = element[0]

                if vari_requests is not None:
                    # vari requests is of the form
                    # [[[<host>:<port>, <prio>, <regex>, <end_type>],...],...]
                    vari_requests = [[socket_conf
                                      for socket_conf in open_requests
                                      if socket_id != socket_conf[0]]
                                     for open_requests in vari_requests]
                    self.log.debug("Remove all occurences from {} from "
                                   "variable request list.".format(socket_id))

                self.log.debug("registered_ids {}".format(registered_ids))
                self.log.debug("element {}".format(element))
                self.log.debug("perm_requests {}".format(perm_requests))

                index_to_remove = []
                # registered_ids is of the form
                # [[[<host>:<port>, <prio>, <regex>, <end_type>],...],...]
                for i, node_set in enumerate(registered_ids):
                    if element in node_set:
                        node_set.remove(element)
                        self.log.debug("Deregister {}".format(socket_id))
#                        self.log.debug("Remove {} from permanent request "
#                                       "allowed list.".format(socket_id))

                        if not node_set:
                            index_to_remove.append(i)
                            # remove open requests (querys)
                            if vari_requests is not None:
                                del vari_requests[i]
                            # remove open requests (streams)
                            if perm_requests is not None:
                                perm_requests.pop(i)
                        else:
                            # perm_requests is a list of node numbers to feed
                            # next i.e. index of the node inside of the node
                            # set whose request will be served next
                            # -> has to be updated because number of
                            # registered nodes changed
                            if perm_requests is not None:
                                perm_requests[i] = (
                                    perm_requests[i] % len(registered_ids[i])
                                )

                # remove left over empty list
                for index in index_to_remove:
                    del registered_ids[index]

            # send signal to TaskManager
            self.control_pub_socket.send_multipart(
                [b"signal",
                 b"CLOSE_SOCKETS",
                 json.dumps(socket_ids).encode("utf-8")]
            )

        return registered_ids, vari_requests, perm_requests

    def react_to_signal(self, signal, socket_ids):

        #---------------------------------------------------------------------
        # START_STREAM
        #---------------------------------------------------------------------
        if signal == b"GET_VERSION":
            self.log.info("Received signal: {}".format(signal))

            self.send_response([signal, __version__])
            return

        #---------------------------------------------------------------------
        # START_STREAM
        #---------------------------------------------------------------------
        elif signal == b"START_STREAM":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            self._start_signal(
                signal=signal,
                send_type="data",
                socket_ids=socket_ids,
                registered_ids=self.open_requ_perm,
                vari_requests=None,
                perm_requests=self.next_requ_node
            )

            return

        #---------------------------------------------------------------------
        # START_STREAM_METADATA
        #---------------------------------------------------------------------
        elif signal == b"START_STREAM_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))
            if not self.config["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", __version__])
            else:
                self._start_signal(
                    signal=signal,
                    send_type="metadata",
                    socket_ids=socket_ids,
                    registered_ids=self.open_requ_perm,
                    vari_requests=None,
                    perm_requests=self.next_requ_node
                )

            return

        #---------------------------------------------------------------------
        # STOP_STREAM
        # STOP_STREAM_METADATA
        #---------------------------------------------------------------------
        elif signal == b"STOP_STREAM" or signal == b"STOP_STREAM_METADATA":
            self.log.info("Received signal: {} for host {}"
                          .format(signal, socket_ids))

            ret_val = self._stop_signal(
                signal=signal,
                socket_ids=socket_ids,
                registered_ids=self.open_requ_perm,
                vari_requests=None,
                perm_requests=self.next_requ_node
            )

            self.open_requ_perm, _, self.next_requ_node = ret_val

            return

        #---------------------------------------------------------------------
        # START_QUERY_NEXT
        #---------------------------------------------------------------------
        elif signal == b"START_QUERY_NEXT":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            self._start_signal(
                signal=signal,
                send_type="data",
                socket_ids=socket_ids,
                registered_ids=self.allowed_queries,
                vari_requests=self.open_requ_vari,
                perm_requests=None
            )

            return

        #---------------------------------------------------------------------
        # START_QUERY_METADATA
        #---------------------------------------------------------------------
        elif signal == b"START_QUERY_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            if not self.config["store_data"]:
                self.log.debug("Send notification that store_data is disabled")
                self.send_response([b"STORING_DISABLED", __version__])
            else:
                self._start_signal(
                    signal=signal,
                    send_type="metadata",
                    socket_ids=socket_ids,
                    registered_ids=self.allowed_queries,
                    vari_requests=self.open_requ_vari,
                    perm_requests=None
                )

            return

        #---------------------------------------------------------------------
        #  STOP_QUERY_NEXT
        #  STOP_QUERY_METADATA
        #---------------------------------------------------------------------
        elif signal == b"STOP_QUERY_NEXT" or signal == b"STOP_QUERY_METADATA":
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))

            ret_val = self._stop_signal(
                signal=signal,
                socket_ids=socket_ids,
                registered_ids=self.allowed_queries,
                vari_requests=self.open_requ_vari,
                perm_requests=None
            )

            self.allowed_queries, self.open_requ_vari, _ = ret_val

            return

        else:
            self.log.info("Received signal: {} for hosts {}"
                          .format(signal, socket_ids))
            self.send_response([b"NO_VALID_SIGNAL"])

    def stop(self):
        self.log.debug("Closing sockets for SignalHandler")

        self.stop_socket(name="com_socket")
        self.stop_socket(name="request_socket")
        self.stop_socket(name="request_fw_socket")
        self.stop_socket(name="control_pub_socket")
        self.stop_socket(name="control_sub_socket")

        if not self.ext_context and self.context:
            self.log.info("Destroying context")
            self.context.term()
#            self.context.destroy(0)
            self.context = None

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()
