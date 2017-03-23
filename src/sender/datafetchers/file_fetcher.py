from __future__ import print_function
from __future__ import unicode_literals

import zmq
import os
import logging
import json
import shutil
import errno

from send_helpers import send_to_targets, DataHandlingError
from __init__ import BASE_PATH
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


class DataFetcher():

    def __init__(self, config, log_queue, id):

        self.id = id
        self.config = config

        self.log = helpers.get_logger("file_fetcher-{0}".format(self.id),
                                      log_queue)

        self.source_file = None
        self.target_file = None

        required_params = ["fix_subdirs",
                           "store_data",
                           ["remove_data", [True, False, "with_confirmation"]],
                           "chunksize",
                           "local_target"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            self.config,
                                                            self.log)

        if check_passed:
            self.log.info("Configuration for data fetcher: {0}"
                          .format(config_reduced))

            self.config["send_timeout"] = -1  # 10
            self.config["remove_flag"] = False
        else:
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    def get_metadata(self, targets, metadata):

        # extract fileEvent metadata
        try:
            # TODO validate metadata dict
            filename = metadata["filename"]
            source_path = metadata["source_path"]
            relative_path = metadata["relative_path"]
        except:
            self.log.error("Invalid event message received.", exc_info=True)
            self.log.debug("metadata={0}".format(metadata))
            # skip all further instructions and continue with next iteration
            raise

        # filename = "img.tiff"
        # filepath = "C:\dir"
        #
        # -->  source_file_path = 'C:\\dir\img.tiff'
        if relative_path.startswith("/"):
            source_file_path = (os.path.normpath(
                os.path.join(source_path, relative_path[1:])))
        else:
            source_file_path = (os.path.normpath(
                os.path.join(source_path, relative_path)))
        self.source_file = os.path.join(source_file_path, filename)

        # TODO combine better with source_file... (for efficiency)
        if self.config["local_target"]:
            target_file_path = (os.path.normpath(
                os.path.join(self.config["local_target"], relative_path)))
            self.target_file = os.path.join(target_file_path, filename)
        else:
            self.target_file = None

        if targets:
            try:
                self.log.debug("get filesize for '{0}'..."
                               .format(self.source_file))
                filesize = os.path.getsize(self.source_file)
                file_mod_time = os.stat(self.source_file).st_mtime
                file_create_time = os.stat(self.source_file).st_ctime
                self.log.debug("filesize({0}) = {1}"
                               .format(self.source_file, filesize))
                self.log.debug("file_mod_time({0}) = {1}"
                               .format(self.source_file, file_mod_time))

            except:
                self.log.error("Unable to create metadata dictionary.")
                raise

            try:
                self.log.debug("create metadata for source file...")
                # metadata = {
                #        "filename"       : ...,
                #        "source_path"     : ...,
                #        "relative_path"   : ...,
                #        "filesize"       : ...,
                #        "file_mod_time"    : ...,
                #        "file_create_time" : ...,
                #        "chunksize"      : ...
                #        }
                metadata["filesize"] = filesize
                metadata["file_mod_time"] = file_mod_time
                metadata["file_create_time"] = file_create_time
                metadata["chunksize"] = self.config["chunksize"]

                self.log.debug("metadata = {0}".format(metadata))
            except:
                self.log.error("Unable to assemble multi-part message.")
                raise

    def send_data(self, targets, metadata, open_connections, context):

        # no targets to send data to -> data can be removed
        # (after possible local storing)
        if not targets:
            self.config["remove_flag"] = True
            return

        # find the targets requesting for data
        targets_data = [i for i in targets if i[3] == "data"]

        # no targets to send data to
        if not targets_data:
            self.config["remove_flag"] = True
            return

        self.config["remove_flag"] = False
        chunksize = metadata["chunksize"]

        chunk_number = 0
        send_error = False

        # reading source file into memory
        try:
            self.log.debug("Opening '{0}'...".format(self.source_file))
            file_descriptor = open(str(self.source_file), "rb")
        except:
            self.log.error("Unable to read source file '{0}'"
                           .format(self.source_file), exc_info=True)
            raise

        self.log.debug("Passing multipart-message for file '{0}'..."
                       .format(self.source_file))
        # sending data divided into chunks
        while True:

            # read next chunk from file
            file_content = file_descriptor.read(chunksize)

            # detect if end of file has been reached
            if not file_content:
                break

            try:
                # assemble metadata for zmq-message
                chunk_metadata = metadata.copy()
                chunk_metadata["chunk_number"] = chunk_number

                chunk_payload = []
                chunk_payload.append(
                    json.dumps(chunk_metadata).encode("utf-8"))
                chunk_payload.append(file_content)
            except:
                self.log.error("Unable to pack multipart-message for file "
                               "'{0}'".format(self.source_file), exc_info=True)

            # send message to data targets
            try:
                send_to_targets(self.log, targets_data, self.source_file,
                                self.target_file, open_connections, None,
                                chunk_payload, context)
            except DataHandlingError:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                                                          chunk_number),
                               exc_info=True)
                send_error = True
            except:
                self.log.error("Unable to send multipart-message for file "
                               "'{0}' (chunk {1})".format(self.source_file,
                                                          chunk_number),
                               exc_info=True)

            chunk_number += 1

        # close file
        try:
            self.log.debug("Closing '{0}'...".format(self.source_file))
            file_descriptor.close()
        except:
            self.log.error("Unable to close target file '{0}'"
                           .format(self.source_file), exc_info=True)
            raise

        # do not remove data until a confirmation is sent back from the
        # priority target
        if self.config["remove_data"] == "with_confirmation":

            # notify cleanup

            self.config["remove_flag"] = False

        # the data was successfully sent -> mark it as removable
        elif not send_error:
            self.config["remove_flag"] = True

    def _datahandling(self, action_function, metadata):
        try:
            action_function(self.source_file, self.target_file)
        except IOError as e:

            # errno.ENOENT == "No such file or directory"
            if e.errno == errno.ENOENT:
                subdir, tmp = os.path.split(metadata["relative_path"])
                target_base_path = os.path.join(
                    self.target_file.split(subdir + os.sep)[0], subdir)

                if metadata["relative_path"] in self.config["fix_subdirs"]:
                    self.log.error("Unable to copy/move file '{0}' to '{1}': "
                                   "Directory {2} is not available"
                                   .format(self.source_file, self.target_file,
                                           metadata["relative_path"]))
                    raise
                elif (subdir in self.config["fix_subdirs"]
                        and not os.path.isdir(target_base_path)):
                    self.log.error("Unable to copy/move file '{0}' to '{1}': "
                                   "Directory {2} is not available"
                                   .format(self.source_file,
                                           self.target_file,
                                           subdir))
                    raise
                else:
                    try:
                        target_path, filename = os.path.split(self.target_file)
                        os.makedirs(target_path)
                        self.log.info("New target directory created: {0}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except OSError as e:
                        self.log.info("Target directory creation failed, was "
                                      "already created in the meantime: {0}"
                                      .format(target_path))
                        action_function(self.source_file, self.target_file)
                    except:
                        self.log.error("Unable to copy/move file '{0}' to "
                                       "'{1}'".format(self.source_file,
                                                      self.target_file),
                                       exc_info=True)
                        self.log.debug("target_path: {0}".format(target_path))
            else:
                self.log.error("Unable to copy/move file '{0}' to '{1}'"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                raise
        except:
            self.log.error("Unable to copy/move file '{0}' to '{1}'"
                           .format(self.source_file, self.target_file),
                           exc_info=True)
            raise

    def finish(self, targets, metadata,
               open_connections, context):

        targets_metadata = [i for i in targets if i[3] == "metadata"]

        if (self.config["store_data"]
                and self.config["remove_data"]
                and self.config["remove_flag"]):

            # move file
            try:
                self._datahandling(shutil.move, metadata)
                self.log.info("Moving file '{0}' ...success."
                              .format(self.source_file))
            except:
                self.log.error("Could not move file {0} to {1}"
                               .format(self.source_file, self.target_file),
                               exc_info=True)
                return

        elif self.config["store_data"]:

            # copy file
            # (does not preserve file owner, group or ACLs)
            try:
                self._datahandling(shutil.copy, metadata)
                self.log.info("Copying file '{0}' ...success."
                              .format(self.source_file))
            except:
                return

        elif self.config["remove_data"] and self.config["remove_flag"]:
            # remove file
            try:
                os.remove(self.source_file)
                self.log.info("Removing file '{0}' ...success."
                              .format(self.source_file))
            except:
                self.log.error("Unable to remove file {0}"
                               .format(self.source_file), exc_info=True)

            self.config["remove_flag"] = False

        # send message to metadata targets
        if targets_metadata:
            try:
                send_to_targets(self.log, targets_metadata, self.source_file,
                                self.target_file, open_connections, metadata,
                                None, context, self.config["send_timeout"])
                self.log.debug("Passing metadata multipart-message for file "
                               "{0}...done.".format(self.source_file))

            except:
                self.log.error("Unable to send metadata multipart-message for "
                               "file '{0}' to '{1}'"
                               .format(self.source_file, targets_metadata),
                               exc_info=True)

    def clean(self):
        pass

    def __exit__(self):
        self.clean()

    def __del__(self):
        self.clean()


if __name__ == '__main__':
    import time
    from shutil import copyfile
    from multiprocessing import Queue
    from logutils.queue import QueueHandler

    logfile = os.path.join(BASE_PATH, "logs", "file_fetcher.log")
    logsize = 10485760

    log_queue = Queue(-1)

    # Get the log Configuration for the lisener
    h1, h2 = helpers.get_log_handlers(logfile, logsize, verbose=True,
                                      onscreen_log_level="debug")

    # Start queue listener using the stream handler above
    log_queue_listener = helpers.CustomQueueListener(log_queue, h1, h2)
    log_queue_listener.start()

    # Create log and set handler to queue handle
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # Log level = DEBUG
    qh = QueueHandler(log_queue)
    root.addHandler(qh)

    receiving_port = "6005"
    receiving_port2 = "6006"
    ext_ip = "0.0.0.0"

    context = zmq.Context.instance()

    receiving_socket = context.socket(zmq.PULL)
    connection_str = "tcp://{0}:{1}".format(ext_ip, receiving_port)
    receiving_socket.bind(connection_str)
    logging.info("=== receiving_socket connected to {0}"
                 .format(connection_str))

    receiving_socket2 = context.socket(zmq.PULL)
    connection_str = "tcp://{0}:{1}".format(ext_ip, receiving_port2)
    receiving_socket2.bind(connection_str)
    logging.info("=== receiving_socket2 connected to {0}"
                 .format(connection_str))

    prework_source_file = os.path.join(BASE_PATH, "test_file.cbf")
    prework_target_file = os.path.join(
        BASE_PATH, "data", "source", "local", "100.cbf")

    copyfile(prework_source_file, prework_target_file)
    time.sleep(0.5)

    metadata = {
        "source_path": os.path.join(BASE_PATH, "data", "source"),
        "relative_path": os.sep + "local",
        "filename": "100.cbf"
    }
    targets = [['localhost:{0}'.format(receiving_port), 1, [".cbf"], "data"],
               ['localhost:{0}'.format(receiving_port2), 0, [".cbf"], "data"]]

    chunksize = 10485760  # = 1024*1024*10 = 10 MiB
    local_target = os.path.join(BASE_PATH, "data", "target")
    open_connections = dict()

    config = {
        "fix_subdirs": ["commissioning", "current", "local"],
        "store_data": False,
        "remove_data": False,
        "chunksize": chunksize,
        "local_target": None
    }

    logging.debug("open_connections before function call: {0}"
                  .format(open_connections))

    datafetcher = DataFetcher(config, log_queue, 0)

    datafetcher.setup()

    datafetcher.get_metadata(targets, metadata)

    datafetcher.send_data(targets, metadata, open_connections, context)

    datafetcher.finish(targets, metadata, open_connections, context)

    logging.debug("open_connections after function call: {0}"
                  .format(open_connections))

    try:
        recv_message = receiving_socket.recv_multipart()
        logging.info("=== received: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
        recv_message = receiving_socket2.recv_multipart()
        logging.info("=== received 2: {0}"
                     .format(json.loads(recv_message[0].decode("utf-8"))))
    except KeyboardInterrupt:
        pass
    finally:
        receiving_socket.close(0)
        receiving_socket2.close(0)
        context.destroy()
