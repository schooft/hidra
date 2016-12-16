from __future__ import print_function
from __future__ import unicode_literals
from six import iteritems

import os
import logging
from inotifyx import binding
# from inotifyx.distinfo import version as __version__
import collections
import threading
import time
import copy

from logutils.queue import QueueHandler
import helpers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


constants = {}
file_event_list = []

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


# Source: inotifyx library code example
# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
class InotifyEvent (object):
    '''
    InotifyEvent(wd, mask, cookie, name)

    A representation of the inotify_event structure.  See the inotify
    documentation for a description of these fields.
    '''

    wd = None
    mask = None
    cookie = None
    name = None

    def __init__(self, wd, mask, cookie, name):
        self.wd = wd
        self.mask = mask
        self.cookie = cookie
        self.name = name

    def __str__(self):
        return '%s: %s' % (self.wd, self.get_mask_description())

    def __repr__(self):
        return '%s(%s, %s, %s, %s)' % (
            self.__class__.__name__,
            repr(self.wd),
            repr(self.mask),
            repr(self.cookie),
            repr(self.name),
        )

    def get_mask_description(self):
        '''
        Return an ASCII string describing the mask field in terms of
        bitwise-or'd IN_* constants, or 0.  The result is valid Python code
        that could be eval'd to get the value of the mask field.  In other
        words, for a given event:

        >>> from inotifyx import *
        >>> assert (event.mask == eval(event.get_mask_description()))
        '''

        parts = []
        for name, value in constants.items():
            if self.mask & value:
                parts.append(name)
        if parts:
            return '|'.join(parts)
        return '0'


def get_event_message(path, filename, paths):

    parent_dir = path
    relative_path = ""
    event_message = {}

    # traverse the relative path till the original path is reached
    # e.g. created file: /source/dir1/dir2/test.tif
    while True:
        if parent_dir not in paths:
            (parent_dir, rel_dir) = os.path.split(parent_dir)
            # the os.sep is needed at the beginning because the relative path
            # is built up from the right
            # e.g.
            # self.paths = ["/tmp/test/source"]
            # path = /tmp/test/source/local/testdir
            # first iteration:  parent_dir = /tmp/test/source/local,
            #                   rel_dir = /testdir
            # second iteration: parent_dir = /tmp/test/source,
            #                   rel_dir = /local/testdir
            relative_path = os.sep + rel_dir + relative_path
        else:
            # remove beginning "/"
            if relative_path.startswith(os.sep):
                relative_path = os.path.normpath(relative_path[1:])
            else:
                relative_path = os.path.normpath(relative_path)

            # the event for a file /tmp/test/source/local/file1.tif is of
            # the form:
            # {
            #   "source_path" : "/tmp/test/source"
            #   "relative_path": "local"
            #   "filename"   : "file1.tif"
            # }
            event_message = {
                "source_path": parent_dir,
                "relative_path": relative_path,
                "filename": filename
            }

            return event_message


class CleanUp (threading.Thread):
    def __init__(self, paths, mon_subdirs, mon_suffixes, cleanup_time,
                 action_time, lock, log_queue):
        self.log = self.get_logger(log_queue)

        self.log.debug("init")
        self.paths = paths

        self.mon_subdirs = mon_subdirs
        self.mon_suffixes = mon_suffixes

        self.cleanup_time = cleanup_time
        self.action_time = action_time

        self.lock = lock

        self.log.debug("threading.Thread init")
        threading.Thread.__init__(self)

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("CleanUp")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def run(self):
        global file_event_list
        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]

        while True:
            try:
                result = []
                for dirname in dirs_to_walk:
                    result += self.traverse_directory(dirname)

                self.lock.acquire()
                file_event_list += result
                self.lock.release()
#                self.log.debug("file_event_list: {0}".format(file_event_list))
                time.sleep(self.action_time)
            except:
                self.log.error("Stopping loop due to error", exc_info=True)
                self.lock.release()
                break

    def traverse_directory(self, dirname):
        event_list = []

        for root, directories, files in os.walk(dirname):
            for filename in files:
                if not filename.endswith(self.mon_suffixes):
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #               "{0}".format(filename))
                    continue

                filepath = os.path.join(root, filename)
                self.log.debug("filepath: {0}".format(filepath))

                try:
                    time_last_modified = os.stat(filepath).st_mtime
                except:
                    self.log.error("Unable to get modification time for file: "
                                   "{0}".format(filepath), exc_info=True)
                    continue

                try:
                    # get current time
                    time_current = time.time()
                except:
                    self.log.error("Unable to get current time for file: {0}"
                                   .format(filepath), exc_info=True)
                    continue

                if time_current - time_last_modified >= self.cleanup_time:
                    self.log.debug("New closed file detected: {0}"
                                   .format(filepath))
#                    self.log.debug("modTime: {0}, currentTime: {1}"
#                                   .format(time_last_modified, time_current))
#                    self.log.debug("time_current - time_last_modified: {0}, "
#                                   "cleanup_time: {1}"
#                                   .format(
#                                       (time_current - time_last_modified),
#                                       self.cleanup_time))
                    event_message = get_event_message(root, filename,
                                                      self.paths)
                    self.log.debug("event_message: {0}".format(event_message))

                    # add to result list
                    event_list.append(event_message)

        return event_list


class EventDetector():

    def __init__(self, config, log_queue):

        self.log = self.get_logger(log_queue)

        required_params = ["monitored_dir",
                           "fix_subdirs",
                           "monitored_events",
                           # "event_timeout",
                           "history_size",
                           "use_cleanup",
                           "time_till_closed",
                           "action_time"]

        # Check format of config
        check_passed, config_reduced = helpers.check_config(required_params,
                                                            config,
                                                            self.log)

        self.wd_to_path = {}
        self.fd = binding.init()

        # Only proceed if the configuration was correct
        if check_passed:
            self.log.info("Configuration for event detector: {0}"
                          .format(config_reduced))

            # TODO why is this necessary
            self.paths = [config["monitored_dir"]]

            self.mon_subdirs = config["fix_subdirs"]

            suffix_list = []
            for key, value in iteritems(config["monitored_events"]):
                suffix_list += value
            self.mon_suffixes = tuple(suffix_list)

            self.mon_events = config["monitored_events"]

            # TODO decide if this should go into config
#            self.timeout = config["event_timeout"]
            self.timeout = 1

            self.history = collections.deque(maxlen=config["history_size"])

            self.cleanup_time = config["time_till_closed"]
            self.action_time = config["action_time"]

            self.lock = threading.Lock()

            self.add_watch()

            if config["use_cleanup"]:
                self.cleanup_thread = CleanUp(self.paths, self.mon_subdirs,
                                              self.mon_suffixes,
                                              self.cleanup_time,
                                              self.action_time,
                                              self.lock, log_queue)
                self.cleanup_thread.start()

        else:
            self.log.debug("config={0}".format(config))
            raise Exception("Wrong configuration")

    # Modification of the inotifyx example found inside inotifyx library
    # Copyright (c) 2005 Manuel Amador
    # Copyright (c) 2009-2011 Forest Bond
    def get_events(self, fd, *args):
        '''
        get_events(fd[, timeout])

        Return a list of InotifyEvent instances representing events read from
        inotify. If timeout is None, this will block forever until at least one
        event can be read.  Otherwise, timeout should be an integer or float
        specifying a timeout in seconds.  If get_events times out waiting for
        events, an empty list will be returned.  If timeout is zero, get_events
        will not block.
        '''
        return [
            InotifyEvent(wd, mask, cookie, name)
            for wd, mask, cookie, name in binding.get_events(fd, *args)
        ]

    # Send all logs to the main process
    # The worker configuration is done at the start of the worker process run.
    # Note that on Windows you can't rely on fork semantics, so each process
    # will run the logging configuration code when it starts.
    def get_logger(self, queue):
        # Create log and set handler to queue handle
        h = QueueHandler(queue)  # Just the one handler needed
        logger = logging.getLogger("inotifyx_events")
        logger.propagate = False
        logger.addHandler(h)
        logger.setLevel(logging.DEBUG)

        return logger

    def add_watch(self):
        try:
            for path in self.get_directory_structure():
                wd = binding.add_watch(self.fd, path)
                self.wd_to_path[wd] = path
                self.log.debug("Register watch for path: {0}".format(path))
        except:
            self.log.error("Could not register watch for path: {0}"
                           .format(path), exc_info=True)

    def get_directory_structure(self):
        # Add the default subdirs
        self.log.debug("paths: {0}".format(self.paths))
        dirs_to_walk = [os.path.normpath(os.path.join(self.paths[0],
                                                      directory))
                        for directory in self.mon_subdirs]
        self.log.debug("dirs_to_walk: {0}".format(dirs_to_walk))
        monitored_dirs = []

        # Walk the tree
        for directory in dirs_to_walk:
            if os.path.isdir(directory):
                monitored_dirs.append(directory)
                for root, directories, files in os.walk(directory):
                    # Add the found dirs to the list for the inotify-watch
                    if root not in monitored_dirs:
                        monitored_dirs.append(root)
                        self.log.info("Add directory to monitor: {0}"
                                      .format(root))
            else:
                self.log.info("Dir does not exist: {0}".format(directory))

        return monitored_dirs

    def get_new_event(self):
        global file_event_list

        try:
            self.lock.acquire()
            # get missed files
            event_message_list = copy.deepcopy(file_event_list)
            file_event_list = []
        finally:
            self.lock.release()

        if event_message_list:
            self.log.info("Added missed files: {0}".format(event_message_list))

        event_message = {}

        events = self.get_events(self.fd, self.timeout)
        removed_wd = None

        for event in events:

            if not event.name:
                continue

            try:
                path = self.wd_to_path[event.wd]
            except:
                path = removed_wd
            parts = event.get_mask_description()
            parts_array = parts.split("|")

            is_dir = ("IN_ISDIR" in parts_array)
            is_created = ("IN_CREATE" in parts_array)
            is_moved_from = ("IN_MOVED_FROM" in parts_array)
            is_moved_to = ("IN_MOVED_TO" in parts_array)

            current_mon_event = None
            for key, value in iteritems(self.mon_events):
                if key in parts_array:
                    current_mon_event = key

#            if not is_dir:
#                self.log.debug("{0} {1} {2}".format(path, event.name, parts)
#                self.log.debug("current_mon_event: {0}"
#                               .format(current_mon_event))
#            self.log.debug(event.name)
#            self.log.debug("is_dir: {0}".format(is_dir))
#            self.log.debug("is_created: {0}".format(is_created))
#            self.log.debug("is_moved_from: {0}".format(is_moved_from))
#            self.log.debug("is_moved_to: {0}".format(is_moved_to))

            # if a new directory is created or a directory is renamed inside
            # the monitored one, this one has to be monitored as well
            if is_dir and (is_created or is_moved_to):

                # self.log.debug("is_dir and is_created: {0} or is_moved_to: "
                #                "{1}".format(is_created, is_moved_to))
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                self.log.info("Directory event detected: {0}, {1}"
                              .format(dirname, parts))
                if dirname in self.paths:
                    self.log.debug("Directory already contained in path list:"
                                   " {0}".format(dirname))
                else:
                    wd = binding.add_watch(self.fd, dirname)
                    self.wd_to_path[wd] = dirname
                    self.log.info("Added new directory to watch: {0}"
                                  .format(dirname))

                    # because inotify misses subdirectory creations if they
                    # happen to fast, the newly created directory has to be
                    # walked to get catch this misses
                    # http://stackoverflow.com/questions/15806488/
                    #        inotify-missing-events
                    traversed_path = dirname
                    for root, directories, files in os.walk(dirname):
                        # Add the found dirs to the list for the inotify-watch
                        for dname in directories:
                            traversed_path = os.path.join(traversed_path,
                                                          dname)
                            wd = binding.add_watch(self.fd, traversed_path)
                            self.wd_to_path[wd] = traversed_path
                            self.log.info("Added new subdirectory to watch: "
                                          "{0}".format(traversed_path))
                        self.log.debug("files: {0}".format(files))
                        for filename in files:
                            # self.log.debug("filename: {0}".format(filename))
                            if not filename.endswith(self.mon_suffixes):
                                self.log.debug("File ending not in monitored "
                                               "Suffixes: {0}"
                                               .format(filename))
                                self.log.debug("detected events were: {0}"
                                               .format(parts))
                                continue
                            event_message = self.get_event_message(path,
                                                                   filename,
                                                                   self.paths)
                            self.log.debug("event_message: {0}"
                                           .format(event_message))
                            event_message_list.append(event_message)
#                            self.log.debug("event_message_list: {0}"
#                                           .format(event_message_list))
                continue

            # if a directory is renamed the old watch has to be removed
            if is_dir and is_moved_from:

                # self.log.debug("is_dir and is_moved_from")
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts)
                # self.log.debug(event.name)

                dirname = os.path.join(path, event.name)
                for watch, watchPath in iteritems(self.wd_to_path):
                    if watchPath == dirname:
                        found_watch = watch
                        break
                binding.rm_watch(self.fd, found_watch)
                self.log.info("Removed directory from watch: {0}"
                              .format(dirname))
                # the IN_MOVE_FROM event always apears before the IN_MOVE_TO
                # (+ additional) events and thus has to be stored till loop
                # is finished
                removed_wd = self.wd_to_path[found_watch]
                # removing the watch out of the dictionary cannot be done
                # inside the loop (would throw error: dictionary changed size
                # during iteration)
                del self.wd_to_path[found_watch]
                continue

            # only files of the configured event type are send
            if (not is_dir and current_mon_event
                    and [path, event.name] not in self.history):

                # self.log.debug("not is_dir")
                # self.log.debug("current_mon_event: {0}"
                #                .format(current_mon_event))
                # self.log.debug("{0} {1} {2}".format(path, event.name, parts)
                # self.log.debug(event.name)

                # only files ending with a suffix specified with the current
                # event are monitored
                if (not event.name.endswith(
                        tuple(self.mon_events[current_mon_event]))):
                    # self.log.debug("File ending not in monitored Suffixes: "
                    #                "{0}".format(event.name))
                    # self.log.debug("detected events were: {0}".format(parts))
                    continue

                event_message = get_event_message(path, event.name, self.paths)
                self.log.debug("event_message {0}".format(event_message))
                event_message_list.append(event_message)

                self.history.append([path, event.name])

        return event_message_list

    def stop(self):
        try:
            for wd in self.wd_to_path:
                try:
                    binding.rm_watch(self.fd, wd)
                except:
                    self.log.error("Unable to remove watch: {0}".format(wd),
                                   exc_info=True)
        finally:
            os.close(self.fd)

    def __exit__(self):
        self.stop()

    def __del__(self):
        self.stop()


if __name__ == '__main__':
    from subprocess import call
    from multiprocessing import Queue

    from eventdetectors import BASE_PATH

    logfile = os.path.join(BASE_PATH, "logs", "inotifyx_events.log")
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

    config = {
        "monitored_dir": os.path.join(BASE_PATH, "data", "source"),
        "fix_subdirs": ["commissioning", "current", "local"],
        "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf"],
                             "IN_MOVED_TO": [".log"]},
        # "event_timeout": 0.1,
        "history_size": 0,
        "use_cleanup": False,
        "time_till_closed": 5,
        "action_time": 120
    }

    source_file = os.path.join(BASE_PATH, "test_file.cbf")
    target_file_base = os.path.join(
        BASE_PATH, "data", "source", "local", "raw") + os.sep

    if not os.path.isdir(target_file_base):
        os.mkdir(target_file_base)

    eventdetector = EventDetector(config, log_queue)

    i = 100
    while i <= 110:
        try:
            logging.debug("copy")
            target_file = "{0}{1}.cbf".format(target_file_base, i)
            call(["cp", source_file, target_file])
#            copyfile(source_file, target_file)
            i += 1

            event_list = eventdetector.get_new_event()
            if event_list:
                print ("event_list:", event_list)

            time.sleep(1)
        except KeyboardInterrupt:
            break

    for number in range(100, i):
        target_file = "{0}{1}.cbf".format(target_file_base, number)
        logging.debug("remove {0}".format(target_file))
        os.remove(target_file)

    log_queue.put_nowait(None)
    log_queue_listener.stop()