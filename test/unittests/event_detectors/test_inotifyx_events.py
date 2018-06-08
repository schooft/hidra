from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import unittest
import os
import time
import logging
from shutil import copyfile

from .__init__ import BASE_DIR
from .test_eventdetector_base import TestEventDetectorBase
from inotifyx_events import EventDetector


class TestEventDetector(TestEventDetectorBase):

    def setUp(self):
        super(TestEventDetector, self).setUp()

        # methods inherited from parent class
        # explicit definition here for better readability
        self._init_logging = super(TestEventDetector, self)._init_logging
        self._create_dir = super(TestEventDetector, self)._create_dir

        self.config = {
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {
                "IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                "IN_MOVED_TO": [".log"]
            },
            # "event_timeout": 0.1,
            "history_size": 0,
            "use_cleanup": False,
            "time_till_closed": 5,
            "action_time": 120
        }

        self.start = 100
        self.stop = 110

        self.source_file = os.path.join(BASE_DIR, "test_1024B.file")

        self.target_base_path = os.path.join(BASE_DIR, "data", "source")
        self.target_relative_path = os.path.join("local", "raw")

        self.target_file_base = os.path.join(self.target_base_path,
                                             self.target_relative_path)
        # TODO why is this needed?
        self.target_file_base += os.sep

        self.eventdetector = None

    def _start_eventdetector(self):
        self.eventdetector = EventDetector(self.config, self.log_queue)

    def test_eventdetector(self):
        self._init_logging()
        self._create_dir(self.target_file_base)
        self._start_eventdetector()

        for i in range(self.start, self.stop):

            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            print("copy {}".format(target_file))
            copyfile(self.source_file, target_file)
            time.sleep(0.1)

            event_list = self.eventdetector.get_new_event()
            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }

            try:
                self.assertEqual(len(event_list), 1)
                self.assertDictEqual(event_list[0],
                                     expected_result_dict)
            except AssertionError:
                print("event_list", event_list)
                raise

    # this should not be executed automatically only if needed for debugging
    @unittest.skip("Only needed for debugging")
    def test_memory_usage(self):
        import resource
        import gc
        # don't care about stuff that would be garbage collected properly
        gc.collect()
        from guppy import hpy

        self._init_logging(loglevel="info")
        self._create_dir(self.target_file_base)
        self._start_eventdetector()

#        self.config["use_cleanup"] = True
#        self.config["monitored_events"] = {
#            "Some_supid_event": [".tif", ".cbf", ".file"]
#        }
#        self.config["time_till_closed"] = 0.2
#        self.config["action_time"] = 0.5

        self.start = 100
        self.stop = 30000
        steps = 10

        memory_usage_old = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print("Memory usage at start: {} (kb)".format(memory_usage_old))

        hp = hpy()
        hp.setrelheap()

        step_loop = (self.stop - self.start) / steps
        print("Used steps:", steps)

        try:
            for s in range(steps):
                start = self.start + s * step_loop
                stop = start + step_loop
#                print ("start=", start, "stop=", stop)
                for i in range(start, stop):

                    target_file = "{}{}.cbf".format(self.target_file_base, i)
                    copyfile(self.source_file, target_file)
                    time.sleep(0.1)

                    if i % 100 == 0:
                        print("copy index {}".format(i))
                        event_list = self.eventdetector.get_new_event()

#                    time.sleep(0.5)

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                print("Memory usage in iteration {}: {} (kb)"
                      .format(s, memory_usage_new))
                if memory_usage_new > memory_usage_old:
                    memory_usage_old = memory_usage_new
                    print(hp.heap())

        except KeyboardInterrupt:
            pass
        finally:
            if self.config["use_cleanup"]:
                time.sleep(4)
                event_list = self.eventdetector.get_new_event()
                print("len of event_list={}".format(len(event_list)))

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                print("Memory usage: {} (kb)".format(memory_usage_new))
                time.sleep(1)

                event_list = self.eventdetector.get_new_event()
                print("len of event_list={}".format(len(event_list)))

                memory_usage_new = (
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
                print("Memory usage: {} (kb)".format(memory_usage_new))

                event_list = self.eventdetector.get_new_event()
                print("len of event_list={}".format(len(event_list)))

            memory_usage_new = (
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            print("Memory usage before stop: {} (kb)"
                  .format(memory_usage_new))
            time.sleep(5)

    def tearDown(self):
        if self.eventdetector is not None:
            self.eventdetector.stop()
            self.eventdetector = None

        # clean up the created files
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                os.remove(target_file)
                logging.debug("remove {}".format(target_file))
            except OSError:
                pass

        super(TestEventDetector, self).tearDown()


if __name__ == '__main__':
    unittest.main()