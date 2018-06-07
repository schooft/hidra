import unittest
import os
import time
import logging
from shutil import copyfile

from __init__ import BASE_DIR
from inotifyx_events import EventDetector


class TestInotifyxEvents(unittest.TestCase):

    def setUp(self):
        # Create log and set handler
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)  # Log level = DEBUG

        self.config = {
            "monitored_dir": os.path.join(BASE_DIR, "data", "source"),
            "fix_subdirs": ["commissioning", "current", "local"],
            "monitored_events": {"IN_CLOSE_WRITE": [".tif", ".cbf", ".file"],
                                 "IN_MOVED_TO": [".log"]},
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

        self.target_file_base = (
            os.path.join(self.target_base_path, self.target_relative_path)
            + os.sep)

        if not os.path.isdir(self.target_file_base):
            os.mkdir(self.target_file_base)

        self.eventdetector = EventDetector(self.config, False)

    def test_eventdetector(self):

        for i in range(self.start, self.stop):

            print("copy")
            filename = "{}.cbf".format(i)
            target_file = "{}{}".format(self.target_file_base, filename)
            copyfile(self.source_file, target_file)
            time.sleep(0.1)

            event_list = self.eventdetector.get_new_event()
            expected_result_dict = {
                u'filename': filename,
                u'source_path': self.target_base_path,
                u'relative_path': self.target_relative_path
            }

            self.assertEqual(len(event_list), 1)
            self.assertDictEqual(event_list[0],
                                 expected_result_dict)

    def tearDown(self):
        self.eventdetector.stop()
        for number in range(self.start, self.stop):
            try:
                target_file = "{}{}.cbf".format(self.target_file_base, number)
                print("remove {}".format(target_file))
                os.remove(target_file)
            except OSError:
                pass


if __name__ == '__main__':
    unittest.main()