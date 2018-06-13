"""Unittest suite for HiDRA
"""

from __future__ import print_function
from __future__ import unicode_literals
from __future__ import absolute_import

import unittest
from importlib import import_module
import pkgutil

import eventdetectors
import datafetchers

__author__ = 'Manuela Kuhn <manuela.kuhn@desy.de>'


def get_eventdetector_suites():
    """Collects all available eventdetector tests

    Returns:
        An array containing all available eventdetector test suites.
    """

    all_suites = []

    # find all event detector test modules
    # iter_modules returns: importer, modname, ispkg
    for _, modname, _ in pkgutil.iter_modules(eventdetectors.__path__):
        # the base class not a test module
        if modname in ["eventdetector_test_base"]:
            continue

        # load the test suite
        module_name = "eventdetectors.{}".format(modname)
        module = import_module(module_name).TestEventDetector
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from eventdetectors.test_inotifyx_events \
        # >     import TestEventDetector as TestInotifyxEvents
        # > loader = unittest.TestLoader()
        # > suite = loader.loadTestsFromTestCase(TestInotifyxEvents)

        # add the test suite
        all_suites.append(suite)

    return all_suites


def get_datafetcher_suites():
    """Collects all available datafetcher tests

    Returns:
        An array containing all available datafetcher test suites.
    """

    all_suites = []

    # find all event detector test modules
    # iter_modules returns: importer, modname, ispkg
    for _, modname, _ in pkgutil.iter_modules(datafetchers.__path__):
        # the base class not a test module
        # TODO exclude test_http_fetcher only temporarily till bug is fixed
        if modname in ["datafetcher_test_base", "test_http_fetcher"]:
            continue

        # load the test suite
        module_name = "datafetchers.{}".format(modname)
        module = import_module(module_name).TestDataFetcher
        suite = unittest.TestLoader().loadTestsFromTestCase(module)
        # this is equivalent to loading one module like this
        # > from datafetchers.test_file_fetcher \
        # >     import TestDataFetcher as TestFileFetcher
        # > loader = unittest.TestLoader()
        # > suite = loader.loadTestsFromTestCase(TestFileFetcher)

        # add the test suite
        all_suites.append(suite)

    return all_suites


def get_suite():
    """Collect all tests to be performed as one suite.

    Returns:
        A unittest TestSuite instance containing all unittests to be performed.
    """

    all_suites = []

    # for testing
#    from eventdetectors.test_inotifyx_events import TestEventDetector as TestInotifyxEvents
#    from eventdetectors.test_watchdog_events import TestEventDetector as TestWatchdogEvents
#    from eventdetectors.test_http_events import TestEventDetector as TestHttpEvents
#    from eventdetectors.test_zmq_events import TestEventDetector as TestZmqEvents
#    from eventdetectors.test_hidra_events import TestEventDetector as TestHidraEvents
#
#    from datafetchers.test_cleanerbase import TestDataFetcher as TestCleanerbase
#    from datafetchers.test_file_fetcher import TestDataFetcher as TestFileFetcher
#    from datafetchers.test_http_fetcher import TestDataFetcher as TestHttpFetcher
#    from datafetchers.test_zmq_fetcher import TestDataFetcher as TestZmqFetcher
#    from datafetchers.test_hidra_fetcher import TestDataFetcher as TestHidraFetcher
#
#    all_suites += [
#        unittest.TestLoader().loadTestsFromTestCase(TestInotifyxEvents),
#        unittest.TestLoader().loadTestsFromTestCase(TestWatchdogEvents),
#        unittest.TestLoader().loadTestsFromTestCase(TestHttpEvents),
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqEvents),
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraEvents),
#
#        unittest.TestLoader().loadTestsFromTestCase(TestCleanerbase),
#        unittest.TestLoader().loadTestsFromTestCase(TestFileFetcher),
#        unittest.TestLoader().loadTestsFromTestCase(TestZmqFetcher),
#        unittest.TestLoader().loadTestsFromTestCase(TestHidraFetcher),
#    ]

    # get the subsuites
    # BUG: if the event detectors are tested before the datafetchers the
    # program does not stop
    all_suites += get_datafetcher_suites()
    all_suites += get_eventdetector_suites()

    # combine all subsuites to one big one
    suite = unittest.TestSuite(all_suites)

    return suite


def main():
    """Run the test suite.
    """

    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(get_suite())


if __name__ == '__main__':
    main()
