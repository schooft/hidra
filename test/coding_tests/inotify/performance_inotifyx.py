# Copyright (c) 2005 Manuel Amador
# Copyright (c) 2009-2011 Forest Bond
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import multiprocessing
import os
import select
import shutil
import sys
import threading
import time

from inotifyx import binding
from inotifyx.distinfo import version as __version__


constants = {}

for name in dir(binding):
    if name.startswith('IN_'):
        globals()[name] = constants[name] = getattr(binding, name)


class InotifyEvent(object):
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


def get_events(fd, *args):
    '''
    get_events(fd[, timeout])

    Return a list of InotifyEvent instances representing events read from
    inotify.  If timeout is None, this will block forever until at least one
    event can be read.  Otherwise, timeout should be an integer or float
    specifying a timeout in seconds.  If get_events times out waiting for
    events, an empty list will be returned.  If timeout is zero, get_events
    will not block.
    '''
    return [
      InotifyEvent(wd, mask, cookie, name)
      for wd, mask, cookie, name in binding.get_events(fd, *args)
    ]


def create_test_files(watch_dir, n_files):
    t_start = time.time()
    for i in range(n_files):
        with open(os.path.join(watch_dir, "test_file"), "w") as f:
            pass

    t_needed = time.time() - t_start
    print("created {} in {} s, ({} Hz)".format(n_files, t_needed, n_files / t_needed))


def create_and_get_events(watch_dir, n_files, use_pr):
    if use_pr:
        print("use multiprocessing")
        job_type = multiprocessing.Process
    else:
        print("use threading")
        job_type = threading.Thread

    create_pr = job_type(
        target=create_test_files,
        args=(watch_dir, n_files)
    )

    try:
        os.mkdir(watch_dir)
    except OSError:
        pass

    fd = binding.init()

    wd_to_path = {}

    try:
        wd = binding.add_watch(fd, watch_dir)
        wd_to_path[wd] = watch_dir
    except:
        print("stopped")
        os.close(fd)
        sys.exit(1)

    create_pr.start()

    n_events = 0
    timeout = 2
    t = time.time()
    run_loop = True
    try:
        while run_loop:
            events = get_events(fd, timeout)

            if not events:
                run_loop = False

            for event in events:
                if event.wd < 0:
                    continue

                path = wd_to_path[event.wd]
                event_type = event.get_mask_description()
                event_type_array = event_type.split("|")

                if "IN_OPEN" in event_type_array:
                    n_events += 1

    except KeyboardInterrupt:
        pass
    finally:
        os.close(fd)

    t_needed = time.time() - t
    print("n_events {} in {} s, ({} Hz)".format(n_events, t_needed, n_events / t_needed))
    create_pr.join()


def _main():
    watch_dir = "/tmp/watch_tree"
    n_files = 1000000

    use_pr = True
    create_and_get_events(watch_dir, n_files, use_pr)

    use_pr = False
    create_and_get_events(watch_dir, n_files, use_pr)


if __name__ == '__main__':
    _main()
