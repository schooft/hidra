# Copyright (C) 2015  DESY, Manuela Kuhn, Notkestr. 85, D-22607 Hamburg
#
# HiDRA is a generic tool set for high performance data multiplexing with
# different qualities of service and based on Python and ZeroMQ.
#
# This software is free: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# This software is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this software.  If not, see <http://www.gnu.org/licenses/>.
#
# Authors:
#     Manuela Kuhn <manuela.kuhn@desy.de>
#

"""
This module implements an example for resetting the status of the receiver
after an error occured.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

import socket

import _environment  # noqa F401 # pylint: disable=unused-import
from hidra.control import ReceiverControl


def main():
    """
    Reset the status of the receiver after an error occured while transvering
    file with a hidra instance running with stop_on_error.
    """

    host = socket.getfqdn()
    control = ReceiverControl(host)

    status = control.get_status()
    print("status before reset", status)

    control.reset_status()

    status = control.get_status()
    print("status after reset", status)


if __name__ == "__main__":
    main()
