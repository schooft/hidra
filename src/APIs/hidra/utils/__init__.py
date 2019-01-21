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

"""Set up environment.
"""

from __future__ import absolute_import
from __future__ import print_function
from __future__ import unicode_literals

from .utils_general import (is_windows,
                            is_linux,
                            check_type,
                            check_any_sub_dir_exists,
                            check_sub_dir_contained,
                            check_all_sub_dir_exist,
                            check_existance,
                            check_writable,
                            check_version,
                            check_host,
                            check_ping,
                            create_dir,
                            create_sub_dirs,
                            change_user,
                            log_user_change)

from .utils_datatypes import (IpcAddresses,
                              Endpoints,
                              MAPPING_ZMQ_CONSTANTS_TO_STR)

from .utils_config import (check_config,
                           load_config,
                           parse_parameters,
                           set_parameters,
                           update_dict,
                           map_conf_format,
                           WrongConfiguration)

from .utils_logging import (CustomQueueListener,
                            get_stream_log_handler,
                            get_file_log_handler,
                            get_log_handlers,
                            get_logger,
                            init_logging,
                            LoggingFunction)

from .utils_network import (execute_ldapsearch,
                            extend_whitelist,
                            convert_socket_to_fqdn,
                            is_ipv6_address,
                            get_socket_id,
                            generate_sender_id,
                            set_ipc_addresses,
                            set_endpoints,
                            start_socket,
                            stop_socket)

from .utils_api import (NotSupported,
                        UsageError,
                        FormatError,
                        ConnectionFailed,
                        VersionError,
                        AuthenticationFailed,
                        CommunicationFailed,
                        DataSavingError,
                        Base)

__all__ = [
    "is_windows",
    "is_linux",
    "check_type",
    "check_any_sub_dir_exists",
    "check_sub_dir_contained",
    "check_all_sub_dir_exist",
    "check_existance",
    "check_writable",
    "check_version",
    "check_host",
    "check_ping",
    "create_dir",
    "create_sub_dirs",
    "IpcAddresses",
    "Endpoints",
    "MAPPING_ZMQ_CONSTANTS_TO_STR",
    "check_config",
    "load_config",
    "parse_parameters",
    "set_parameters",
    "update_dict",
    "WrongConfiguration",
    "CustomQueueListener",
    "get_stream_log_handler",
    "get_file_log_handler",
    "get_log_handlers",
    "get_logger",
    "init_logging",
    "LoggingFunction",
    "execute_ldapsearch",
    "extend_whitelist",
    "convert_socket_to_fqdn",
    "is_ipv6_address",
    "get_socket_id",
    "generate_sender_id",
    "set_ipc_addresses",
    "set_endpoints",
    "start_socket",
    "stop_socket",
    "NotSupported",
    "UsageError",
    "FormatError",
    "ConnectionFailed",
    "VersionError",
    "AuthenticationFailed",
    "CommunicationFailed",
    "DataSavingError",
    "Base"
]