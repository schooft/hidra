#!/usr/bin/env python

from __future__ import print_function
import __init__  # noqa F401
import utils
import argparse


def get_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--config_file",
                        type=str,
                        default="/opt/hidra/conf/datamanager.conf",
                        help="Location of the configuration file")

    return parser.parse_args()


if __name__ == "__main__":

    args = get_arguments()

    config_file = args.config_file
    params = utils.parse_parameters(utils.read_config(config_file))["asection"]

    print("Configured settings:")
    print("Monitored direcory:            {}".format(params["monitored_dir"]))
    print("Watched subdirectories are:    {}".format(params["fix_subdirs"]))

    msg = "Data is written to:            {}"
    if params["store_data"]:
        print(msg.format(params["local_target"]))
    else:
        print(msg.format("Data is not stored locally"))

    msg = "Data is sent to:               {}"
    if params["use_data_stream"]:
        print(msg.format(params["data_stream_targets"]))
    else:
        print(msg.format("Data is not sent as priority stream anywhere"))

    print("Remove data from the detector: {}".format(params["remove_data"]))
    print("Whitelist:                     {}".format(params["whitelist"]))