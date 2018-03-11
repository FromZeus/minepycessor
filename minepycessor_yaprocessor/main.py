import argparse
import logging
import time

from bus import DBBus
import logger
from yaprocessor import YaProcessor


def main():
    try:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "conf",
            help="Path to .yml file with client id and scopes"
        )
        parser.add_argument(
            "-s", "--screen", dest="screen_name",
            help="Name of the server's screen"
        )
        parser.add_argument(
            "--loglevel", dest="loglevel",
            choices=[
                "CRITICAL", "ERROR", "WARNING",
                "INFO", "DEBUG", "NOTSET"
            ],
            help="Logging level"
        )
        parser.add_argument(
            "-l", "--log", dest="log",
            help="Redirect logging to file"
        )
        args = parser.parse_args()

        with open(args.conf, "r") as f:
            conf = yaml.load(f)

        if args.screen_name is not None:
            conf["screen_name"] = args.screen_name
        if args.loglevel is not None:
            conf["loglevel"] = args.loglevel

        global log

        if args.log is not None:
            log = logging.getLogger(__name__)
            log.addHandler(logger.FileHandler(args.log))
            log.setLevel(getattr(logging, conf["loglevel"]))
        else:
            log = logging.getLogger(__name__)
            log.addHandler(logger.StreamHandler())
            log.setLevel(getattr(logging, conf["loglevel"]))

        dbus = DBBus(
            self.conf["database"]["host"],
            self.conf["database"]["user"],
            self.conf["database"]["password"],
            self.conf["database"]["db"]
        )
        dbus.connect()

        yaproc = YaProcessor(conf, dbus.get_token(conf["database"]["token_name"],
            conf["database"]["token_table"]))
        yaproc.process()

    except KeyboardInterrupt:
        print('\nThe process was interrupted by the user')
        raise SystemExit

if __name__ == "__main__":
    main()