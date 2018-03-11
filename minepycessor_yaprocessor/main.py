import argparse
import logging
import yaml

from minepycessor_libs import DBBus, QueueBus
from minepycessor_libs import logger
from minepycessor_yaprocessor import YaProcessor


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
        if args.log is not None:
            conf["logfile"] = args.log
        if conf["logfile"] == "":
            conf["logfile"] = None

        global log

        if args.log is not None:
            log = logging.getLogger(__name__)
            log.addHandler(logger.FileHandler(args.log))
            log.setLevel(getattr(logging, conf["loglevel"]))
        else:
            log = logging.getLogger(__name__)
            log.addHandler(logger.StreamHandler())
            log.setLevel(getattr(logging, conf["loglevel"]))

        qbus = QueueBus(
            conf["mqueue"]["host"],
            conf["mqueue"]["port"],
            conf["mqueue"]["user"],
            conf["mqueue"]["password"],
            loglevel=conf["loglevel"],
            logfile=conf["logfile"]
        )
        qbus.connect()
        dbus = DBBus(
            conf["database"]["host"],
            conf["database"]["port"],
            conf["database"]["user"],
            conf["database"]["password"],
            conf["database"]["db"],
            loglevel=conf["loglevel"],
            logfile=conf["logfile"]
        )
        dbus.connect()

        token = dbus.get_token(
            conf["database"]["token_name"],
            conf["database"]["token_table"]
        )
        log.debug("Got this token from database {}".format(token))
        yaproc = YaProcessor(
            conf,
            token,
            timeout=conf["yaprocessor"]["timeout"],
            loglevel=conf["loglevel"],
            logfile=conf["logfile"]
        )
        yaproc.process(qbus)

    except KeyboardInterrupt:
        print('\nThe process was interrupted by the user')
        raise SystemExit


if __name__ == "__main__":
    main()
