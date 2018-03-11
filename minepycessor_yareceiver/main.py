#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
import logging
import ssl
import traceback
import urlparse
import yaml

from io import BytesIO

from minepycessor_libs import YaAuth
from minepycessor_libs import QueueBus, DBBus
from minepycessor_libs import logger


class AuthServer:
    def __init__(self, auth, qbus, dbus, conf):
        def handler(*args, **kwargs):
            SimpleHTTPRequestHandler(auth, qbus, dbus, conf, *args, **kwargs)
        httpd = HTTPServer((conf["address"], conf["port"]), handler)
        httpd.socket = ssl.wrap_socket(
            httpd.socket,
            keyfile=conf["keyfile"],
            certfile=conf["certfile"],
            server_side=True
        )
        httpd.serve_forever()


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):
    def __init__(self, auth, qbus, dbus, conf, *args, **kwargs):
        self.auth = auth
        self.qbus = qbus
        self.dbus = dbus
        self.conf = conf
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"Thank you for choosing Giga-Ruby!")
        if "?" in self.path:
            path, tmp = self.path.split("?", 1)
            qs = urlparse.parse_qs(tmp)
            try:
                token = self.auth.get_token(qs["code"])
                self.dbus.put_token(
                    self.conf["database"]["token_name"],
                    token,
                    self.conf["database"]["token_table"]
                )
            except:
                log.error("Can't get access token/n{}".format(
                    traceback.format_exc()))

    def do_POST(self):
        content_length = int(self.headers["Content-Length"])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.end_headers()
        response = BytesIO()
        response.write(b"This is POST request. ")
        response.write(b"Received: ")
        response.write(body)
        log.debug("Body:\n{}".format(body))
        self.wfile.write(response.getvalue())
        try:
            self.qbus.put_push(body, self.conf["mqueue"]["queue"])
        except:
            log.error("Can't parse push \n{}".format(
                traceback.format_exc()))


def main():
    try:
        parser = argparse.ArgumentParser()

        parser.add_argument(
            "conf",
            help="Path to .yml file with client id and scopes"
        )
        parser.add_argument(
            "-a", "--address", dest="address",
            help="Name of the server"
        )
        parser.add_argument(
            "-p", "--port", dest="port",
            help="Port of http info server"
        )
        parser.add_argument(
            "-r", "--redirect", dest="redirect_url",
            help="Redirect URL for getting token"
        )
        parser.add_argument(
            "-k", "--keyfile", dest="keyfile",
            help="Path to keyfile"
        )
        parser.add_argument(
            "-c", "--certfile", dest="certfile",
            help="Path to certfile"
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

        if args.address is not None:
            conf["address"] = args.address
        if args.port is not None:
            conf["port"] = args.port
        if args.redirect_url is not None:
            conf["redirect_url"] = args.redirect_url
        if args.keyfile is not None:
            conf["keyfile"] = args.keyfile
        if args.certfile is not None:
            conf["certfile"] = args.certfile
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

        auth = YaAuth(
            conf["redirect_url"],
            conf["client_id"],
            conf["client_secret"],
            conf["push_secret"],
            scope=conf["scope"],
            loglevel=conf["loglevel"],
            logfile=conf["logfile"]
        )
        auth.get_auth_url()
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
        dbus.create_token_table(conf["database"]["token_table"])

        httpd = AuthServer(auth, qbus, dbus, conf)

    except KeyboardInterrupt:
        print("\nThe process was interrupted by the user")
        raise SystemExit


if __name__ == "__main__":
    main()
