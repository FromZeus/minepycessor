from collections import MutableMapping
import logging
import hashlib
import urllib

import logger

import requests


class YaPush(MutableMapping, object):
    def __init__(self, message, loglevel="INFO", logfile=None):
        self.message = message
        self._parsed = {}
        self._verified = None

        global log

        if "log" not in globals():
            if logfile is not None:
                log = logging.getLogger(__name__)
                log.addHandler(logger.FileHandler(logfile))
                log.setLevel(getattr(logging, loglevel))
            else:
                log = logging.getLogger(__name__)
                log.addHandler(logger.StreamHandler())
                log.setLevel(getattr(logging, loglevel))

    # Collection methods start

    def __getitem__(self, key):
        return self._parsed[key]

    def __setitem__(self, key, value):
        self._parsed[key] = value

    def __delitem__(self, key):
        del self._parsed[key]

    def __iter__(self):
        return iter(self._parsed)

    def __len__(self):
        return len(self._parsed)

    # Collection methods end

    def __repr__(self):
        return "Push: {}".format(self._parsed)

    def to_str(self):
        return "{}".format(self._parsed)

    def from_str(self, push_str):
        self._parsed = eval(push_str)
        return self._parsed

    @property
    def verified(self):
        return self._verified

    @verified.setter
    def verified(self, value):
        pass

    @verified.deleter
    def verified(self):
        del self._verified

    @property
    def parsed(self):
        return self._parsed

    @parsed.setter
    def parsed(self, value):
        pass

    @parsed.deleter
    def parsed(self):
        del self._parsed

    def parse(self, message=None):
        if message is not None:
            self.message = message
        smessage = self.message.split("&")
        result = {}

        for el in smessage:
            k, v = el.split("=")
            result[k] = v

        self._parsed = result

        return self._parsed

    def verify(self, push_secret):
        params = [
            self._parsed["notification_type"],
            self._parsed["operation_id"], self._parsed["amount"],
            self._parsed["currency"], self._parsed["datetime"],
            self._parsed["sender"], self._parsed["codepro"],
            push_secret, self._parsed["label"]
        ]

        if "unaccepted" in self._parsed and \
                self._parsed["unaccepted"] == "true":
            log.warning("Payment failed to accept! UNACCEPTED")
        if self._parsed["codepro"] == "true":
            log.warning("Payment failed to accept! CODEPRO")

        check_str = "&".join(params)
        check_str = urllib.unquote_plus(check_str).decode("utf8")
        log.debug("Check string: {}".format(check_str))
        check_sum = hashlib.sha1(check_str).hexdigest()

        if check_sum == self._parsed["sha1_hash"]:
            log.info("Push verified")
            self._verified = True
        else:
            log.warning(
                "Push aren't verified\nChecksum:\t{}\nControlled:\t{}".
                format(check_sum, self._parsed["sha1_hash"])
            )
            self._verified = False

        return self.verified

    def get_details(self, token):
        headers = {"Authorization": "Bearer {}".format(token)}
        data = {"operation_id": self._parsed["operation_id"]}

        r = requests.post(
            "https://money.yandex.ru/api/operation-details",
            headers=headers, data=data
        )

        if r.status_code == 200:
            log.info("Got details successfully")
            self._parsed.update(r.json())
            return r.json()
        else:
            log.error("Can't get details. Reason: %s" % (r.reason))
