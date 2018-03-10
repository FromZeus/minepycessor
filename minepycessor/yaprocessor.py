from collections import MutableMapping
import logging
import re
import subprocess
import traceback
import hashlib
import urllib

import logger

import requests


class Push(MutableMapping, object):
    def __init__(self, message, loglevel="DEBUG", filelog=None):
        self.message = message
        self.parsed = self.parse(self.message)
        self._verified = None

        global log

        if "log" not in globals():
            if filelog is not None:
                log = logging.getLogger(__name__)
                log.addHandler(logger.FileHandler(filelog))
                log.setLevel(getattr(logging, loglevel))
            else:
                log = logging.getLogger(__name__)
                log.addHandler(logger.StreamHandler())
                log.setLevel(getattr(logging, loglevel))

    # Collection methods start

    def __getitem__(self, key):
        return self.parsed[key]

    def __setitem__(self, key, value):
        self.parsed[key] = value

    def __delitem__(self, key):
        del self.parsed[key]

    def __iter__(self):
        return iter(self.parsed)

    def __len__(self):
        return len(self.parsed)

    # Collection methods end

    def __repr__(self):
        return "Push: {}".format(self.parsed)

    @property
    def verified(self):
        return self._verified

    @verified.setter
    def verified(self, value):
        pass

    @verified.deleter
    def verified(self):
        del self._verified

    @classmethod
    def parse(cls, message):
        smessage = message.split("&")
        result = {}
        for el in smessage:
            k, v = el.split("=")
            result[k] = v

        return result

    def verify(self, push_secret):
        params = [self.parsed["notification_type"],
            self.parsed["operation_id"], self.parsed["amount"],
            self.parsed["currency"], self.parsed["datetime"],
            self.parsed["sender"], self.parsed["codepro"],
            push_secret, self.parsed["label"]]

        if "unaccepted" in self.parsed and self.parsed["unaccepted"] == "true":
            log.warning("Payment failed to accept! UNACCEPTED")
        if self.parsed["codepro"] == "true":
            log.warning("Payment failed to accept! CODEPRO")

        check_str = "&".join(params)
        check_str = urllib.unquote_plus(check_str).decode("utf8")
        log.info("Check string: {}".format(check_str))
        check_sum = hashlib.sha1(check_str).hexdigest()

        if check_sum == self.parsed["sha1_hash"]:
            log.info("Push verified")
            self._verified = True
        else:
            log.warning("Push aren't verified\nChecksum:\t{}\nControlled:\t{}".
                format(check_sum, self.parsed["sha1_hash"]))
            self._verified = False

        return self.verified

    def get_details(self, token):
        headers = {"Authorization": "Bearer {}".format(token)}
        data = {"operation_id": self.parsed["operation_id"]}

        r = requests.post(
            "https://money.yandex.ru/api/operation-details",
            headers=headers, data=data
        )

        if r.status_code == 200:
            log.info("Got details successfully")
            self.parsed.update(r.json())
            return r.json()
        else:
            log.error("Can't get details. Reason: %s" % (r.reason))


class YaProcessor(object):
    def __init__(self, conf, token, loglevel="DEBUG", filelog=None):
        self.conf = conf
        self.token = token

        global log

        if "log" not in globals():
            if filelog is not None:
                log = logging.getLogger(__name__)
                log.addHandler(logger.FileHandler(filelog))
                log.setLevel(getattr(logging, loglevel))
            else:
                log = logging.getLogger(__name__)
                log.addHandler(logger.StreamHandler())
                log.setLevel(getattr(logging, loglevel))

    @classmethod
    def parse_message(cls, message):
        sp = message.split(";")
        res = [sp[0].strip()]
        if len(sp) > 1:
            res.append(sp[1].strip())
        else:
            res.append(None)

        return res

    def perform_command(self, nickname, target):
        try:
            subprocess.call(
                [
                    "screen", "-x", self.conf["screen_name"],
                    "-p", "0", "-X", "stuff",
                    "{}^M".format(
                        re.sub(
                            "{{ name }}", nickname,
                            self.conf["menue"][target]["command"]
                        )
                    )
                ]
            )
        except:
            log.error("Can't perform command {}\n{}".format(
                target, traceback.format_exc()))

    def process(self, message):
        push = Push(message)
        push.verify(self.conf["push_secret"])
        push.get_details(self.token)
        log.info(push)

        if "message" in push:
            target, nickname = self.parse_message(push["message"])
            if nickname is None:
                log.warning(
                    "Command hasn't been performed."
                    "Comment is empty for {}".format(push["sender"])
                )
            else:
                if target in self.conf["menue"]:
                    if float(push["withdraw_amount"]) == \
                            float(self.conf["menue"][target]["price"]):
                        self.perform_command(nickname, target)
                    else:
                        log.warning(
                            "Command hasn't been performed."
                            "Payment desn't correspond to price. {}".format(
                                push["withdraw_amount"])
                        )
                else:
                    log.warning(
                        "Command hasn't been performed."
                        "No such target in menue. {}".format(target)
                    )
        else:
            log.warning(
                "Something wrong with push, there is no 'message' field"
            )
