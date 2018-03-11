import logging
import re
import subprocess
import traceback

import logger
from bus import QueueBus
from push import YaPush


def loop(func):
    def wrapper(self, *args, **kwargs):
        if self.timeout is not None:
            while True:
                func(self, *args, **kwargs)
                time.sleep(self.timeout)
        else:
            func(self, *args, **kwargs)
        return
    return wrapper


class YaProcessor(object):
    def __init__(self, conf, token, loglevel="DEBUG", filelog=None):
        self.conf = conf
        self.token = token
        self.timeout = 5

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

    def process_msg(self, message):
        push = YaPush(message)
        push.parse()
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

    @loop
    def process(self):
        qbus = QueueBus(
            self.conf["mqueue"]["host"],
            self.conf["mqueue"]["user"],
            self.conf["mqueue"]["password"]
        )
        qbus.connect()

        push_str = qbus.get_push(self.conf["mqueue"]["queue"])
        self.process_msg(push_str)
