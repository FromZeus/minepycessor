import logging
import traceback
import urllib

import logger

import requests


class YaAuth(object):
    def __init__(self, redirect_url, client_id, client_secret,
                 push_secret, scope=["operation-details"],
                 base_url="https://sp-money.yandex.ru/oauth",
                 loglevel="INFO", logfile=None):
        self.base_url = base_url
        self.redirect_url = redirect_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.push_secret = push_secret
        self.scope = scope
        self.loglevel = loglevel
        self.logfile = logfile

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

    def get_auth_url(self):
        try:
            auth_url = \
                self.base_url + "/authorize?" + "&".join(
                    [
                        "redirect_uri={}".format(
                            urllib.quote_plus(self.redirect_url)),
                        "scope={}".format("+".join(self.scope)),
                        "client_id={}".format(self.client_id),
                        "response_type=code"
                    ]
                )
            log.info(
                "\n### Auth URL ###\n\n{}\n\n###############".
                format(auth_url)
            )
            return auth_url
        except:
            log.error("Can't get auth URL for{}\n{}".format(
                self.client_id, traceback.format_exc()))

    def get_token(self, code):
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_url
        }

        r = requests.post(self.base_url + "/token", data=data)

        if r.status_code == 200:
            log.info("Access token has been gotten successfully")
            log.debug(r.json()["access_token"])
            return r.json()["access_token"]
        else:
            log.error("Can't get access token for %s\nReason: %s" % (
                self.client_id, r.reason))
