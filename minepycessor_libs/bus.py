import logging
import traceback

import logger

import pika
import MySQLdb


class QueueBus(object):
    def __init__(self, host, user, password):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None

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

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def connect(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=host,
                credentials=pika.PlainCredentials(user, password)
            )
        )

    def disconnect(self):
        self.connection.close()

    def put_push(self, push, queue):
        if self.connection is not None:
            channel = self.connection.channel()
            channel.queue_declare(queue=queue)
            channel.basic_publish(exchange="",
                                  routing_key=queue,
                                  body=push.to_str())

    def get_push(self, queue):
        if self.connection is not None:
            channel = self.connection.channel()
            channel.queue_declare(queue=queue)
            push_str = channel.basic_get(queue=queue, no_ack=True)

            return push_str


class DBBus(object):
    def __init__(self, host, user, password, db,
                 loglevel="DEBUG", filelog=None):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.connection = None

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

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, traceback):
        self.disconnect()

    def __del__(self):
        self.disconnect()

    def connect(self):
        self.connection = MySQLdb.connect(host=self.host, user=self.user,
            passwd=self.password, db=self.db)

    def disconnect(self):
        self.connection.close()

    def create_token_table(self, table):
        if self.connection is not None:
            c = self.connection.cursor()
            sql = '''CREATE TABLE IF NOT EXISTS {} (
                Id int,
                TokenName varchar(255),
                Token varchar(255),
                PRIMARY KEY (Id)
            );
            '''.format(table)
            c.execute(sql)
            c.close()

    def put_token(self, name, token, table):
        if self.connection is not None:
            c = self.connection.cursor()
            sql = '''INSERT INTO {} (TokenName, Token)
                VALUES ({}, {}) ON DUPLICATE KEY UPDATE;
            '''.format(table, name, token)
            c.execute(sql)
            c.close()

    def get_token(self, name, table):
        if self.connection is not None:
            c = self.connection.cursor()
            sql = '''SELECT Token FROM {}
                WHERE TokenName == {};
            '''.format(table, name)
            c.execute(sql)
            res = c.fetchone()
            c.close()

            return res