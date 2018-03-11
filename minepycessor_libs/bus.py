import logging
import traceback

import logger

import pika
import MySQLdb


class QueueBus(object):
    def __init__(self, host, user, password,
                 loglevel="INFO", logfile=None):
        self.host = host
        self.user = user
        self.password = password
        self.connection = None

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

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, traceback):
        if self.connection is not None:
            self.disconnect()

    def __del__(self):
        if self.connection is not None:
            self.disconnect()

    def connect(self):
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.host,
                    credentials=pika.PlainCredentials(
                        self.user, self.password),
                    heartbeat=0
                )
            )
            log.debug("{}: Connected to {}".format(
                self.__class__.__name__, self.host))
        except:
            log.error("{}: Can't connect to {}\n{}".format(
                self.__class__.__name__, self.host, traceback.format_exc()))

    def disconnect(self):
        try:
            self.connection.close()
            log.debug("{}: Disconnected from {}".format(
                self.__class__.__name__, self.host))
        except:
            log.error("{}: Can't disconnect from {}\n{}".format(
                self.__class__.__name__, self.host, traceback.format_exc()))

    def put_push(self, push_str, queue):
        if self.connection is not None:
            try:
                channel = self.connection.channel()
                channel.queue_declare(queue=queue, durable=True)
                channel.basic_publish(exchange="",
                                      routing_key=queue,
                                      properties=pika.BasicProperties(
                                        delivery_mode=2,
                                      ),
                                      body=push_str)
                log.debug("{}: Push has been put to queue {}".format(
                    self.__class__.__name__, queue))
            except:
                log.error("{}: Can't put push to queue {}\n{}".format(
                    self.__class__.__name__, queue, traceback.format_exc()))

    def get_push(self, queue):
        if self.connection is not None:
            push_str = None
            try:
                channel = self.connection.channel()
                channel.queue_declare(queue=queue, durable=True)
                method_frame, header_frame, push_str = channel.basic_get(
                    queue=queue)
                if method_frame is None or \
                        method_frame.NAME == "Basic.GetEmpty":
                    log.debug("{}: Queue {} is empty".format(
                        self.__class__.__name__, queue))
                else:
                    channel.basic_ack(delivery_tag=method_frame.delivery_tag)
                    log.debug(
                        "{}: Push has been gotten successfully from queue {}".
                        format(self.__class__.__name__, queue)
                    )
            except:
                log.error("{}: Can't get push from queue {}\n{}".format(
                    self.__class__.__name__, queue, traceback.format_exc()))

            return push_str


class DBBus(object):
    def __init__(self, host, user, password, db,
                 loglevel="INFO", logfile=None):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.connection = None

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

    def __enter__(self):
        self.connect()

    def __exit__(self, type, value, traceback):
        if self.connection is not None:
            self.disconnect()

    def __del__(self):
        if self.connection is not None:
            self.disconnect()

    def connect(self):
        try:
            self.connection = MySQLdb.connect(
                host=self.host,
                user=self.user,
                passwd=self.password,
                db=self.db
            )
            log.debug("{}: Connected to database {} at host {}".format(
                self.__class__.__name__, self.db, self.host))
        except:
            log.error(
                "{}: Can't connect to database {} at host {}\n{}".
                format(
                    self.__class__.__name__,
                    self.db,
                    self.host,
                    traceback.format_exc()
                )
            )

    def disconnect(self):
        try:
            self.connection.close()
            log.debug("{}: Disconnected from {} database {}".format(
                self.__class__.__name__, self.host, self.db))
        except:
            log.error("{}: Can't disconnect from {} database {}\n{}".format(
                self.__class__.__name__, self.host, self.db,
                traceback.format_exc()))

    def create_token_table(self, table):
        if self.connection is not None:
            try:
                try:
                    c = self.connection.cursor()
                    sql = '''CREATE TABLE IF NOT EXISTS {} (
                        TokenName varchar(255) NOT NULL PRIMARY KEY,
                        Token varchar(512)
                    );
                    '''.format(table)
                    c.execute(sql)
                    log.debug(
                        "{}: Table {} has been created successfully".
                        format(self.__class__.__name__, table)
                    )
                finally:
                    c.close()
            except:
                log.error("{}: Can't create table {}\n{}".format(
                    self.__class__.__name__, table, traceback.format_exc()))

    def put_token(self, name, token, table):
        if self.connection is not None:
            try:
                try:
                    c = self.connection.cursor()
                    sql = '''INSERT INTO {table} (TokenName, Token)
                        VALUES ("{name}", "{token}") ON DUPLICATE KEY
                        UPDATE TokenName="{name}", Token="{token}";
                    '''.format(table=table, name=name, token=token)
                    c.execute(sql)
                    log.debug("{}: Token {} added to {}".format(
                        self.__class__.__name__, name, table))
                    self.connection.commit()
                finally:
                    c.close()
            except:
                log.error("{}: Can't add token {} to {}\n{}".format(
                    self.__class__.__name__, name, table,
                    traceback.format_exc()))

    def get_token(self, name, table):
        if self.connection is not None:
            try:
                try:
                    c = self.connection.cursor()
                    sql = '''SELECT Token FROM {}
                        WHERE TokenName="{}";
                    '''.format(table, name)
                    c.execute(sql)
                    res = c.fetchone()
                    log.debug("{}: Got token {} from {}".format(
                        self.__class__.__name__, name, table))
                finally:
                    c.close()
            except:
                log.error("{}: Can't get token {} from {}\n{}".format(
                    self.__class__.__name__, name, table,
                    traceback.format_exc()))

            return res
