#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 03 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import logging


class SQSHandler(logging.Handler):  # Inherit from logging.Handler

    def __init__(
        self,
        access_key,
        secret_key,
        queue,
        ):
        logging.Handler.__init__(self)
        self.connection = SQSConnection(access_key, secret_key)
        self.queue = self.connection.get_queue(queue)

    def emit(self, record):

        if self.queue:
            m = Message()
            m.set_body(self.format(record))
            self.queue.write(m)

# vi: ts=4 expandtab
