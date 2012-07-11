#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

# Simple messaging class

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
import argparse
import json
import re
import uuid
import pickle
import platform
import sys


class SyncMessage:

    __name__ = 'SyncMessage'

    def __init__(self, **kargs):
        self.uuid = str(uuid.uuid1())
        self.__python_version = platform.python_version()
        (
            self.__system,
            self.__node,
            self.__release,
            self.__version,
            self.__machine,
            self.__process,
            ) = platform.uname()
        self.json_key = '%s_' % self.uuid

        for key in kargs:
            if key == 'json':
                try:
                    j = None
                    with open(kargs[key], 'rb') as f:
                        j = json.load(f)
                    f.close()

                    if j:
                        for item in j:
                            setattr(self, '%s_%s' % (self.json_key,
                                    item), j[item])
                except IOError, e:

                    print e
                    sys.exit(1)
            else:
                setattr(self, key, kargs[key])

    def iter_unpack(self, key=None):
        if not key:
            key = self.json_key

        key_re = re.compile('%s.*' % key)
        for val in self.__dict__.keys():
            if key_re.match(val):
                nkey = val.replace('%s_' % key, '')
                yield (nkey, getattr(self, val))

    def unpack(self, key=None, pretty=False):
        pack = {}
        for (key, val) in self.iter_unpack(key=key):
            pack[key] = val

        if pretty:
            return json.dumps(pack)

        return pack

    def passed(self):
        try:
            return self.success
        except KeyError:
            return None

    def failed(self):
        try:
            return self.success
        except KeyError:
            return None

    def get(self, key):
        try:
            return getattr(self, key)
        except KeyError:
            return None

    def set(self, key, value):
        setattr(self, key, value)

    def ack(self, uuid):
        try:
            if getattr(self, 'ack') == uuid:
                return True
            else:
                return False
        except KeyError:
            return None

    def __repr__(self):
        ret_string = ''
        for i in self.__dict__.keys():
            if not ret_string:
                ret_string = '%s=%s' % (i, getattr(self, i))
            else:

                ret_string = ret_string + ', %s=%s' % (i, getattr(self,
                        i))

        ret_string = str('%s(%s)' % (self.__class__.__name__,
                         ret_string))
        return ret_string


class SQSConn:

    def __init__(
        self,
        access_key,
        secret_key,
        queue_name,
        ):
        self.conn = SQSConnection(access_key, secret_key)
        self.queue = self.conn.create_queue(queue_name)

    def connected(self):
        c = False
        q = False
        if self.conn:
            c = True
        if self.queue:
            q = True

        return (c, q)

    def send(self, syncmessage):

        if not isinstance(syncmessage, SyncMessage):
            raise Exception('Invalid message type')

        if self.queue:
            m = Message()
            m.set_body(pickle.dumps(syncmessage))
            self.queue.write(m)
        else:
            raise Exception('No Queue Connection!')

    def get(self, delete=False):
        if self.queue:
            while self.queue.count > 0:
                try:
                    m = self.queue.read()
                    if m:
                        message = pickle.loads(m.get_body())
                        yield message

                        if delete:
                            m.delete()
                    else:
                        break
                except pickle.UnpicklingError as e:
                    raise 'Error de-pickling!\n%s' % e


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_key', action='store', required=True,
                        help='AWS Secret Key')
    parser.add_argument('--access_key', action='store', required=True,
                        help='AWS Access Key/ID')
    parser.add_argument('--queue', action='store', required=True,
                        help='Queue to use for storing message')
    parser.add_argument('--success', dest='success', action='store_true'
                        , help='Record command as successfull')
    parser.add_argument('--fail', dest='success', action='store_false',
                        help='Record command as a failure')
    parser.add_argument('--msg', action='store', required=True,
                        help='Message component')
    parser.add_argument('--json', action='store',
                        help='Name of JSON file containing futher key/value pairs'
                        )

    opts = parser.parse_args()

    # Prepare the message

    message = SyncMessage(msg=opts.msg, json=opts.json,
                          success=opts.success)

    # Write the message

    sqsconn = SQSConn(opts.access_key, opts.secret_key, opts.queue)
    sqsconn.send(message)

# vi: ts=4 expandtab
