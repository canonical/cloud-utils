#!/usr/bin/python
# -*- coding: utf-8 -*-

# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

# Simple Utility for reading SQS Message Queue

import argparse
from syncimgs.syncmessage import SQSConn

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_key', action='store', required=True,
                        help='AWS Secret Key')
    parser.add_argument('--access_key', action='store', required=True,
                        help='AWS Access Key/ID')
    parser.add_argument('--queue', action='store', required=True,
                        help='Queue to use for storing message')
    parser.add_argument('--all', action='store_true',
                        help='Read all queue items')
    parser.add_argument('--max', action='store', type=int,
                        help='Max number of elements to read')

    opts = parser.parse_args()
    sqsconn = SQSConn(opts.access_key, opts.secret_key, opts.queue)

    counted = 0
    for item in sqsconn.get():
        print item.unpack(pretty=True)
        print '\n'

        if opts.max and not opts.all:
            if opts.max >= counted:
                break
