#!/usr/bin/python
# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 October 2011
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## Mirrors apt repositories to Amazon S3
import sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.s3.bucket import Bucket
import argparse
import sys

def upload(bucket, logs, remote_name, access_key, secret_key, valid_time):

	try:
		conn = S3Connection( access_key, secret_key )
		bucket = conn.get_bucket( bucket )
		key = Key( bucket )
		key.name = remote_name
		key.set_contents_from_filename( logs )

		key1 = Key( bucket )
		key1.name = remote_name
		print key1.generate_url( valid_time )

	except Exception, e:
		print "ERROR GENERATING KEY\n%s" % e


if __name__=="__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('--secret_key', action="store",
		help="AWS Secret Key")
	parser.add_argument('--access_key', action="store",
		help="AWS Access Key/ID")
	parser.add_argument('--logs', action="store",
		help="Local log file location")
	parser.add_argument('--key_name', action="store", default=None,
		help="Name of the remote log")
	parser.add_argument('--bucket', action="store",
		help="Name of the bucket to save logs in")
	parser.add_argument('--time', action="store", type=int, default=86400,
		help="Time in seconds for URL to be valid")

	opts = parser.parse_args()

	if not opts.secret_key or not opts.access_key or not opts.logs or not opts.bucket:
		print "--secret_key, --access_key, --logs and --bucket are required"
		sys.exit(1)

	if not opts.key_name:
		opts.key_name = opts.logs

	upload( opts.bucket, opts.logs, opts.key_name, opts.access_key, opts.secret_key, opts.time )
