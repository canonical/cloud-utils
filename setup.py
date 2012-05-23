#!/usr/bin/python
# vi: ts=4 expandtab
#
#    Distutils magic for ec2-init
#    Copyright (C) 2009 Canonical Ltd.
#
#    Author: Ben Howard <ben.howard@ubuntu.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License version 3, as
#    published by the Free Software Foundation.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
from distutils.core import setup
from glob import glob
import os.path
import subprocess

def is_f(p):
    return(os.path.isfile(p))

setup(name='sync-images',
      version='0.1',
      description='Tools utilizing query2 for local and cloud synchronization',
      author='Ben Howard',
      author_email='ben.howard@ubuntu.com',
      url='http://launchpad.net/cloud-utils/',
      packages=['syncimgs' ],
      )
