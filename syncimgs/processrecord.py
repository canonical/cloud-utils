#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## This provides an interface for recording the builds.

from syncimgs.easyrep import EasyRep
import uuid
import shutil

class ProcessRecord(EasyRep):

    def __init__(
        self,
        distro,
        build_serial,
        release_tag,
        stream='server',
        config=None,
        directory=None,
        ):
        self.distro = distro
        self.build_serial = build_serial
        self.release_tag = release_tag
        self.stream = stream
        self.config = config
        self.directory = directory
        self.arches = {}
        self.uuid = str(uuid.uuid4())

    def __arch__(self, arch):

        if not arch in self.arches:
            self.arches[arch] = {'files': [], 'logs': []}

    def purge(self, confirm=False):
        if not confirm:
            return False

        try:
            shutil.rmtree(self.directory)
        except Exception, e:

            return e

        return self.directory

    def merge(self, old):

        merge_count = 0
        for (arch, arch_content) in old.get_arches():
            if arch not in self.arches:
                self.arches[arch] = old.get_arches()
                merge_count += 1

        return merge_count

    def get_arches(self):

        for arch in self.arches:
            yield (arch, self.arches[arch])

    def set(
        self,
        key,
        value,
        arch=None,
        ):
        self.__arch__(arch)

        if arch:
            self.arches[arch][key] = value
        else:

            setattr(self, key, value)

    def add(self, arch, **kargs):
        self.__arch__(arch)

        for key in kargs:

            if key == 'file':
                self.arches[arch]['files'].append(kargs[key])
            elif key == 'logs':

                self.arches[arch]['logs'].append(kargs[key])
            else:

                self.arches[arch][key] = kargs[key]

    def get_value(self, arch, value):

        try:
            for v in self.arches[arch]:
                if v == value:
                    return value
        except KeyError:

            return None

    def iter(self, arch):
        for v in self.arches[arch]:
            yield (v, self.arches[arch][v])


# vi: ts=4 expandtab
