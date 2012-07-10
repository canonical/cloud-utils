#!/usr/bin/python
# -*- coding: utf-8 -*-

# vi: ts=4 noexpandtab

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## This provides the functionality of downloading, validating and parsing
##      parsing the JSON for a build. Hopefully, this makes things a whole
##      lot easier to use.

import bz2
import gzip
import json
import logging
import os
import re
import tempfile
import sys
import StringIO
import urllib2
import random
from syncimgs.easyrep import EasyRep
from syncimgs.execute import RunCMD

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('_sync-image_')


class Unpacker(EasyRep):

    __name__ = 'Unpacker'

    def __init__(self, **kargs):

        self.builds = []
        for key in kargs:
            if key == 'unpack':
                unpack = kargs[key]

                if type(unpack).__name__ == 'list':
                    self.unpack = unpack
                elif type(unpack).__name__ == 'dict':

                    for key in unpack:
                        setattr(self, key, unpack[key])

                del unpack
            else:

                setattr(self, key, kargs[key])

    def set(self, key, value):
        setattr(self, key, value)


## Classes for pulling out the registration lists

class Registration(Unpacker):

    # Stupid class to make parsing easier

    __name__ = 'Regsitration'

    def dump(self,
             output='%(build_serial)s %(id)s %(arch)s %(instance_type)s %(region)s '
             ):

        supported_keys = [
            'region',
            'id',
            'ramdisk_id',
            'registered_name',
            'build_serial',
            'path',
            'sha1',
            'cloud',
            'instance_type',
            'arch',
            ]

        for key in supported_keys:
            if not hasattr(self, key):
                setattr(self, key, None)

        output = output % {
            'region': self.region_name,
            'id': self.published_id,
            'instance_type': self.instance_type,
            'cloud': self.cloud,
            'ramdisk_id': self.ramdisk_id,
            'registered_name': self.registered_name,
            'build_serial': self.build_serial,
            'path': self.path,
            'sha1': self.sha1,
            'arch': self.arch,
            }

        return output


class InstanceType(Unpacker):

    __name__ = 'InstanceType'

    def iter_registrations(self):
        for item in self.registrations:
            yield Registration(unpack=item,
                               registered_name=self.registered_name)

    def get_region(self, region):
        for reg in self.iter_registrations():
            if reg.region_name == region:
                return reg


class CloudReg(Unpacker):

    __name__ = 'CloudRegistrations'

    def iter_types(self):
        for item in self.instance_types:
            yield InstanceType(unpack=item)

    def iter_type_names(self):
        for item in self.instance_types:
            yield item['name']

    def iter_type_all(self):
        for item in self.instance_types:
            yield (item['name'], InstanceType(unpack=item))


## Classes for pulling out the build info

class ArchBuild(EasyRep):

    """
    Contains a collection of build files
    """

    __name__ = 'ArchBuild'

    def __init__(self, **kargs):
        self.files = []
        self.cloud_registration = {}
        for key in kargs:
            if key == 'unpack':
                unpack = kargs[key]

                if type(unpack).__name__ == 'dict':
                    for item in unpack:
                        if type(unpack[item]).__name__ == 'list':
                            if item == 'file_list':
                                for i in unpack[item]:
                                    self.files.append(i)
                            elif item == 'cloud_registrations':
                                for i in unpack[item]:
                                    self.cloud_registration[i['name']] = i
                        else:
                            setattr(self, item, unpack[item])
            else:

                setattr(self, key, kargs[key])

    def iter_clouds(self, unpacked=True):
        for c in self.cloud_registration:
            yield (c, CloudReg(unpack=self.cloud_registration[c]))

    def iter_files(self, unpacked=True, first_types=None):
        unpacked_re = re.compile('.*/unpacked/.*')

        if first_types:
            for f in self.files:
                b = BuildFiles(build_id=self.get('build_id'), unpack=f)
                if not unpacked and unpacked_re.match(b.path):
                    continue

                if b.file_type not in first_types.split():
                    continue

                yield b

        for f in self.files:
            b = BuildFiles(build_id=self.get('build_id'), unpack=f)
            if not unpacked and unpacked_re.match(b.path):
                continue

            if first_types:
                if b.file_type in first_types.split():
                    continue

            yield b

    def download_info_type(self, file_type='tar.gz', unpacked=False):
        for f in self.iter_files(unpacked=unpacked):
            if f.file_type == file_type:
                return (f.path, f.sha1)


class Builds(Unpacker):

    __name__ = 'Builds'

    def iter_builds(self, sort=True, latest=None):
        b_list = {}
        skip = False

        for build in self.unpack:
            b_list[build['build_serial']] = build

        total_builds = len(b_list)
        start_index = 0

        if type(latest).__name__ == 'bool':
            latest = 'latest'

        if latest:

            keep = latest.lower()
            if keep == 'latest':
                index = sorted(b_list)[-1]
                yield Build(unpack=b_list[index])
                skip = True
            elif not re.search(r'\w-\d', keep):

                start_index = total_builds - int(keep.replace('n-', ''))

                if start_index < 0:
                    start_index = 0

        if not skip:

            for build in self.unpack:
                b_list[build['build_serial']] = build

            curr_index = 0
            for b in sorted(b_list):
                if curr_index >= start_index:
                    yield Build(unpack=b_list[b])

    def count(self):
        return len(self.unpack)


class Build(EasyRep):

    __name__ = 'Build'

    def __init__(self, **kargs):
        self.arch_builds = {}
        for key in kargs:
            if key == 'unpack':
                self.unpack = kargs[key]

                for item in self.unpack:
                    if item == 'arches':
                        for arch in self.unpack[item]:
                            self.arch_builds[arch] = \
                                ArchBuild(arch=arch,
                                    unpack=self.unpack[item][arch])
                    else:
                        setattr(self, item, self.unpack[item])

                del self.unpack
            else:

                setattr(self, key, kargs[key])

    def iter_arches(self):
        for f in self.arch_builds:
            yield (f, self.arch_builds[f])

    def arches(self):
        for arch in self.arch_builds:
            yield arch

    def count(self):
        return len(self.arch_builds)


class BuildFiles(EasyRep):

    """
    Files pertaining to a single build; Unpacks the contents of a
    dictionary to a class file
    """

    def __init__(self, **kargs):

        for key in kargs:
            if key == 'unpack' and type(kargs[key]).__name__ == 'dict':
                for k in kargs[key]:
                    setattr(self, k, kargs[key][k])
            else:

                setattr(self, key, kargs[key])

    def dump(self,
            unpacked=False,
            url_base=None,
            output="%(file_type)s %(path)s %(sha1)s",
            ):

        if 'unpacked' in self.path and not unpacked:
            return

        url = self.path
        if url_base:
            url = "%s/%s" % (url_base, self.path)

        output = output % {
            'description': self.description,
            'sha1': self.sha1,
            'sha512': self.sha512,
            'buildid': self.buildid,
            'path': url,
            'file_type': self.file_type,
            }

        return output



class BuildCatalog(EasyRep):

    def __init__(self, **kargs):
        self.distros = {}
        for key in kargs:
            if key == 'json':
                self.process_json(kargs[key])
            else:
                setattr(self, key, kargs[key])

    def process_json(self, json):
        '''Break apart the JSON into manageable chunks'''

        for tlc in json:
            if type(json[tlc]).__name__ == 'dict':
                for key in json[tlc]:
                    setattr(self, '%s_%s' % (tlc, key), json[tlc][key])
                    logger.debug('Registered variable %s_%s' % (tlc,
                                 key))

            if type(json[tlc]).__name__ == 'list' and tlc == 'catalog':
                logger.debug('Registering build image variables')
                for i in json[tlc]:
                    if type(i).__name__ == 'dict':
                        for bt in i['build_types']:
                            logger.debug('Creating build iterator for %s %s'
                                     % (i['distro_code_name'], bt))
                            setattr(self, '%s_%s'
                                    % (i['distro_code_name'], bt),
                                    i['build_types'][bt])

                            if i['distro_code_name'] \
                                not in self.distros:
                                logger.debug('    Adding distro %s to distro list'
                                         % i['distro_code_name'])
                                self.distros[i['distro_code_name']] = []

                            if bt \
                                not in self.distros[i['distro_code_name'
                                    ]]:
                                self.distros[i['distro_code_name'
                                        ]].append(bt)

    def __splitter__(self, item):

        if type(item).__name__ == 'list':
            return item
        elif type(item).__name__ == 'str':

            return item.split()

    def iter_release_tag_counts(self, distro, stream='server'):
        for sub_stream in self.__splitter__(stream):
            bl = {}

            base = self.__get__('%s_%s' % (distro, sub_stream))
            for sub in base:
                if sub['release_tag'] not in bl:
                    bl[sub['release_tag']] = 1
                else:

                    bl[sub['release_tag']] += 1

            for tag in bl:
                yield (tag, bl[tag])

    def get_tag_count(
        self,
        distro,
        tag,
        stream,
        ):
        for (release_tag, count) in \
            self.iter_release_tag_counts(distro, stream=stream):
            if tag == release_tag:
                return count

    def iter_distro_release_types(self, distro, stream='server'):
        """Iterates over the release types in the JSON"""

        for sub_stream in self.__splitter__(stream):
            types = {}
            base = self.__get__('%s_%s' % (distro, sub_stream))
            for sub in base:
                types[sub['release_tag']] = None

            for t in types:
                yield t

    def distro_builds(
        self,
        distro,
        stream='server',
        release_tag='all',
        ):
        """Iterates over the distro builds"""

        for sub_stream in self.__splitter__(stream):
            base = self.__get__('%s_%s' % (distro, sub_stream))
            builds = []

            for sub in base:
                if sub['release_tag'] == release_tag or release_tag \
                    == 'all':
                    builds.append(sub)

            return Builds(distro=distro, stream=sub_stream,
                          release_tag=release_tag, unpack=builds)

    def iter_build_object(
        self,
        distro,
        stream='server',
        release_tag='all',
        ):

        # "all" means any matching

        base = self.__get__('%s_%s' % (distro, stream))

        for sub in base:
            if sub['release_tag'] == release_tag or release_tag \
                == 'all':
                yield Build(distro=distro, stream=stream,
                            release_tags=release_tag, unpack=sub)

    def get_builds(
        self,
        distro,
        stream='server',
        release_tag='all',
        ):

        # "all" means any matching

        ret = {}
        base = self.__get__('%s_%s' % (distro, stream))

        for sub in base:
            if sub['release_tag'] == release_tag or release_tag \
                == 'all':
                build_serial = sub['build_serial']
                ret[build_serial] = {}

                for arch in sub['arches']:

                    ret[build_serial][arch] = []

                    for fl in sub['arches'][arch]['file_list']:
                        anon = BuildFiles(build_id=sub['arches'
                                ][arch]['build_id'], unpack=fl)
                        ret[build_serial][arch].append(anon)

        return ret

    def get_files(
        self,
        distro,
        stream='server',
        release_tag='all',
        ):

        # "all" means any matching

        ret = {}
        base = self.__get__('%s_%s' % (distro, stream))

        for sub in base:
            if sub['release_tag'] == release_tag or release_tag \
                == 'all':
                build_serial = sub['build_serial']
                ret[build_serial] = {}

                for arch in sub['arches']:

                    ret[build_serial][arch] = []

                    for fl in sub['arches'][arch]['file_list']:
                        anon = BuildFiles(buildid=sub['arches'
                                ][arch]['build_id'], unpack=fl)
                        ret[build_serial][arch].append(anon)

        return ret

    def get_distro_stream(self, distro):
        return self.__get__('distros')[distro]

    def iter_distro_stream(self, distro):
        for s in self.__get__('distros')[distro]:
            yield s

    def get_distros(self):
        return self.__get__('distros')

    def iter_distros(self):
        for d in self.__get__('distros'):
            yield d

    def fetch_files(
        self,
        distro,
        stream='server',
        release_tag='all',
        arch='all',
        included=[],
        excluded=[],
        output="%(path)s",
        url_base=None,
        serial=None,
        all=False,
        ):

        """
            Return formated text
        """

        data = ""
        for f in self.iter_files(
                    distro,
                    stream,
                    release_tag,
                    arch,
                    included,
                    excluded,
                    serial,
                    all,
                    ):

            url_base = random.choice(self.mirrors_transfer)

            d = f.dump(output=output, url_base=url_base)
            if d:
               data += "%s\n" % d

        return data

    def iter_files(
        self,
        distro,
        stream='server',
        release_tag='all',
        arch='all',
        included=[],
        excluded=[],
        serial=None,
        all=False,
        ):

        '''
        Iterates over the files for a distro and returns them
        - stream: the stream type
        - release_tag: the release tag of the build, ie. daily or release
        - arches:
        '''

        if type(arch).__name__ != 'list':
            arch = arch.split()

        if type(included).__name__ != 'list':
            included = included.split()

        if type(excluded).__name__ != 'list':
            excluded = excluded.split()

        results = self.get_files(distro, stream, release_tag)
        serials = sorted(results.keys())

        for bf in results:

            if serial and bf != serial:
                continue

            elif not all and bf != serials[-1]:
                next

            for b_arch in results[bf]:

                if b_arch in arch or 'all' in arch:
                    for b_file in results[bf][b_arch]:

                        if b_file.file_type in included:
                            yield b_file
                            continue

                        elif len(included) > 0:
                            continue

                        if b_file.file_type not in excluded:
                            yield b_file

                        elif len(excluded) == 0:
                            yield b_file


    def get(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            return None

    def iter_regs(
        self,
        distro,
        stream,
        release_tag='all',
        instance_type=None,
        arch=None,
        cloud=None,
        latest=True,
        ):

        # Convience function for getting registrations

        builds = self.distro_builds(distro, stream,
                                    release_tag=release_tag)
        for bf in builds.iter_builds(latest=latest):
            if release_tag != 'all' and release_tag != bf.release_tag:
                print '%s %s' % (release_tag, bf.release_tag)
                continue

            for (_arch, arch_build) in bf.iter_arches():
                (path, sha1) = arch_build.download_info_type()
                if arch and _arch != arch:
                    continue

                for (_cloud, cloud_build) in arch_build.iter_clouds():
                    if cloud and _cloud.lower() != cloud.lower():
                        continue

                    for itypes in cloud_build.iter_types():
                        if instance_type and itypes.name.lower() \
                            != instance_type.lower():
                            continue

                        for reg in itypes.iter_registrations():
                            reg.set('path', '%s/%s'
                                    % (str(self.mirrors_transfer[0]),
                                    path))
                            reg.set('sha1', sha1)
                            reg.set('build_serial', bf.build_serial)
                            reg.set('instance_type', itypes.name)
                            reg.set('cloud', _cloud)
                            reg.set('arch', _arch)
                            reg.set('release_tag', bf.release_tag)
                            yield reg

    def get_reg(
        self,
        distro,
        stream,
        release_tag,
        instance_type,
        arch,
        cloud,
        region,
        output='%(build_serial)s %(id)s %(arch)s %(instance_type)s %(region)s',
        all_regions=False
        ):

        reg_data=""

        for r in self.iter_regs(
            distro,
            stream,
            release_tag=release_tag,
            instance_type=instance_type,
            arch=arch,
            cloud=cloud,
            ):

            if all_regions:
                reg_data += "%s\n" % r.dump(output=output)

            elif r.region_name == region:
                return r.dump(output=output)

        return reg_data

class CloudJSON(EasyRep):

    def __init__(self, **kargs):
        for key in kargs:
            setattr(self, key, kargs[key])
            logger.info("Setting CloudJSON variable %s to %s"
                    % (key, kargs[key]))

        if hasattr(self,'url'):
            url = getattr(self, 'url')
            self.get_from_url(url)
            self.json_location = url
            logger.info("JSON will be fetched over http(s)")

        elif hasattr(self,'file'):
            fname = getattr(self, 'file')
            self.get_from_file(fname)
            self.json_location = fname
            logger.info("JSON will be fetched from a local file")

        if not hasattr(self,'gpg_keyring'):
            # Set the default gpg keyring
            self.gpg_keyring="/usr/share/keyrings/ubuntu-cloudimg-keyring.gpg"


    def __checkattr__(self, attr):
        if hasattr(self, attr):
            return getattr(self, attr)
        return False

    def get(self, name):
        try:
            return getattr(self, name)
        except:

            return None

    def get_dump(self):
        return self.__get__('json')

    def write_epoch(self, history):

        bc = getattr(self, 'build_catalog')

        if not self.is_new(history):
            return

        if not bc:
            raise Exception('Unable to write epoch to history file; catalog has not been parsed'
                            )

        with open(history, 'a') as w:
            w.write('%s %s\n' % (self.json_location,
                    bc.manifest_serial))
        w.close()

    def is_new(self, history):

        if not os.path.isfile(history):
            return True

        last_epoch = None
        with open(history, 'r') as f:
            for line in f.readlines():
                line = line.split()
                if line[0] == self.json_location:
                    last_epoch = line[1]
        f.close()

        bc = getattr(self, 'build_catalog')

        if not last_epoch:
            return True

        if int(bc.manifest_serial) > int(last_epoch):
            return True

        return None

    def check_gpg_sig(self, gpg_file, gpg_sig):

        if not hasattr(self,'gpg_verify'):
            logger.info('GPG Verification is not configured')
            return True

        gpg_good = False
        logger.info('Checking GPG signature')

        gpg_cmd = 'gpg --no-default-keyring --keyring %s --verify %s %s' % \
                (self.gpg_keyring, gpg_sig, gpg_file)

        run_cmd = RunCMD(gpg_cmd, output='log', info=True)

        if not run_cmd.was_successful():
            for line in run_cmd.iter_output():
                logger.critical(line)
            os.unlink(gpg_file)
            os.unlink(gpg_sig)

        else:
            gpg_good = True

        if not gpg_good:
            raise Exception("GPG Validation error!","Unable to validate GPG signature")

        return True

    def get_from_file(self, file_name, gpg_sig=None):
        try:

            if self.__checkattr__('gpg_verify'):
                self.check_gpg_sig(file_name, gpg_sig)

            fh = open(file_name, 'rb')
            setattr(self, 'build_catalog',
                    BuildCatalog(json=json.loads(fh.read())))
            fh.close()
        except IOError as e:
            logger.critical("Unable to read from file!\n%s" % e)
            sys.exit(1)

    def get_from_url(self, url):

        http_re = re.compile('^http.*')
        https_re = re.compile('^https.*')

        try:

            if not https_re.match(url) and not http_re.match(url):
                logger.critical("Unsupported URL Schema; please use HTTP(S)")
                sys.exit(1)

            logger.info('Fetching %s' % url)

            request = urllib2.Request(url)
            request.add_header('Accept-encoding', 'gzip')
            response = urllib2.urlopen(request)
            fetched_json = None

            if response.info().get('Content-Encoding') == 'gzip':
                buf = StringIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
                fetched_json = f.read()
            elif response.info().get('Content-Type') \
                == 'application/x-bzip2':
                fetched_json = bz2.decompress(response.read())
            else:
                fetched_json = response.read()

            (json_fh, json_n) = tempfile.mkstemp()
            json_data = open(json_n, 'wb')
            json_data.write(fetched_json)
            json_data.close()

            if getattr(self, 'gpg_verify'):
                gpg_url = "%s.gpg" % url
                if "bz2" in url:
                    gpg_url = url.replace('bz2', 'gpg')

                logger.info('Fetching GPG signature of %s' % gpg_url)
                remote_gpg = urllib2.urlopen(gpg_url)
                fetched_gpg = remote_gpg.read()
                (gpg_fh, gpg_n) = tempfile.mkstemp()
                gpg_data = open(gpg_n, 'wb')
                gpg_data.write(fetched_gpg)
                gpg_data.close()

                self.check_gpg_sig(json_n, gpg_n)
                os.unlink(gpg_n)

            os.unlink(json_n)

            setattr(self, 'build_catalog',
                    BuildCatalog(json=json.loads(fetched_json)))
        except IOError as e:

            raise Exception('Unable to fetch JSON from remote source.\n%s'
                             % e)

        return False


# Example of how to parse out some details...still needs work
#logger.setLevel(logging.DEBUG)
#cloudjson = CloudJSON(gpg_verify=True, url="http://cloud-images.ubuntu.com/query2/server/quantal/ec2.json.bz2")
#catalog = cloudjson.build_catalog
#for r in catalog.iter_regs("quantal", "server"): #, instance_type="ebs"):
#    print r.dump()
# iterator example
#   for bf in bc.iter_files("oneiric", stream="server", release_tag="daily", arch="i386 amd64", included="qcow2 tar.gz"):
#       print bf
