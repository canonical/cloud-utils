#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## This provides the functionality of downloading, validating and parsing
##      parsing the JSON for a build. Hopefully, this makes things a whole
##      lot easier to use.

from syncimgs.build_json import CloudJSON
from syncimgs.execute import RunCMD
from syncimgs.easyrep import EasyRep
from syncimgs.processrecord import ProcessRecord
from syncimgs.download_file import URLFetcher
import syncimgs.yaml_config as yaml_config

from datetime import date
import copy
import logging
import pickle
import os
import random
import re

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('_sync-image_')

suite_config_keys = [
    'publish',
    'mirror',
    'name_convention',
    'max_dailies',
    'max_milestones',
    'keep_pre_release',
    'arches',
    'mirror_arches',
    'custom_cmd',
    'list_cmd',
    'check_cmd',
    'publish_cmd',
    'unpublish_cmd',
    'sync_dir',
    'pristine',
    'host_url',
    'steam',
    'unpacked',
    'check_types',
    'customize_type',
    'publish',
    'reg_log',
    ]


class LocalConfiguration(EasyRep):

    """Local distro configuration for handling per-distro overrides"""

    def __init__(
        self,
        catalog,
        conf,
        distro,
        ):

        self.suite = distro

        tl = len(catalog.mirrors_transfer)

        if tl == 1:
            self.transfer = catalog.mirrors_transfer[0]
        else:
            self.transfer = catalog.mirrors_transfer[random.randint(0,
                    len(catalog.mirrors_transfer))]

        override = None

        if conf.overrides is not None and type(conf.overrides).__name__ \
            == 'list':
            for o in conf.overrides:
                if distro in o:
                    override = o[distro]
                    logger.info('Override configuration found for suite %s'
                                 % distro)

        for default in conf.get_attrs():
            setattr(self, default[0], default[1])

        if override and conf.enable_overrides:
            for opt in override:
                setattr(self, opt, override[opt])
        else:

            logger.info('%s will use the default configuration'
                        % distro)


class CloudImgWorker(EasyRep):

    """The rule-processing engine for process handling"""

    def __init__(
        self,
        configuration,
        catalog,
        no_cache=False,
        run=False,
        dry_run=False,
        ):
        self.configuration = configuration
        self.catalog = catalog
        self.no_cache = no_cache
        self.exit_code = 0
        self.error_logs = []
        self.dry_run = dry_run

        if run:
            logger.info('Worker initialized for processing cloud images'
                        )
            self.runner()

    def __replacements__(
        self,
        fd,
        build,
        arch,
        distro,
        local,
        pristine,
        ):

        return {
            'arch': arch,
            'distro': distro,
            'suite': distro,
            'build_serial_id': fd.build_id,
            'build_id': build.build_serial_id,
            'build_serial': build.build_serial,
            'release_tag': build.release_tag,
            'tag': build.release_tag,
            'file_description': fd.description,
            'file_type': fd.file_type,
            'sha1': fd.sha1,
            'sha512': fd.sha512,
            'file_path': fd.path,
            'local': local,
            'pristine_file': pristine,
            'date': date.today(),
            }

    def code_name_to_version(self, code_name):
        """Returns the Ubuntu version string from the code name"""

        year = 7
        rep = '%s' % year

        for c in range(104, 122):

            if c % 2 == 0:
                year += 1

            if c % 2 == 1:
                rep = '%s.10' % year
            else:
                rep = '%s.4' % year

            if code_name[0] == chr(c):
                return rep

    def __base_replacements__(
        self,
        string,
        suite,
        stream,
        ):
        """Basic replacements of:
                    - %(distro)s:       code name of the distro
                    - %(version)s:      suite version name, i.e 12.04
                    - %(date)s:         todays date in YYYY-MM-DD format
                    - %(stream)s:       stream of build, i.e. server, desktop
        """

        return string % {
            'distro': suite,
            'version': self.code_name_to_version(suite),
            'date': date.today(),
            'stream': stream,
            }

    def __del_replacements__(
        self,
        string,
        suite,
        stream,
        tag,
        build_serial,
        ):
        """Basic replacements of:
                    - %(distro)s:        code name of the distro
                    - %(version)s:      suite version name, i.e 12.04
                    - %(date)s:         todays date in YYYY-MM-DD format
                    - %(tag)s:          build tag, i.e. release, beta
                    - %(build_serial)s: build serial of build, i.e. 20120101
        """

        return string % {
            'distro': suite,
            'version': self.code_name_to_version(suite),
            'date': date.today(),
            'stream': stream,
            'build_serial': build_serial,
            'tag': tag,
            'release_tag': tag,
            }

    def current_registrations(self, distro, d_conf):
        """Checks to find out if an update is needed"""

        if not d_conf.list_cmd:
            return None

        check_cmd = self.__base_replacements__(d_conf.list_cmd, distro,
                d_conf.stream)

        logger.info('Checking registrations for %s' % distro)
        run_cmd = RunCMD(check_cmd, output='log', info=True)
        return run_cmd.output

    def __registersplitter__(
        self,
        regs,
        noarch=False,
        until=None,
        overrun=False,
        release_tag=None,
        ):
        """Iterates on a perline basis, yielding <release_tag> <build_serial> <arch>
            noarch: results are compressed independent of the architecture
            overrun: don't error out on index errors
            until: the max count from the bottom of the list
        """

        if regs:

            count = 0
            if noarch:
                anon = {}
                for line in regs.splitlines():
                    (tag, serial, arch) = line.split()

                    if tag not in anon:
                        anon[tag] = []

                    if serial not in anon[tag]:
                        anon[tag].append(serial)

                for tag in anon:

                    if release_tag and release_tag != tag:
                        continue

                    for serial in anon[tag]:
                        yield (anon[tag], serial, None)
                        count += 1

                        if until and count >= until:
                            return
            else:

                for line in regs.splitlines():
                    try:
                        (tag, serial, arch) = line.split()

                        if release_tag and release_tag != tag:
                            continue

                        yield (tag, serial, arch)
                        count += 1

                        if until and count >= until:
                            return
                    except IndexError:

                        if not overrun:
                            logger.warn("""
'Registration command failed to return properly formated text\n
\t\t\tPlease make sure the command returns <tag> <build_serial> <arch>'
""")
                            logger.info("""
Failing-safe. Assuming that images has been registered already
""")
                            yield (None, None, None)

    def is_registered(
        self,
        registrations,
        tag,
        build_serial,
        arch,
        ):
        """Returns True is registered or false if not"""

        for (r_tag, r_serial, r_arch) in \
            self.__registersplitter__(registrations):
            if tag == r_tag and build_serial == r_serial and arch \
                == r_arch:
                return True

        return False

    def run_cmd(
        self,
        cmd,
        cmd_name,
        log_file,
        spaces='',
        last_line=False,
        ):
        """Runs some arbitrary command. Returns True/False and the last if requested"""

        logger.info('%sStarting %s command' % (spaces, cmd_name))

        run_cmd = RunCMD(cmd, spaces=spaces, info=True)
        run_cmd.write_log(log_file)

        if run_cmd.was_successful():
            logger.info('%s%s complete' % (spaces, cmd_name))

            if last_line:
                line = run_cmd.last_line()

                if line:
                    return (True, line)
                else:
                    return (True, None)
            else:

                return (True, None)
        else:

            logger.critical('%sFAILED to run %s' % (cmd_name, spaces))
            return (False, None)

    def deregister_build_serial(
        self,
        cmd,
        distro,
        stream,
        build_serial,
        tag,
        log_file,
        spaces='',
        ):
        """De-register an image
        """

        bs = '%s %s %s %s' % (distro, stream, build_serial, tag)

        logger.info('%sLogging to %s' % (spaces, log_file))
        del_cmd = self.__del_replacements__(cmd, distro, stream, tag,
                build_serial)

        run_cmd = RunCMD(del_cmd, output='log', info=True,
                         spaces='       ')
        run_cmd.write_log(log_file)

        if not run_cmd.was_successful():
            logger.critical('FAILED TO DEREGISTER IMAGE! NOT PROCEEDING WITH CLEANUP!'
                            )
            return
        else:
            logger.info('%sSuccessfully de-registered %s' % (spaces,
                        bs))

        return run_cmd.was_successful()

    def truncate_build_list(
        self,
        d_conf,
        tag,
        serials,
        spaces='',
        ):
        """Prevent processing more images than are allowed to be registered"""

        logger.info('%sCaluclating the builds to keep/copy' % spaces)

        serials = sorted(serials)
        tag = tag.split('-')[0]
        release_tags = ('release', 'alpha', 'beta', 'rc')
        keep = None

        if tag in release_tags:
            keep = d_conf.max_milestones
        else:
            keep = d_conf.max_dailies

        logger.info('%sKeep rule defined as: %s' % (spaces, keep))

        keep = str(keep.lower()).replace('n-', '')

        if keep == 'all':
            logger.info('%sIncluding all serials in list' % spaces)
            return serials
        elif keep == 'latest':

            logger.info('%sReturing latest serial' % spaces)

            try:
                return [serials[-1]]
            except:
                return serials
        else:

            logger.info('%sCalculating serials to return' % spaces)

            keep = int(keep)
            logger.info('%s  Keep count is %s' % (spaces, keep))

            if keep >= len(serials):
                logger.info('%s  All serials in list are valid'
                            % spaces)
                return serials
            else:

                delta = len(serials) - keep
                new_serials = []

                logger.info('%s  Returning %s serials out of %s'
                            % (spaces, delta, len(serials)))

                for index in range(delta, len(serials)):
                    new_serials.append(serials[index])

                return sorted(new_serials)

    def iter_recorded_registration_tags(
        self,
        log,
        distro,
        stream,
        tag,
        spaces='',
        ):
        rl = self.read_registrations(log, spaces)

        if distro in rl:
            for s in rl[distro]:
                if s == stream:
                    for t in rl[distro][s]:
                        if t == tag:
                            for bs in rl[distro][s][t]:
                                yield bs

    def is_recorded(
        self,
        build_serial,
        arch,
        log,
        distro,
        stream,
        tag,
        spaces='',
        ):
        """check if a build_serial, arch, distro, stream was recorded as registered"""

        for record in self.iter_recorded_registration_tags(log, distro,
                stream, tag, spaces=''):
            if record.build_serial == build_serial:
                if record.get_value(arch, 'registration'):
                    return True

        return False

    def del_registration(
        self,
        reg,
        log,
        spaces='',
        ):

        rl = self.read_registrations(log, spaces)
        crl = copy.copy(rl)
        removed = False

        for s in rl[reg.distro]:
            for t in rl[reg.distro][s]:
                for bs in rl[reg.distro][s][t]:
                    if reg.uuid == bs.uuid:
                        crl[reg.distro][s][t].remove(bs)
                        logger.info('%sRemoved record %s' % reg.uuid)

        if removed:
            try:
                with open(log, 'wb') as w:
                    pickle.dump(crl, w, -1)
                w.close()

                return True
            except Exception as e:
                logger.critical('Unable to write registration' \
                                'pickle after deletion\n%s' % e)
                pass

        return False

    def read_registrations(self, log, spaces=''):
        try:
            registration_logs = None

            if not os.path.exists(log):
                logger.info('%sRegistration log does not exist.'
                            % spaces)
                return {}

            with open(log, 'rb') as log:
                registration_logs = pickle.load(log)
            log.close()

            return registration_logs
        except pickle.PickleError, e:

            logger.info('%sFailed to read log, assuming that pickling is bad!'
                         % spaces)
            return {}
        except IOError as e:

            logger.info('%sFailed to read registration log!\n%s' % (spaces,e))
            pass
        except Exception, e:

            logger.info('%Error reading registration log!\n%s' % (spaces,e))
            pass

    def del_record(
        self,
        log,
        pr,
        spaces='',
        ):
        """Deletes a record and the local files"""

        pass

    def record_records(
        self,
        log,
        pr,
        spaces='',
        ):
        """Saves/merges the on-disk pickle of of the registration objects"""

        try:
            rl = self.read_registrations(log, spaces=spaces)

            for distro in pr:
                logger.info('%sProcessing records for %s' % (spaces,
                            distro))

                # Make sure that the data-structure exists in the read-in records

                if distro not in rl:
                    logger.info('%s  Adding %s to record log'
                                % (spaces, distro))
                    rl[distro] = {}

                for stream in pr[distro]:
                    if stream not in rl[distro]:
                        rl[distro][stream] = {}

                for tag in pr[distro][stream]:
                    if tag not in rl[distro][stream]:
                        rl[distro][stream][tag] = []

            # Add/merge records

            for d in pr:
                for s in pr[d]:
                    for t in pr[d][s]:
                        logger.info('%s  %s-%s has %s records'
                                    % (spaces, d, s, len(pr[d][s][t])))

                        for bs in pr[d][s][t]:
                            found = None
                            duplicates = []
                            for old_bs in rl[distro][s][t]:
                                if old_bs.build_serial \
                                    == bs.build_serial:
                                    duplicates.append(old_bs)
                                    found = old_bs

                            if found:
                                logger.info('%s  Merging records for %s %s %s %s (%s)'
                                         % (
                                    spaces,
                                    bs.distro,
                                    s,
                                    t,
                                    bs.build_serial,
                                    found.uuid,
                                    ))
                                found.merge(bs)

                                for dup in duplicates:
                                    if dup.uuid != found.uuid:
                                        rl[distro][s][t].remove(dup)
                                        logger.info('%s  Purged duplicate with obj id: %s'
         % (spaces, dup.uuid))
                            else:

                                logger.info('%s  Adding new record to log %s %s %s %s'
                                         % (spaces, bs.distro, s, t,
                                        bs.build_serial))
                                rl[distro][stream][tag].append(bs)

            with open(log, 'wb') as w:
                pickle.dump(rl, w, -1)
            w.close()

            logger.info('%sRecorded registration in logs' % spaces)
        except Exception, e:

            logger.critical('ERROR reading recorded registration pickle!\n%s'
                             % e)
            pass

    def add_record(
        self,
        pr,
        pack,
        **kargs
        ):
        """record build using the ProcessRecord Class to make organization easier"""

        d_conf = pack['d_conf']
        local_file = pack['local_file']
        log_files = pack['log_files']
        directory = pack['directory']
        build_serial = pack['build_serial']
        tag = pack['release_tag']
        arch = pack['arch']
        distro = pack['distro']
        stream = pack['stream']

        if type(pr).__name__ != 'dict':
            pr = {}

        if distro not in pr:
            pr[distro] = {stream: {tag: []}}

        if stream not in pr[distro]:
            pr[distro][stream] = {tag: []}

        if tag not in pr[distro][stream]:
            pr[distro][stream][tag] = []

        record = None
        for r in pr[distro][stream][tag]:
            if r.build_serial == build_serial:
                record = r

        if not record:
            record = ProcessRecord(
                distro,
                build_serial,
                tag,
                stream=stream,
                config=d_conf,
                directory=directory,
                )
            pr[distro][stream][tag].append(record)

        record.add(arch, file=local_file, logs=log_files)

        for key in kargs:
            if key == 'spaces':
                spaces = kargs[key]
            else:

                record.set(key, kargs[key], arch)

        logger.info('%sRecored record' % spaces)
        return pr

    def runner(self):
        """the main guts of the process"""

        conf = self.configuration
        catalog = self.catalog

        for distro in catalog.get_distros():

            if distro not in conf.suites or conf.suites != 'available':
                next

            logger.info('Creating configuration for %s' % distro)
            d_conf = LocalConfiguration(catalog, conf, distro)

            included = []
            included.extend(d_conf.publish)
            included.extend(d_conf.mirror)

            current_registrations = self.current_registrations(distro,
                    d_conf)

            builds = catalog.distro_builds(distro, d_conf.stream)
            logger.info('Found %s build(s) for %s %s'
                        % (builds.count(), builds.distro,
                        builds.stream))
            new_serial_count = 0

            tags = {}

            # Count up the number of releases

            for build in builds.iter_builds():
                if build.release_tag not in tags:
                    tags[build.release_tag] = [build.build_serial]
                else:

                    tags[build.release_tag].append(build.build_serial)

            for tag in tags:
                tags[tag] = sorted(tags[tag])
                logger.info('  Found %s serials for %s:           %s'
                            % (len(tags[tag]), tag,
                            ' '.join(tags[tag])))

                tags[tag] = self.truncate_build_list(d_conf, tag,
                        tags[tag], spaces='   ')
                logger.info('  Limitied new serials to %s for %s: %s'
                            % (len(tags[tag]), tag,
                            ' '.join(tags[tag])))

            # Now process them

            publish = []
            for build in builds.iter_builds():
                logger.info('  Build Serial: %s, Release Tag: %s, Arches: %s '
                             % (build.build_serial, build.release_tag,
                            ' '.join(build.arches())))

                if build.build_serial not in tags[tag]:
                    logger.info('      Skipped. This build is excluded due to max_dailies or max_milestone setting'
                                )
                    continue

                new_serial_count += 1

                for (arch, sub_build) in build.iter_arches():
                    logger.info('    Build: %s' % arch)
                    reg = self.is_registered(current_registrations,
                            build.release_tag, build.build_serial, arch)
                    rec = self.is_recorded(
                        build.build_serial,
                        arch,
                        d_conf.reg_log,
                        distro,
                        builds.stream,
                        build.release_tag,
                        spaces='',
                        )

                    if arch not in d_conf.arches and arch \
                        not in d_conf.mirror_arches:
                        logger.info('       Not configured for processing; skipping'
                                    )
                        continue

                    # check registrations
                    #  reg: the true/false value from checking the registration command
                    #  rec: the true/false value from loooking in the logs

                    if reg and rec:
                        logger.warn('       Already registered, skipping processing'
                                    )
                        continue

                    elif reg and not rec:
                        logger.warn('       Image was registered _outside_ of this program!'
                                    )
                        continue

                    elif not reg and rec:
                        logger.warn('       Image is recorded in logs as registered, but is missing.'
                                    )
                        logger.warn('       This is likely due to a problem with your registration script!'
                                    )
                        logger.warn('       Re-processing image!')

                    else:
                        logger.info('      Arch %s is cleared for processing'
                                     % arch)

                    first_types = []
                    first_files = None
                    if d_conf.check_types:
                        for ft in d_conf.check_types:
                            if ft in d_conf.mirror or ft \
                                in d_conf.publish:
                                first_types.append(ft)

                        if len(first_types) > 0:
                            first_files = ' '.join(first_types)
                            logger.info('      Fetching files [ %s ] first to process checking'
                                     % first_files)
                    else:

                        logger.info('      No file types declared for checking'
                                    )

                    for fd in \
                        sub_build.iter_files(unpacked=d_conf.unpacked,
                            first_types=first_files):
                        if fd.file_type not in included:
                            continue

                        # Do the work
                        local_file = '%s/%s' % (d_conf.sync_dir,
                                fd.path)
                        pristine_file = '%s/%s' % (d_conf.pristine,
                                fd.path)
                        log_file_base = '%s/%s' % (d_conf.process_logs,
                                fd.path)
                        log_files = []
                        logger.info('      File: %s' % fd.path)
                        logger.info('         File Type:     %s'
                                    % fd.file_type)
                        logger.info('         Expected SHA1: %s'
                                    % fd.sha1)
                        logger.info('         Fetch URL:     %s' % fd.url)
                        logger.info('         Local Path:    %s'
                                    % local_file)
                        logger.info('         Log Path:      %s'
                                    % log_file_base)

                        if not d_conf.pristine:
                            pristine_file = None

                        furl = URLFetcher(
                            fd.url,
                            fd.sha1,
                            local_file,
                            conf.download_log,
                            ignore_cache=self.no_cache,
                            fake=self.dry_run,
                            pristine=pristine_file,
                            spaces='         ',
                            )

                        if not furl.get():
                            print furl
                            raise Exception('Failed to download %s'
                                    % fd.url)

                        replacements = self.__replacements__(
                            fd,
                            build,
                            arch,
                            distro,
                            local_file,
                            pristine_file,
                            )
                        logger.info('         Download SHA1: %s'
                                    % furl.checksum)

                        if fd.file_type in d_conf.check_types \
                            and d_conf.check_cmd:
                            logger.info('         File type is configured as a check file'
                                    )

                            cmd = d_conf.check_cmd % replacements
                            log_file = '%s.check-log.txt' \
                                % log_file_base
                            log_files.append(log_file)
                            if not self.run_cmd(cmd, 'Check Rules',
                                    log_file, spaces='             '):
                                logger.info('         No further processing for this build needed'
                                        )
                                break

                        if arch in d_conf.mirror_arches:
                            logger.info('         Architecture rule is to only mirror this file'
                                    )
                            continue

                        if fd.file_type in d_conf.customize_types:
                            logger.info('         File type is configured for customization'
                                    )

                            cmd = d_conf.custom_cmd % replacements
                            log_file = '%s.customization.log' \
                                % log_file_base
                            log_files.append(log_file)
                            spaces = '             '
                            (status, out) = self.run_cmd(cmd,
                                    'Customization', log_file,
                                    spaces=spaces, last_line=True)
                            if status:
                                logger.info('%sCustomized file'
                                        % spaces)

                                emitted_re = \
                                    re.compile('^::EMITTED-FILE::.*')
                                for line in out.splitlines():
                                    if emitted_re.match(line):
                                        local_file = \
                                            line.replace('::EMITTED-FILE::', '')

                                        replacements['local'] = \
                                            local_file

                                        logger.info('%sCustom command emitted: %s'
                                             % (spaces, local_file))

                                        logger.info('%sUsing emitted file for publishing'
                                             % spaces)
                            else:

                                logger.critical('       FAILED TO CUSTOMIZE. Processing of this build\n       IS ABORTED! Other builds will be processed'
                                        )
                                publish = None
                                self.exit_code += 1
                                break

                        if fd.file_type in d_conf.publish:
                            logger.info('         File type is queued for publishing'
                                    )
                            anon = {
                                'd_conf': d_conf,
                                'replacements': replacements,
                                'log_file': '%s.publish-log.txt' \
                                    % log_file_base,
                                'local_file': local_file,
                                'directory': os.path.dirname(local_file),
                                'build_serial': build.build_serial,
                                'release_tag': build.release_tag,
                                'log_files': log_files,
                                'arch': arch,
                                'distro': distro,
                                'stream': builds.stream,
                                }

                            publish.append(anon)

            # new registrations

            process_records = {}
            if publish:
                pub_count = 0
                logger.info('  Publishing %s images for %s'
                            % (len(publish), distro))

                for p in publish:

                    # unpack the dict describing the file to publish

                    d_conf = p['d_conf']
                    replacements = p['replacements']
                    local_file = p['local_file']
                    build_serial = p['build_serial']
                    release_tag = p['release_tag']
                    arch = p['arch']
                    distro = p['distro']
                    spaces = '      '

                    cmd = d_conf.publish_cmd % replacements
                    cmd_n = 'Publicising'

                    logger.info('    Processing %s' % local_file)
                    status = None
                    reg = None

                    if self.dry_run:
                        logger.info('%sSimulated publishing.' % spaces)
                        cmd = '/bin/true'
                        reg = (release_tag, build_serial, arch,
                               'FAKE-%s_%s' % (build_serial, arch))
                        logger.info('%sRegistration information is simulated'
                                     % spaces)
                    else:
                        (status, reg) = self.run_cmd(cmd, cmd_n,
                                log_file, spaces=spaces, last_line=True)
                        reg = reg.split()
                        logger.info('%sRegistration returned: %s'
                                    % (spaces, reg))

                    if status:

                        (r_tag, r_build_serial, r_arch, cloud_reg) = reg
                        if r_tag == release_tag and r_arch == arch \
                            and build_serial == r_build_serial:
                            logger.info('%sImage registered as %s'
                                    % (spaces, cloud_reg))
                            process_records = \
                                self.add_record(process_records, p,
                                    spaces=spaces,
                                    registration=cloud_reg,
                                    published=True)
                            pub_count += 1
                        else:

                            logger.info('%SFAILED TO PUBLISH/REGISTER IMAGE! Aborting set!'
                                     % spaces)
                            process_records = \
                                self.add_record(process_records, p,
                                    spaces=spaces, registration=None,
                                    published=False)
                            break
                    else:

                        logger.critical('%sFAILED TO PUBLISH IMAGE! Publishing of this build\n'

                                + '%s%sIS ABORTED! Other builds will be processed'
                                 % (spaces, spaces, spaces))

                logger.info('    Published/regestered %s images'
                            % pub_count)

                if pub_count > 0 and d_conf.reg_log:
                    logger.info('    Recording registrations and logs to: %s'
                                 % d_conf.reg_log)
                    self.record_records(d_conf.reg_log,
                            process_records, spaces='    ')

            # handle de-registrations

            for tag in catalog.iter_distro_release_types(distro,
                    stream=d_conf.stream):
                logger.info('  Processing de-registrations for %s %s tagged %s'
                             % (distro, d_conf.stream, tag))

                if not d_conf.unpublish_cmd:
                    logger.info('    Skipping. No unpublish command defined')
                    continue

                keep = d_conf.max_milestones

                if tag == 'daily':
                    keep = d_conf.max_dailies

                logger.info('    Rule is to keep: %s' % keep)
                spaces = '     '

                serials = {}
                for bs in \
                    self.iter_recorded_registration_tags(d_conf.reg_log,
                        distro, d_conf.stream, tag, spaces='     '):
                    serials[bs.build_serial] = bs

                keep_list = self.truncate_build_list(d_conf, tag,
                        sorted(serials.keys()), spaces='     ')
                logger.info('%sKeeping %s serials:       %s' % (spaces,
                            len(keep_list), ' '.join(keep_list)))

                drop_list = []
                for s in sorted(serials.keys()):
                    if s not in keep_list:
                        drop_list.append(s)

                if len(drop_list) > 0:
                    logger.info('%sDeregistering %s serials: %s'
                                % (spaces, len(drop_list),
                                ' '.join(drop_list)))

                    for s in drop_list:
                        sspace = '%s  ' % spaces
                        bs = serials[s]

                        logger.info('%sDe-registration for %s %s %s'
                                    % (spaces, bs.distro, bs.stream,
                                    bs.build_serial))

                        log_file = \
                            '%s/deregistration/%s-%s-%s-%s.log.txt' \
                            % (d_conf.process_logs, bs.distro,
                               bs.stream, bs.build_serial,
                               bs.release_tag)

                        deregistered = self.deregister_build_serial(
                            d_conf.unpublish_cmd,
                            bs.distro,
                            bs.stream,
                            bs.build_serial,
                            bs.release_tag,
                            log_file,
                            spaces=sspace,
                            )

                        if deregistered:
                            logger.info('%sProceeding with local purging'
                                     % spaces)

                            e = bs.purge(confirm=True)
                            if isinstance(e, Exception):
                                logger.critical('FAILED to DELETE DIRECTORY!'
                                        )
                                logger.critical(e)
                                logger.info('%sSkipping removal of local records'
                                         % spaces)
                            else:

                                logger.info('%sPurged %s' % (spaces, e))

                                if self.del_registration(bs,
                                        d_conf.process_logs,
                                        spaces=sspace):
                                    logger.info('%sDeregistration complete')
                                else:

                                    logger.critical('%sFAILED to remove local record')
                        else:

                            logger.info('%sSkipping rest of de-registration steps'
                                     % spaces)
                else:

                    logger.info('No images to de-register')

# vi: ts=4 expandtab
