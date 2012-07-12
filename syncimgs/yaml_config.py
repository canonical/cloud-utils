#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

import logging
import yaml

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('_sync-image_')


class YAMLConfig(yaml.YAMLObject):

    yaml_tag = u'!Control'
    logger.info('Loading YAML configuration')

    def __init__(self, **kargs):
        for key in kargs:
            setattr(key, kargs[key])

    def fix_strings(self):
        for i in self.__dict__.keys():
            value = getattr(self, i)

            if type(value).__name__ == 'str':
                new_value = value.replace('STR_REPLACEMENT', '%')
                setattr(self, i, new_value)

        # set None to undefined

        required = [
            'sync_dir',
            'pristine',
            'history_log',
            'process_logs',
            'download_log',
            'host_url',
            'enable_overrides',
            'unpacked',
            'gpg_validate',
            'suites',
            'stream',
            'publish',
            'mirror',
            'name_convention',
            'copy_pristine',
            'max_dailies',
            'max_milestones',
            'keep_pre_release',
            'arches',
            'mirror_arches',
            'customize_types',
            'check_types',
            'process_logs',
            ]

        for r in required:
            if self.get(r) is None:
                raise Exception('Configuration variable %s is required'
                                % r)

        optional = [
            'custom_cmd',
            'list_cmd',
            'check_cmd',
            'publish_cmd',
            'publish_cmd',
            'unpublish_cmd',
            ]

        for o in optional:
            if not self.get(o):
                setattr(self, o, None)

    def dump(self):
        for i in self.__dict__.keys():
            print '%s: %s' % (i, getattr(self, i))

    def set(self, name, value):
        setattr(self, name, value)

    def get(self, name):
        try:
            return getattr(self, name)
        except AttributeError:
            return None

    def get_attrs(self):
        for i in self.__dict__.keys():
            yield (i, getattr(self, i))

    def __repr__(self):
        ret_string = None
        for i in self.__dict__.keys():
            if not ret_string:
                ret_string = '%s=%s' % (i, getattr(self, i))
            else:
                ret_string = ret_string + ', %s=%s' % (i, getattr(self,
                        i))

        ret_string = str('%s(%s)' % (self.__class__.__name__,
                         ret_string))
        return ret_string


def read_yaml_file(yaml_file):

    try:

        yml_obj = None

        if yaml_file != '__DEFAULT__':
            yml_file = open(yaml_file, 'r')
            yml_obj = load_yaml(yml_file.readlines())
            yml_file.close()
        else:

            yml_obj = load_yaml(get_default_config())

        yml_obj.fix_strings()
        return yml_obj
    except IOError:
        raise Exception('Unable to read YAML Configuration file')


def load_yaml(string):
    yml = '\n'.join(string)
    yml = yml.replace('%', 'STR_REPLACEMENT')
    yml = yml.replace('\t', '   ')
    yml = yml.replace('#cloudimg-sync-config:', '--- !Control')
    return yaml.load(yml)


def config_help():
    print """
cloudimg-sync is a tool for mirroring, customizing and registering the
Ubuntu Cloud Images on arbitrary clouds using a rule-engine.

Example configuration:
---------------------------
"""
    get_default_config()
    print """
---------------------------

Overrides:

    The default rules will be run against all the suites presented in the JSON
    file. If you define a SuiteRuleOverride, you can define _just_ the rules you
    overriden. Overrides are defined on per suite basis; one can have as many overrides
    per suite as they like. This feature is useful for publishing to many different clouds.

Configuration Elements:

    sync_dir: Directory to place the customized and final images for publishing

    pristine: If you want pristine copies of th images, define this location

    history_log: The name of a file for recording downloads. This is used as a
        cache lookup so that files are not needlessly downloaded. This file is
        stored as a Python Pickle.

    reg_log: This file records the registrations of the images. It is a Python Pickle and
        contains objects that were used during the customization and registrations of
        the images.

    process_logs: /srv/cloudimgs/logs

    download_log: /srv/cloudimgs/download.log

    host_url: http://cloud-images.ubuntu.com/query2/server/release/builds.json

    enable_overrides: True

    gpg_validate: Validate the GPG signature on the JSON file.

    sync_dir: The directory to synchronize the files to

    host_url: The URL to look for the JSON file at

    suite: The code name of the suite to mirror. "available" means all suites presented in the
        JSON

    mirror: Files matching this type will be downloaded, but not published.

    name_convention: How to name the files. If left undefined, files will use the suggested
        name in the JSON. You may use any of the variable replacement strings above.

    copy_pristine: If set to True, a pristine copy of the file will be placed in the pristine
        directory

    max_dailies: The number of daily images to retain. "latest" means to keep only those
        defined in the JSON

        Valid choices are:
            - All
            - n-# (where n latest and # is a number, i.e. n-3 will keep the last three)
            - latest

    max_milestones: The number of milestones to keep. "latest" means to keep only those defined
        in the JSON

        Valid choices are:
            - All
            - n-# (where n latest and # is a number, i.e. n-3 will keep the last three)
            - latest
            - current

    keep_pre_release: When set to "True" alhpa's and beta's will be mirrored.

    arches: What architectures should be mirrored

    mirror_arches: What architectures should be mirrored, but no commands run against

    check_types: Files to run the check commands against

    customize_types: Files to run the customization commands against

Custom Commands:

    Custom commands are lists of arbitrary CLI commands or language commands.
    If your command(s) requires an interpreter, use "#!/..." syntax. For
    example, to run a Perl command, you would use:

        custom_cmd: |
            #!/usr/bin/perl -e -w
            use awesomeness;

            party = awesomeness.get_party(%(local_file)s);

            unless party.failed() {
                party.done(%(sha512)s);
            }


    If using multiple "#!/..." files, it is recommended to use files instead of storing
    these commands in the configuration file.

    To pass arguments to custom commands, you can use any of the string replacements
    explained above. The example above passes the value JSON value of the file's SHA512
    and local file name in the custom command.

    Commands are executed in the order present below. Please note, none of these commands
    are run in a chroot. You are responsable for handling any chroot'ing that you need in
    your script.

        check_cmd: command to check if an image should be published or not. This is
            useful for checking things like the manifest file to look for key update,
            like kernel, glibc, etc.

            This command is called with substitutions. For example:
                /srv/cloudimgs/bin/publish-ec2 %(directory)s %(file)s %(distro)s %(arch)s %(build_serial)s

            This command is run on _each_ file that is listed in the check file list.

            If command exists successfully, then image processing for that specific build serial
                continues normally.

        custom_cmd: Commands for customizing the image, such as installing keys or packages
            These commands _are not_ run in a chroot.

            The command is called with the directory of the downloaded files and the file
                to be customized.

            If the command exits successfully, then the last line should be the name of the
                file to be published. If there is no output from the command, but it
                successfully exits, then the filename is assumed to be unchanged.

        list_cmd: Command(s) to run a query to get a listing of names of
            published and/or mirrored images.

            The command _MUST_ return registrations with the following rules:
                * One registration per line, formated as:
                     <tag> <build_serial> <arch>
                * The command can use the following as parameters:
                    - %(distro)s:       code name of the distro
                    - %(version)s:      suite version name, i.e 12.04
                    - %(date)s:         todays date in YYYY-MM-DD format
                    - %(stream)s:       stream of build, i.e. server, desktop

            This option should be used to _check_ if an image should be processed or not.

            A non-zero exit status is treated as a failure. In the event of a failure the program
                will skip the build serial.

            State tracking is done both locally and by cloud registration if list_cmd is defined.
                The output of list_cmd is authoritative, while the local state tracking is used
                purely for logging and to reduce bandwidth usage.

        publish_cmd: This command published an image.

            The command will be passed substitutions defined above. For example:
                /srv/cloudimgs/bin/publish-ec2 %(directory)s %(file)s %(distro)s %(arch)s %(build_serial)s

                * You must define the inputs.

            This command _MUST_ return the registration following the following rules:
                * Only one image can be registered at a time
                * An image is considered to be registered when it exits succesfully and
                    emits "REGISTERED: <tag> <build_serial> <arch> <cloud_reg_id>" as the last line
                    of output. <cloud_reg_id> should be the cloud dependent registration.

        unpublish_cmd: This command is used to unpublish an image/

            The command must return a non-zero exit status for a failure
            The command will be passed: <tag> <distro> <arch> <build_serial>


    Valid string replacements for list_cmd and unpublish_cmd:
                %(distro)s : distro/suite name, i.e. precise
                %(version)s : the version number string, i.e. 12.043
                %(date)s : today's date
                %(stream)s : the stream, i.e server

    Valid string replacements for the unpublish_cmd:
                %(distro)s : distro/suite name, i.e. precise
                %(version)s : the version number string, i.e. 12.043
                %(date)s : today's date
                %(stream)s : the stream, i.e server
                %(build_serial)s: the build serial of the image
                %(tag)s: the release tag
                %(release_tag)s: the release tag

    Valid string replacements for check_cmd, publish_cmd, and custom_cmd:
                %(distro)s : suite/distro name
                %(suite)s : suite/distro name
                %(build_serial_id)s: unique architecture build serial number
                %(build_serial)s: build serial number, i.e 20120101.1
                %(build_id)s: unique build ID
                %(release_tag)s: the release tag
                %(tag)s: the release tag
                %(file_description)s: the description of the file
                %(file_type)s: the file type, i.e. qcow2, root.tar.gz
                %(sha1)s: the SHA1 of the file
                %(sha512)s: the SHA512 of the file
                %(file_path)s: the remote path of the file
                %(local)s: the local path of the file
                %(date)s: todays date

"""


def get_default_config():
    return """
#cloudimg-sync-config:
sync_dir: /srv/cloudimgs/data
pristine: /srv/cloudimgs/pristine
history_log: /srv/cloudimgs/history.log
reg_log: /srv/cloudimgs/build.log
process_logs: /srv/cloudimgs/logs
download_log: /srv/cloudimgs/download.log
host_url: http://cloud-images.ubuntu.com/query2/server/release/builds.json
enable_overrides: True
unpacked: False
gpg_validate: True
suites: available
stream: server
publish: [ manifest ]
mirror: [ manifest ]
name_convention: None
copy_pristine: True
max_dailies: latest
max_milestones: latest
keep_pre_release: True
arches: [ i386, amd64 ]
mirror_arches: [ armel, armhf ]
customize_types: [ ]
check_types: [ manifest ]
custom_cmd: |
    /srv/cloudimgs/bin/custom.sh
list_cmd: |
    echo "release 20120222 amd64"
check_cmd: |
    /bin/true
publish_cmd: |
    /bin/true
unpublish_cmd: |
    /bin/true
overrides:
"""

# vi: ts=4 expandtab
