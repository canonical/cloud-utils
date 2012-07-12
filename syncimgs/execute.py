#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 21 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## Execution control module. This script handles the hand-of-custom scripts.

import logging
import tempfile
import traceback
import os
import subprocess
from syncimgs.easyrep import EasyRep
from subprocess import Popen, PIPE

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('_sync-image_')
logger.setLevel(logging.DEBUG)


class RunCMD(EasyRep):

    def __init__(
        self,
        command,
        spaces='',
        **kargs
        ):
        self.cmd = command
        self.output = None
        self.spaces = spaces
        temp_f = self.__write__()

        try:

            if 'info' in kargs:
                if kargs['info']:
                    count = 1
                    cmds = command.splitlines()

                    if len(cmds) > 1:
                        logger.info('%sExecuting %s commands'
                                    % (spaces, len(cmds)))
                        for line in command.splitlines():
                            logger.info('%s   %s: %s' % (spaces, count,
                                    line))
                            count += 1
                    else:

                        logger.info('%sExecuting one command' % spaces)
                        logger.info('%s   %s' % (spaces,
                                    '\n'.join(cmds)))

            run_cmd = Popen([temp_f], shell=True, stdout=PIPE, stderr=PIPE)
            (self.output, self.error) = run_cmd.communicate()

            if 'output' in kargs:
                if kargs['output'] == 'screen':
                    print self.output
                elif kargs['output'] == 'log':

                    for line in self.output.splitlines():
                        logger.info('%sCMD stdout: %s ' % (self.spaces,
                                    line))

                    for line in self.error.splitlines():
                        logger.info('%sCMD stderr: %s ' % (self.spaces,
                                    line))

            self.return_code = run_cmd.poll()

            if self.was_successful():
                logger.info('%sCommand set exit code was %s [ %s ]'
                            % (self.spaces, self.return_code,
                            self.was_successful()))
            else:
                logger.warn('%sCOMMAND FAILED!' % self.spaces)

            os.unlink(temp_f)
        except subprocess.CalledProcessError, e:

            logger.debug(traceback.format_exc(e))
            raise Exception('%sCommand returned %s: %s' % (self.spaces,
                            e.returncode, temp_f))
        except OSError, e:

            logger.debug(traceback.format_exc(e))
            raise Exception('%sCmd failed to execute: %s'
                            % (self.spaces, temp_f))

    def was_successful(self):
        if self.return_code == 0:
            return True
        else:

            return False

    def last_line(self):
        line = None
        for line in self.iter_output():
            pass

        return line

    def iter_output(self):
        if not self.output:
            yield None
            return

        for line in self.output.splitlines():
            yield line

    def write_log(self, log):

        try:
            dirname = os.path.dirname(log)

            if not os.path.exists(dirname):
                logger.info('%sCreating directory %s' % (self.spaces,
                            dirname))
                os.makedirs(dirname)

            with open(log, 'wb') as logfile:
                logfile.write(self.output)
            logfile.close()

            logger.info('%sLog file saved: %s' % (self.spaces, log))
        except IOError, e:

            logger.info('Error while saving file!\n%s' % e)
            pass

    def __write__(self):

        try:
            (temp_fh, temp_n) = tempfile.mkstemp()
            temp_f = open(temp_n, 'w')
            temp_f.write(self.cmd)
            temp_f.close()

            os.chmod(temp_n, 10700)
            logger.info('%sWrote script to %s' % (self.spaces, temp_n))

            os.close(temp_fh)
            return temp_n
        except IOError as e:
            logger.critical('Unable to buffer command file: %s' % e)

        return None


# bob = RunCMD("/bin/false", output="log")
# print bob.was_successful()

# vi: ts=4 expandtab
