#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 21 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

## This provides the functionality of downloading and validating a file

from syncimgs.easyrep import EasyRep
import pickle
import os
import shutil
import urllib2
import logging
import hashlib

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger('_sync-image_')


class URLFetcher(EasyRep):

    def __init__(
        self,
        url,
        sha1,
        local_location,
        download_log,
        ignore_cache=False,
        pristine=None,
        spaces='',
        fake=False,
        ):

        self.sha1 = sha1
        self.url = url
        self.local_location = local_location
        self.download_log = download_log
        self.pristine = pristine
        self.checksum = 'INVALID'
        self.spaces = spaces
        self.fake = fake  # Useful for dry run
        self.ignore_cache = ignore_cache
        self.cache = {'pristine': {}, 'processed': {}}

        if self.fake:
            self.checksum = sha1

    def __checksum__(self, location=None, block_size=2 ** 20):
        hsh = hashlib.sha1()
        checksum = None

        if not location:
            location = self.local_location
            attribute_name = 'checksum'
        else:
            attribute_name = '%s_checksum' % location

        try:
            l_file = open(location, 'rb')

            while True:
                data = l_file.read(block_size)
                if not data:
                    break
                hsh.update(data)

            checksum = hsh.hexdigest()
            setattr(self, attribute_name, checksum)
        except IOError, e:

            logger.critical('Unable to open/read file %s' % location)
        except Exception, e:

            logger.critical('Exception encountered while getting checksum\n%s'
                             % e)
        finally:

            pass

        if self.sha1:
            if self.sha1 == checksum:
                return True
            else:
                return False

        return True

    def __purge_from_cache__(self, sha1, spaces=""):

        try:
            if self.sha1 in self.cache['pristine']:
                del self.cache['pristine'][self.sha1]

            if self.sha1 in self.cache['processed']:
                del self.cache['pristine'][self.sha1]
        except Exception:

            logger.info('%sFailed to %s purge from cache' % (spaces,
                        sha1))

    def check_logs_copy(self):
        if not self.download_log:
            return False

        if not os.path.isfile(self.download_log):
            return False

        if self.ignore_cache:
            return False

        setattr(self, 'cache_found', False)

        try:
            local_cache = None
            cache = None
            with open(self.download_log, 'rb') as f:
                try:
                    cache = pickle.load(f)
                except:
                    self.cache = {'pristine': {}, 'processed': {}}
                    return False
            f.close()

            if not cache or type(cache).__name__ != 'dict':
                self.cache = {'pristine': {}, 'processed': {}}
                return False
            else:
                self.cache = cache

            # Check pristine first, then the regular downloads

            if self.sha1 in cache['pristine']:
                local_cache = cache['pristine'][self.sha1]
            elif self.sha1 in cache['processed']:

                local_cache = cache['processed'][self.sha1]
            else:

                return False

            # Now verify that the file exists

            if os.path.isfile(local_cache):
                logger.info('%sFound cached copy of SHA1 %s'
                            % (self.spaces, self.sha1))
                logger.info('%sCheck validity of downloaded file'
                            % self.spaces)

                if not self.__checksum__(location=local_cache):
                    logger.info('%sCache of SHA1 %s is invalid'
                                % (self.spaces, self.sha1))
                    self.__purge_from_cache__(self.sha1)
                    return False
                else:

                    logger.info('%sUsing cached copy of SHA %s'
                                % (self.spaces, self.sha1))
                    setattr(self, 'checksum', self.sha1)

                if self.pristine and os.path.exists(self.pristine):
                    if local_cache != self.pristine:
                        shutil.copy(local_cache, self.pristine)
                        logger.info('%sCopied new pristine from %s'
                                    % (self.spaces, local_cache))
                        shutil.copy(local_cache, self.local_location)
                        logger.info('%sCopied from pristine: %s'
                                    % (self.spaces,
                                    self.local_location))
                        return self.local_location
                    else:

                        shutil.copy(local_cache, self.local_location)
                        logger.info('%sCopied from pristine: %s'
                                    % (self.spaces,
                                    self.local_location))
                        return self.pristine
                elif local_cache != self.local_location:

                    shutil.copy(local_cache, self.local_location)
                    logger.info('  Copied from cache: %s'
                                % self.local_location)
                    return self.local_location
            else:

                self.__purge_from_cache__(self.sha1)
                return False

            return False
        except IOError, e:

            logger.critical('Error reading the download log %s\n%s'
                            % (self.download_log, e))
            pass
        except Exception, e:

            logger.critical('Exception encountered while checking cache file: %s:\n%'
                             % (self.download_log, e))
            logger.cirtical(type(e).__name__)
            pass

        return False

    def __writelog__(self):

        if not self.download_log:
            return

        try:
            with open(self.download_log, 'wb') as w:
                pickle.dump(self.cache, w)
            w.close()

            logger.info('%sRecorded SHA1 for cache lookup in %s'
                        % (self.spaces, self.download_log))
        except IOError, e:

            logger.critical('Error recording download file')
            pass
        except Exception, e:

            logger.critical('Encountered exception while recording download log\n%s'
                             % e)
            pass

    def get(self):
        try:

            if self.fake:
                logger.info('%sDownload is simulated for testing'
                            % self.spaces)
                return True

            if self.check_logs_copy():
                return True

            local = self.local_location
            dirname = os.path.dirname(self.local_location)

            if not os.path.exists(dirname):
                logger.info('%sCreating directory %s' % (self.spaces,
                            dirname))
                os.makedirs(dirname)

            if self.pristine:
                local = self.pristine
                pristine_dir = os.path.dirname(self.pristine)
                logger.info('%sPristine path: %s' % (self.spaces,
                            local))

                if not os.path.exists(pristine_dir):
                    logger.info('%sCreating local storage directory %s'
                                % (self.spaces, pristine_dir))
                    os.makedirs(pristine_dir)

            l_file = open(local, 'wb')
            r_file = urllib2.urlopen(self.url)
            l_file.write(r_file.read())
            l_file.close()

            if self.pristine:
                shutil.copy(self.pristine, self.local_location)

            if not self.__checksum__():
                raise Exception('CHECKSUM_ERROR', 'Expected %s, got %s'
                                % (getattr(self, 'sha1'), getattr(self,
                                'checksum')))

            if self.pristine:
                self.cache['pristine'][self.checksum] = self.pristine
            else:

                self.cache['processed'][self.checksum] = \
                    self.local_location

            self.__writelog__()

            return True
        except OSError, e:

            logger.critical('Error while creating directory %s\n%s'
                            % (dirname, e))
            pass
        except IOError, e:

            logger.critical('Unable to write file %s\n%s'
                            % (self.local_location, e))
            pass
        except Exception, e:

            logger.critical('Exception encountered while downloading file from %s\n%s'
                             % (self.url, e))
            pass


# u = URLFetcher("http://cloud-images.ubuntu.com/query2/builds.json", "a9339f0ab5010cb1f94015223e1c5ab1697589b0", "/tmp/test.stuff", "/tmp/download.log")
# u.get()
# print u
# print u.checksum

# vi: ts=4 expandtab
