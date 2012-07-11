#!/usr/bin/python
# -*- coding: utf-8 -*-

## Copyright (C) 2011 Ben Howard <ben.howard@canonical.com>
## Date: 25 February 2012
##
## This comes with ABSOLUTELY NO WARRANTY; for details see COPYING.
## This is free software, and you are welcome to redistribute it
## under certain conditions; see copying for details.

import json


class EasyRep:

    def __init__(self):
        pass

    def __get__(self, name):
        try:
            return getattr(self, name)
        except:

            return None

    def __getitem__(self, name):
        return self.__get__(name)

    def json(self, indent=4, sort=True):
        ret_string = {}
        for i in self.__dict__.keys():
            val = getattr(self, i)

            if isinstance(val, EasyRep):
                obj = {}
                for key in val.callables():
                    obj[key] = val.get(key)

                val = obj
            ret_string[i] = val

        return json.dumps(ret_string, indent=indent, skipkeys=True,
                          sort_keys=sort)

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

    def iter(self, name):
        var = self.__get__(name)

        if type(var).__name__ == 'list' or type(var).__name__ == 'dict':
            for i in var:
                yield i
        else:

            yield var

    def callables(self):
        for i in self.__dict__.keys():
            yield i

    def get_callables(self):
        calls = [i for i in self.callables()]
        return calls

    def get(self, name):
        return self.__get__(name)


# vi: ts=4 expandtab
