#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2014 California Institute of Technology.
# License: 3-clause BSD.  The full license text is available at:
#  - http://trac.mystic.cacr.caltech.edu/project/pathos/browser/dill/LICENSE

from __future__ import absolute_import

from .dill import dump, dumps, load, loads, dump_session, load_session, \
    Pickler, Unpickler, register, copy, pickle, pickles, \
    HIGHEST_PROTOCOL, DEFAULT_PROTOCOL, PicklingError, UnpicklingError, \
    HANDLE_FMODE, CONTENTS_FMODE, FILE_FMODE
from . import source, temp, detect

# make sure "trace" is turned off
detect.trace(False)

try:
    from imp import reload
except ImportError:
    pass

# put the objects in order, if possible
try:
    from collections import OrderedDict as odict
except ImportError:
    try:
        from ordereddict import OrderedDict as odict
    except ImportError:
        odict = dict
objects = odict()
# local import of dill._objects
#from . import _objects
#objects.update(_objects.succeeds)
#del _objects

# local import of dill.objtypes
from . import objtypes as types

def load_types(pickleable=True, unpickleable=True):
    """load pickleable and/or unpickleable types to dill.types"""
    # local import of dill.objects
    from . import _objects
    if pickleable:
        objects.update(_objects.succeeds)
    else:
        [objects.pop(obj,None) for obj in _objects.succeeds]
    if unpickleable:
        objects.update(_objects.failures)
    else:
        [objects.pop(obj,None) for obj in _objects.failures]
    objects.update(_objects.registered)
    del _objects
    # reset contents of types to 'empty'
    [types.__dict__.pop(obj) for obj in list(types.__dict__.keys()) \
                             if obj.find('Type') != -1]
    # add corresponding types from objects to types
    reload(types)

def extend(use_dill=True):
    '''add (or remove) dill types to/from pickle'''
    from .dill import _revert_extension, _extend
    if use_dill: _extend()
    else: _revert_extension()
    return

extend()

del absolute_import
del odict

# end of file
