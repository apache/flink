#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2014 California Institute of Technology.
# License: 3-clause BSD.  The full license text is available at:
#  - http://trac.mystic.cacr.caltech.edu/project/pathos/browser/dill/LICENSE
"""
Methods for detecting objects leading to pickling failures.
"""

from __future__ import absolute_import
from inspect import ismethod, isfunction, istraceback, isframe, iscode
from .pointers import parent, reference, at, parents, children

from .dill import _trace as trace
from .dill import PY3

def outermost(func): # is analogous to getsource(func,enclosing=True)
    """get outermost enclosing object (i.e. the outer function in a closure)

    NOTE: this is the object-equivalent of getsource(func, enclosing=True)
    """
    if PY3:
        if ismethod(func):
            _globals = func.__func__.__globals__ or {}
        elif isfunction(func):
            _globals = func.__globals__ or {}
        else:
            return #XXX: or raise? no matches
        _globals = _globals.items()
    else:
        if ismethod(func):
            _globals = func.im_func.func_globals or {}
        elif isfunction(func):
            _globals = func.func_globals or {}
        else:
            return #XXX: or raise? no matches
        _globals = _globals.iteritems()
    # get the enclosing source
    from .source import getsourcelines
    try: lines,lnum = getsourcelines(func, enclosing=True)
    except: #TypeError, IOError
        lines,lnum = [],None
    code = ''.join(lines)
    # get all possible names,objects that are named in the enclosing source
    _locals = ((name,obj) for (name,obj) in _globals if name in code)
    # now only save the objects that generate the enclosing block
    for name,obj in _locals: #XXX: don't really need 'name'
        try:
            if getsourcelines(obj) == (lines,lnum): return obj
        except: #TypeError, IOError
            pass
    return #XXX: or raise? no matches

def nestedcode(func): #XXX: or return dict of {co_name: co} ?
    """get the code objects for any nested functions (e.g. in a closure)"""
    func = code(func)
    if not iscode(func): return [] #XXX: or raise? no matches
    nested = []
    for co in func.co_consts:
        if co is None: continue
        co = code(co)
        if co: nested.append(co)
    return nested

def code(func):
    '''get the code object for the given function or method

    NOTE: use dill.source.getsource(CODEOBJ) to get the source code
    '''
    if PY3:
        im_func = '__func__'
        func_code = '__code__'
    else:
        im_func = 'im_func'
        func_code = 'func_code'
    if ismethod(func): func = getattr(func, im_func)
    if isfunction(func): func = getattr(func, func_code)
    if istraceback(func): func = func.tb_frame
    if isframe(func): func = func.f_code
    if iscode(func): return func
    return

def nested(func): #XXX: or return dict of {__name__: obj} ?
    """get any functions inside of func (e.g. inner functions in a closure)

    NOTE: results may differ if the function has been executed or not.
    If len(nestedcode(func)) > len(nested(func)), try calling func().
    If possible, python builds code objects, but delays building functions
    until func() is called.
    """
    if PY3:
        att1 = '__code__'
        att0 = '__func__'
    else:
        att1 = 'func_code' # functions
        att0 = 'im_func'   # methods

    import gc
    funcs = []
    # get the code objects, and try to track down by referrence
    for co in nestedcode(func):
        # look for function objects that refer to the code object
        for obj in gc.get_referrers(co):
            # get methods
            _ = getattr(obj, att0, None) # ismethod
            if getattr(_, att1, None) is co: funcs.append(obj)
            # get functions
            elif getattr(obj, att1, None) is co: funcs.append(obj)
            # get frame objects
            elif getattr(obj, 'f_code', None) is co: funcs.append(obj)
            # get code objects
            elif hasattr(obj, 'co_code') and obj is co: funcs.append(obj)
#     frameobjs => func.func_code.co_varnames not in func.func_code.co_cellvars
#     funcobjs => func.func_code.co_cellvars not in func.func_code.co_varnames
#     frameobjs are not found, however funcobjs are...
#     (see: test_mixins.quad ... and test_mixins.wtf)
#     after execution, code objects get compiled, and them may be found by gc
    return funcs


def freevars(func):
    """get objects defined in enclosing code that are referred to by func

    returns a dict of {name:object}"""
    if PY3:
        im_func = '__func__'
        func_code = '__code__'
        func_closure = '__closure__'
    else:
        im_func = 'im_func'
        func_code = 'func_code'
        func_closure = 'func_closure'
    if ismethod(func): func = getattr(func, im_func)
    if isfunction(func):
        closures = getattr(func, func_closure) or ()
        func = getattr(func, func_code).co_freevars # get freevars
    else:
        return {}
    return dict((name,c.cell_contents) for (name,c) in zip(func,closures))

def globalvars(func):
    """get objects defined in global scope that are referred to by func

    return a dict of {name:object}"""
    if PY3:
        im_func = '__func__'
        func_code = '__code__'
        func_globals = '__globals__'
    else:
        im_func = 'im_func'
        func_code = 'func_code'
        func_globals = 'func_globals'
    if ismethod(func): func = getattr(func, im_func)
    if isfunction(func):
        globs = getattr(func, func_globals) or {}
        func = getattr(func, func_code).co_names # get names
    else:
        return {}
    #NOTE: if name not in func_globals, then we skip it...
    return dict((name,globs[name]) for name in func if name in globs)

def varnames(func):
    """get names of variables defined by func

    returns a tuple (local vars, local vars referrenced by nested functions)"""
    func = code(func)
    if not iscode(func):
        return () #XXX: better ((),())? or None?
    return func.co_varnames, func.co_cellvars


def baditems(obj, exact=False, safe=False): #XXX: obj=globals() ?
    """get items in object that fail to pickle"""
    if not hasattr(obj,'__iter__'): # is not iterable
        return [j for j in (badobjects(obj,0,exact,safe),) if j is not None]
    obj = obj.values() if getattr(obj,'values',None) else obj
    _obj = [] # can't use a set, as items may be unhashable
    [_obj.append(badobjects(i,0,exact,safe)) for i in obj if i not in _obj]
    return [j for j in _obj if j is not None]


def badobjects(obj, depth=0, exact=False, safe=False):
    """get objects that fail to pickle"""
    from dill import pickles
    if not depth:
        if pickles(obj,exact,safe): return None
        return obj
    return dict(((attr, badobjects(getattr(obj,attr),depth-1,exact,safe)) \
           for attr in dir(obj) if not pickles(getattr(obj,attr),exact,safe)))

def badtypes(obj, depth=0, exact=False, safe=False):
    """get types for objects that fail to pickle"""
    from dill import pickles
    if not depth:
        if pickles(obj,exact,safe): return None
        return type(obj)
    return dict(((attr, badtypes(getattr(obj,attr),depth-1,exact,safe)) \
           for attr in dir(obj) if not pickles(getattr(obj,attr),exact,safe)))

def errors(obj, depth=0, exact=False, safe=False):
    """get errors for objects that fail to pickle"""
    from dill import pickles, copy
    if not depth:
        try:
            pik = copy(obj)
            if exact:
                assert pik == obj, \
                    "Unpickling produces %s instead of %s" % (pik,obj)
            assert type(pik) == type(obj), \
                "Unpickling produces %s instead of %s" % (type(pik),type(obj))
            return None
        except Exception:
            import sys
            return sys.exc_info()[1]
    return dict(((attr, errors(getattr(obj,attr),depth-1,exact,safe)) \
           for attr in dir(obj) if not pickles(getattr(obj,attr),exact,safe)))

del absolute_import


# EOF
