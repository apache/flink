################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import functools
from inspect import getmembers, isfunction, isclass
from typing import TypeVar, Callable, Any, Union, Type, Optional, cast
from abc import ABCMeta, abstractmethod
import warnings
from typing_extensions import override
from textwrap import dedent, indent

__all__ = ["Deprecated", "Experimental", "Internal", "PublicEvolving", "Public"]

# TypeVar for anything callable (function or class)
T = TypeVar("T", bound=Union[Callable[..., Any], Type[Any]])


class BaseAPIStabilityDecorator(metaclass=ABCMeta):
    """
    Base class for implementing API stability decorators.

    This abstract base class provides the foundation for creating decorators that
    mark API elements (functions or classes) with stability indicators. It handles
    the mechanics of applying documentation directives to both standalone functions
    and entire classes, including their public methods.
    """

    @abstractmethod
    def get_directive(self, func_or_cls: T) -> str:
        """
        Returns the Sphinx directive that should be appended to the docs of the function/class
        for the given decorator.
        """
        pass

    @staticmethod
    def _get_element_type_name(func_or_cls: T) -> str:
        """
        Returns a string representation of the API element's type.
        """
        if isfunction(func_or_cls):
            return "function"
        elif isclass(func_or_cls):
            return "class"
        else:
            return "API"

    def __call__(self, func_or_cls: T) -> T:
        """
        Appends a directive to the docstring of the given function or class.
        If a class, it also appends the directive to the docstrings of the public functions
        and properties of that class.
        """
        directive = dedent(self.get_directive(func_or_cls))

        docstring = func_or_cls.__doc__ or ""

        # Class/Function docstrings can be at an arbitrary level of indentation depending on the
        # depth. We should dedent the docstring here so that we can insert the directive at the
        # correct indentation.
        docstring = dedent(docstring)

        # Avoid duplicating directives if already present in the docstring.
        if directive not in docstring:
            try:
                func_or_cls.__doc__ = f"{docstring}\n{directive}"
            except (AttributeError, TypeError):
                pass

        # Add the decorator to an internal __stability_decorators set on the class/function
        # being decorated, for later introspection.
        if hasattr(func_or_cls, '__stability_decorators'):
            stability_decorators = getattr(func_or_cls, '__stability_decorators')
            stability_decorators.add(self.__class__)
        else:
            # Not every decorated object accepts attribute assignment (a property, for
            # example). Those simply cannot be introspected; that is not a reason to fail
            # at import time.
            try:
                setattr(func_or_cls, '__stability_decorators', {self.__class__})
            except (AttributeError, TypeError):
                pass

        if isclass(func_or_cls):
            for name, method in getmembers(
                func_or_cls,
                lambda member: isfunction(member) or isinstance(member, property)
            ):
                if not name.startswith("_"):
                    method_docstring = method.__doc__ or ""
                    method_docstring = dedent(method_docstring)

                    if directive not in method_docstring:
                        method.__doc__ = f"{method_docstring}\n{directive}"

        return func_or_cls


class Deprecated(BaseAPIStabilityDecorator):
    """
    Decorator to mark classes and functions as deprecated since a certain version, with an
    optional extra-details parameter.

    Example:

    .. code-block:: python

        @Deprecated(since="1.2.3", detail="Use :class:`MyNewClass` instead)
        class MyClass:

            @Deprecated(since="1.0.0")
            def func(self):
                pass

    :param str since: The version that this class/function was deprecated in.
    :param str detail: Optional explanatory detail for the deprecation.
    """

    def __init__(self, since: str, detail: Optional[str] = None):
        self.since = since
        self.detail = detail

    def get_directive(self, func_or_cls: T) -> str:
        directive = f".. deprecated:: {self.since}"
        if self.detail is not None:
            directive = f"{directive}\n{indent(dedent(self.detail), '   ')}"
        return directive

    def _get_message(self, func_or_cls: T) -> str:
        """
        Returns the warning message emitted when the deprecated API element is used.
        """
        name = getattr(func_or_cls, "__qualname__", None) or getattr(
            func_or_cls, "__name__", "This API"
        )
        msg = f"{name} has been deprecated since version {self.since}."
        if self.detail is not None:
            msg = f"{msg} {self.detail}"
        return msg

    @override
    def __call__(self, func_or_cls: T) -> T:
        """
        Arranges for a :class:`DeprecationWarning` to be emitted when the decorated API element
        is *used*, and calls the base class for docstring modification.

        The warning must not be emitted here: this method runs while the module defining the
        API is being imported, so warning here would warn every user that imports PyFlink,
        whether or not they use the deprecated API, and would never warn the ones who do.
        """
        # staticmethod/classmethod objects are not functions and do not proxy __qualname__ on
        # all supported Python versions. Decorate the function they wrap instead, and
        # re-package it so that the descriptor still behaves as one.
        if isinstance(func_or_cls, (staticmethod, classmethod)):
            return cast(T, type(func_or_cls)(self(func_or_cls.__func__)))

        func_or_cls = super().__call__(func_or_cls)

        if isclass(func_or_cls):
            self._deprecate_class(func_or_cls)
        elif isfunction(func_or_cls):
            return cast(T, self._deprecate_function(func_or_cls))
        # Anything else (a property, for instance) cannot be wrapped without changing what the
        # decorated name refers to, so the docstring directive is all we apply.
        return func_or_cls

    def _deprecate_function(self, func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Returns a wrapper around the given function that warns before delegating to it.
        """
        msg = self._get_message(func)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # stacklevel=2 attributes the warning to the caller of the deprecated function
            # rather than to this wrapper.
            warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        return wrapper

    def _deprecate_class(self, cls: Type[Any]) -> None:
        """
        Wraps the __init__ of the given class so that instantiating it warns.

        The class itself is returned unchanged by :func:`__call__`; replacing it with a wrapper
        would break isinstance checks and subclassing.
        """
        msg = self._get_message(cls)
        original_init = cls.__init__

        @functools.wraps(original_init)
        def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
            # As in PEP 702, only instantiating the deprecated class itself warns. A subclass
            # is not necessarily deprecated, and this also avoids warning twice when a
            # deprecated class inherits the __init__ of a deprecated base class.
            if type(self) is cls:
                warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
            if original_init is object.__init__ and (args or kwargs) \
                    and type(self).__new__ is object.__new__:
                # object.__new__ rejects excess arguments only for classes that define
                # neither __new__ nor __init__; installing this __init__ would otherwise
                # silence that error.
                raise TypeError(f"{type(self).__name__}() takes no arguments")
            original_init(self, *args, **kwargs)

        try:
            cls.__init__ = __init__  # type: ignore[misc]
        except (AttributeError, TypeError):
            # Extension types and the like do not allow their __init__ to be replaced; fall
            # back to documenting the deprecation only.
            pass


class Experimental(BaseAPIStabilityDecorator):
    """
    Decorator to mark classes for experimental use.

    Classes with this annotation are neither battle-tested nor stable, and may be changed or
    removed in future versions.

    Example:

    .. code-block:: python

        @Experimental()
        class MyClass:

            @Experimental()
            def func(self):
                pass

    """

    def get_directive(self, func_or_cls: T) -> str:
        return f"""
.. warning:: This *{self._get_element_type_name(func_or_cls)}* is marked as **experimental**. It
             is neither battle-tested nor stable, and may be changed or removed in future
             versions.
        """


class Internal(BaseAPIStabilityDecorator):
    """
    Decorator to mark functions within stable, public APIs as an internal developer API.

    Developer APIs are stable but internal to Flink and might change across releases.

    Example:

    .. code-block:: python

        @Internal()
        class MyClass:

            @Internal()
            def func(self):
                pass

    """

    def get_directive(self, func_or_cls: T) -> str:
        return f"""
.. caution:: This *{self._get_element_type_name(func_or_cls)}* is marked as **internal**.
             It as an internal developer API, which are stable but internal to Flink and
             might change across versions.
        """


class PublicEvolving(BaseAPIStabilityDecorator):
    """
    Decorator to mark classes and functions for public use, but with evolving interfaces.

    Classes and functions with this decorator are intended for public use and have stable behaviour.
    However, their interfaces and signatures are not considered to be stable and might be changed
    across versions.

    Example:

    .. code-block:: python

        @PublicEvolving()
        class MyClass:

            @PublicEvolving()
            def func(self):
                pass

    """

    def get_directive(self, func_or_cls: T) -> str:
        return f"""
.. note:: This *{self._get_element_type_name(func_or_cls)}* is marked as **evolving**. It is
          intended for public use and has stable behaviour. However, its interface/signature is
          not considered to be stable and might be changed across versions.
        """


class Public(BaseAPIStabilityDecorator):
    """
    Decorator to mark classes and functions for as public, stable interfaces.

    Classes and functions with this decorator are stable across minor releases (2.0, 2.1, 2.2, etc).
    Only major releases (1.0, 2.0, 3.0, etc) can break interfaces with this annotation.

    Example:

    .. code-block:: python

        @Public()
        class MyClass:

            @Public()
            def func(self):
                pass

    """

    def get_directive(self, func_or_cls: T) -> str:
        return f"""
.. note:: This *{self._get_element_type_name(func_or_cls)}* is marked as **public**. It is
          intended for public use is stable across minor version releases.
        """
