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

from inspect import getmembers, isfunction, isclass
from typing import TypeVar, Callable, Any, Union, Type, Optional
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
            func_or_cls.__doc__ = f"{docstring}\n{directive}"

        # Add the decorator to an internal __stability_decorators set on the class/function
        # being decorated, for later introspection.
        if hasattr(func_or_cls, '__stability_decorators'):
            stability_decorators = getattr(func_or_cls, '__stability_decorators')
            stability_decorators.add(self.__class__)
        else:
            setattr(func_or_cls, '__stability_decorators', {self.__class__})

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
        return f".. deprecated:: {self.since}\n{indent(dedent(self.detail), '   ')}"

    @override
    def __call__(self, func_or_cls: T) -> T:
        """
        Emit a warning on the deprecation of the given function/class. Then call the base class
        for docstring modification.
        """
        msg = f"{func_or_cls.__qualname__} has been deprecated since version {self.since}."
        if self.detail is not None:
            msg = f"{msg} {self.detail}"

        warnings.warn(msg, category=DeprecationWarning, stacklevel=2)
        return super().__call__(func_or_cls)


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
