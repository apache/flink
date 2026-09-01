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
import abc
import enum
import inspect
import os
import subprocess
import sys
import unittest
import warnings

from pyflink.util.api_stability_decorators import (
    Deprecated,
    Experimental,
    Internal,
    Public,
    PublicEvolving,
)


def _catch_warnings():
    """
    Returns a context manager recording every warning raised within it.
    """
    context = warnings.catch_warnings(record=True)
    warnings.simplefilter("always")
    return context


class DeprecatedTests(unittest.TestCase):
    """
    Tests for the :class:`Deprecated` decorator, which must warn when a deprecated API is
    used, and not when it is defined.
    """

    def test_decoration_does_not_warn(self):
        with _catch_warnings() as caught:

            @Deprecated(since="1.0.0", detail="Use :func:`new_func` instead.")
            def func():
                pass

            @Deprecated(since="1.0.0")
            class Cls(object):
                def __init__(self):
                    pass

        self.assertEqual([], [str(warning.message) for warning in caught])

    def test_importing_pyflink_table_does_not_warn(self):
        # A regression test for the decorators warning at import time: every deprecated API of
        # pyflink.table used to warn as soon as the package was imported. This needs a fresh
        # interpreter, as pyflink.table is already imported in the one running the tests.
        script = (
            "import warnings\n"
            "with warnings.catch_warnings(record=True) as caught:\n"
            "    warnings.simplefilter('always')\n"
            "    import pyflink.table\n"
            "print([str(warning.message) for warning in caught\n"
            "       if 'has been deprecated since version' in str(warning.message)])\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        self.assertEqual(0, result.returncode, result.stderr.decode("utf-8"))
        self.assertEqual("[]", result.stdout.decode("utf-8").strip())

    def test_function_warns_when_called(self):
        @Deprecated(since="1.0.0", detail="Use :func:`new_func` instead.")
        def func(a, b=2):
            return a + b

        with _catch_warnings() as caught:
            self.assertEqual(3, func(1))

        self.assertEqual(1, len(caught))
        self.assertIs(DeprecationWarning, caught[0].category)
        self.assertEqual(
            f"{func.__qualname__} has been deprecated since version 1.0.0. "
            f"Use :func:`new_func` instead.",
            str(caught[0].message),
        )

    def test_function_without_detail_warns_when_called(self):
        @Deprecated(since="1.0.0")
        def func():
            pass

        with _catch_warnings() as caught:
            func()

        self.assertEqual(1, len(caught))
        self.assertEqual(
            f"{func.__qualname__} has been deprecated since version 1.0.0.",
            str(caught[0].message),
        )

    def test_function_wrapper_preserves_metadata(self):
        @Deprecated(since="1.0.0")
        def func():
            """Some documentation."""

        self.assertEqual("func", func.__name__)
        self.assertEqual(
            "DeprecatedTests.test_function_wrapper_preserves_metadata.<locals>.func",
            func.__qualname__,
        )
        self.assertIn("Some documentation.", func.__doc__)

    def test_class_warns_when_instantiated(self):
        @Deprecated(since="1.0.0", detail="Use :class:`NewClass` instead.")
        class Cls(object):
            def __init__(self, x):
                self.x = x

        with _catch_warnings() as caught:
            instance = Cls(1)

        self.assertEqual(1, instance.x)
        self.assertEqual(1, len(caught))
        self.assertIs(DeprecationWarning, caught[0].category)
        self.assertEqual(
            f"{Cls.__qualname__} has been deprecated since version 1.0.0. "
            f"Use :class:`NewClass` instead.",
            str(caught[0].message),
        )

    def test_class_without_own_init_warns_when_instantiated(self):
        @Deprecated(since="1.0.0")
        class Cls(object):
            pass

        with _catch_warnings() as caught:
            Cls()

        self.assertEqual(1, len(caught))
        self.assertEqual(
            f"{Cls.__qualname__} has been deprecated since version 1.0.0.",
            str(caught[0].message),
        )

        # The wrapper must not turn an unexpected argument into a silent no-op.
        with _catch_warnings():
            with self.assertRaises(TypeError):
                Cls(1)

    def test_class_is_returned_unchanged(self):
        @Deprecated(since="1.0.0")
        class Cls(object):
            def __init__(self):
                self.x = 1

        class Subclass(Cls):
            pass

        with _catch_warnings():
            instance = Subclass()

        self.assertIsInstance(instance, Cls)
        self.assertTrue(issubclass(Subclass, Cls))
        self.assertEqual("Cls", Cls.__name__)

    def test_subclass_of_deprecated_class_does_not_warn(self):
        # Deprecating a class says nothing about its subclasses, which are the ones users are
        # typically pointed at. This mirrors PEP 702.
        @Deprecated(since="1.0.0")
        class Cls(object):
            def __init__(self):
                pass

        class Subclass(Cls):
            pass

        with _catch_warnings() as caught:
            Subclass()

        self.assertEqual([], [str(warning.message) for warning in caught])

    def test_deprecated_subclass_inheriting_init_warns_once(self):
        @Deprecated(since="1.0.0")
        class Cls(object):
            def __init__(self):
                pass

        @Deprecated(since="2.0.0")
        class Subclass(Cls):
            pass

        with _catch_warnings() as caught:
            Subclass()

        self.assertEqual(
            [f"{Subclass.__qualname__} has been deprecated since version 2.0.0."],
            [str(warning.message) for warning in caught],
        )

    def test_function_warning_points_at_the_caller(self):
        @Deprecated(since="1.0.0")
        def func():
            pass

        with _catch_warnings() as caught:
            lineno = inspect.currentframe().f_lineno + 1
            func()

        self.assertEqual(1, len(caught))
        self.assertEqual(os.path.abspath(__file__), os.path.abspath(caught[0].filename))
        self.assertEqual(lineno, caught[0].lineno)

    def test_class_warning_points_at_the_caller(self):
        @Deprecated(since="1.0.0")
        class Cls(object):
            def __init__(self):
                pass

        with _catch_warnings() as caught:
            lineno = inspect.currentframe().f_lineno + 1
            Cls()

        self.assertEqual(1, len(caught))
        self.assertEqual(os.path.abspath(__file__), os.path.abspath(caught[0].filename))
        self.assertEqual(lineno, caught[0].lineno)

    def test_docstring_directives_are_still_applied(self):
        @Deprecated(since="1.0.0", detail="Use :func:`new_func` instead.")
        def func():
            """Function documentation."""

        @Deprecated(since="1.0.0")
        class Cls(object):
            """Class documentation."""

            def method(self):
                """Method documentation."""

        self.assertEqual(
            "Function documentation.\n.. deprecated:: 1.0.0\n   Use :func:`new_func` instead.",
            func.__doc__,
        )
        self.assertEqual("Class documentation.\n.. deprecated:: 1.0.0", Cls.__doc__)
        self.assertEqual("Method documentation.\n.. deprecated:: 1.0.0", Cls.method.__doc__)

    def test_stability_decorators_attribute_is_still_populated(self):
        @Deprecated(since="1.0.0")
        def func():
            pass

        @Deprecated(since="1.0.0")
        @PublicEvolving()
        class Cls(object):
            def __init__(self):
                pass

        self.assertEqual({Deprecated}, getattr(func, "__stability_decorators"))
        self.assertEqual({Deprecated, PublicEvolving}, getattr(Cls, "__stability_decorators"))

    def test_static_and_class_methods(self):
        with _catch_warnings() as caught_at_decoration:

            class Cls(object):
                @Deprecated(since="1.0.0")
                @staticmethod
                def static_method(x):
                    """Static method documentation."""
                    return x

                @Deprecated(since="1.0.0")
                @classmethod
                def class_method(cls, x):
                    return x

        self.assertEqual([], [str(warning.message) for warning in caught_at_decoration])
        self.assertIsInstance(Cls.__dict__["static_method"], staticmethod)
        self.assertIsInstance(Cls.__dict__["class_method"], classmethod)
        self.assertIn(".. deprecated:: 1.0.0", Cls.static_method.__doc__)

        with _catch_warnings() as caught:
            self.assertEqual(1, Cls.static_method(1))
            self.assertEqual(2, Cls.class_method(2))

        self.assertEqual(
            [
                f"{Cls.__qualname__}.static_method has been deprecated since version 1.0.0.",
                f"{Cls.__qualname__}.class_method has been deprecated since version 1.0.0.",
            ],
            [str(warning.message) for warning in caught],
        )

    def test_property(self):
        # A property cannot be wrapped without replacing the descriptor, so it only gets the
        # docstring directive. It must not raise.
        with _catch_warnings() as caught:

            class Cls(object):
                @Deprecated(since="1.0.0")
                @property
                def value(self):
                    """Property documentation."""
                    return 1

            self.assertEqual(1, Cls().value)

        self.assertEqual([], [str(warning.message) for warning in caught])
        self.assertIn(".. deprecated:: 1.0.0", Cls.__dict__["value"].__doc__)

    def test_abstract_class(self):
        with _catch_warnings() as caught_at_decoration:

            @Deprecated(since="1.0.0")
            class Abstract(abc.ABC):
                """Abstract class documentation."""

                @abc.abstractmethod
                def method(self):
                    pass

        self.assertEqual([], [str(warning.message) for warning in caught_at_decoration])
        self.assertIn(".. deprecated:: 1.0.0", Abstract.__doc__)

        with self.assertRaises(TypeError):
            Abstract()

    def test_enum_class(self):
        with _catch_warnings() as caught:

            @Deprecated(since="1.0.0")
            class Colour(enum.Enum):
                """Enum documentation."""

                RED = 1

            self.assertIs(Colour.RED, Colour(1))
            self.assertEqual(1, Colour.RED.value)

        self.assertEqual([], [str(warning.message) for warning in caught])
        self.assertIn(".. deprecated:: 1.0.0", Colour.__doc__)


class OtherStabilityDecoratorTests(unittest.TestCase):
    """
    Tests that the decorators other than :class:`Deprecated` document without warning.
    """

    def test_decorators_never_warn(self):
        for decorator in (Experimental, Internal, Public, PublicEvolving):
            with self.subTest(decorator=decorator.__name__):
                with _catch_warnings() as caught:

                    @decorator()
                    def func():
                        """Function documentation."""

                    @decorator()
                    class Cls(object):
                        """Class documentation."""

                        def method(self):
                            """Method documentation."""

                    func()
                    Cls().method()

                self.assertEqual([], [str(warning.message) for warning in caught])

    def test_decorated_elements_are_returned_unchanged(self):
        for decorator in (Experimental, Internal, Public, PublicEvolving):
            with self.subTest(decorator=decorator.__name__):

                def func():
                    pass

                class Cls(object):
                    pass

                self.assertIs(func, decorator()(func))
                self.assertIs(Cls, decorator()(Cls))

    def test_docstring_directives_and_attribute(self):
        @Public()
        class Cls(object):
            """Class documentation."""

            def method(self):
                """Method documentation."""

        self.assertIn("is marked as **public**", Cls.__doc__)
        self.assertIn("is marked as **public**", Cls.method.__doc__)
        self.assertEqual({Public}, getattr(Cls, "__stability_decorators"))


if __name__ == "__main__":
    unittest.main()
