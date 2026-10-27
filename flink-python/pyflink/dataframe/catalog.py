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

from typing import Dict, List, Optional

from pyflink.common import Configuration
from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.errors import _raise_as_value_error
from pyflink.table.catalog import Catalog, CatalogDescriptor
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = [
    "create_catalog",
    "get_catalog",
    "use_catalog",
    "get_current_catalog",
    "list_catalogs",
    "use_database",
    "get_current_database",
    "list_databases",
]


def _validate_name(name: str, what: str) -> None:
    if not isinstance(name, str):
        raise TypeError(f"{what} must be a string")
    if not name:
        raise ValueError(f"{what} must not be empty")


def _validate_catalog_options(options: Dict[str, str]) -> None:
    if not isinstance(options, dict):
        raise TypeError("options must be a dict of string keys and values")
    for key, value in options.items():
        if not isinstance(key, str):
            raise TypeError("option keys must be strings")
        if not key:
            raise ValueError("option keys must not be empty")
        if not isinstance(value, str):
            raise TypeError(f"option {key!r} must have a string value")


@PublicEvolving()
def create_catalog(name: str, options: Dict[str, str]) -> None:
    """
    Create a catalog in the environment used by DataFrame operations.

    The ``type`` option picks which kind of catalog to create, for example ``generic_in_memory``.
    Flink has to know about that type, either built in or on the classpath. Every other option is
    passed through to the catalog. This does the same as ``CREATE CATALOG`` in SQL.

    :param name: Name under which the catalog is created.
    :param options: Catalog options, including the ``type`` option.
    :raises TypeError: If ``name`` is not a string or ``options`` is not a dict of strings.
    :raises ValueError: If ``name`` or an option key is empty, or if Flink cannot create a catalog
        from ``options``.
    :raises ~pyflink.util.exceptions.CatalogException: If a catalog named ``name`` already exists,
        or if the catalog reports an error while being created.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        >>> pf.list_catalogs()
        ['default_catalog', 'my_catalog']

    .. versionadded:: 2.4.0
    """
    _validate_name(name, "name")
    _validate_catalog_options(options)
    configuration = Configuration()
    for key, value in options.items():
        configuration.set_string(key, value)
    descriptor = CatalogDescriptor.of(name, configuration)
    try:
        get_or_create_table_environment().create_catalog(name, descriptor)
    except Exception as error:
        _raise_as_value_error(error)


@PublicEvolving()
def get_catalog(name: str) -> Optional[Catalog]:
    """
    Return a registered catalog by name.

    :param name: Name of the catalog.
    :return: The :class:`~pyflink.table.catalog.Catalog`, or ``None`` if no catalog is registered
        under ``name``.
    :raises TypeError: If ``name`` is not a string.
    :raises ValueError: If ``name`` is empty.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        >>> pf.get_catalog("my_catalog").list_databases()
        ['default']
        >>> pf.get_catalog("missing") is None
        True

    .. versionadded:: 2.4.0
    """
    _validate_name(name, "name")
    return get_or_create_table_environment().get_catalog(name)


@PublicEvolving()
def use_catalog(name: str) -> None:
    """
    Set the current catalog.

    Table paths without a catalog part, such as ``my_table`` or ``my_database.my_table``, are
    resolved against the current catalog. Switching catalogs also moves you to that catalog's
    default database.

    :param name: Name of a registered catalog.
    :raises TypeError: If ``name`` is not a string.
    :raises ValueError: If ``name`` is empty.
    :raises ~pyflink.util.exceptions.CatalogException: If no catalog named ``name`` exists.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.create_catalog("my_catalog", {"type": "generic_in_memory"})
        >>> pf.use_catalog("my_catalog")
        >>> pf.get_current_catalog()
        'my_catalog'

    .. versionadded:: 2.4.0
    """
    _validate_name(name, "name")
    get_or_create_table_environment().use_catalog(name)


@PublicEvolving()
def get_current_catalog() -> Optional[str]:
    """
    Return the name of the current catalog.

    :return: The current catalog name, or ``None`` if no current catalog is set.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.get_current_catalog()
        'default_catalog'

    .. versionadded:: 2.4.0
    """
    return get_or_create_table_environment().get_current_catalog()


@PublicEvolving()
def list_catalogs() -> List[str]:
    """
    Return the names of all registered catalogs.

    :return: Catalog names.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.list_catalogs()
        ['default_catalog']

    .. versionadded:: 2.4.0
    """
    return get_or_create_table_environment().list_catalogs()


@PublicEvolving()
def use_database(name: str) -> None:
    """
    Set the current database within the current catalog.

    Table paths without a database part, such as ``my_table``, are resolved against the current
    database.

    :param name: Name of a database in the current catalog.
    :raises TypeError: If ``name`` is not a string.
    :raises ValueError: If ``name`` is empty.
    :raises ~pyflink.util.exceptions.CatalogException: If the current catalog has no database
        named ``name``.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.use_database("default_database")
        >>> pf.get_current_database()
        'default_database'

    .. versionadded:: 2.4.0
    """
    _validate_name(name, "name")
    get_or_create_table_environment().use_database(name)


@PublicEvolving()
def get_current_database() -> Optional[str]:
    """
    Return the name of the current database.

    :return: The current database name, or ``None`` if no current database is set.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.get_current_database()
        'default_database'

    .. versionadded:: 2.4.0
    """
    return get_or_create_table_environment().get_current_database()


@PublicEvolving()
def list_databases() -> List[str]:
    """
    Return the names of all databases in the current catalog.

    :return: Database names.
    :raises ValueError: If no current catalog is set.

    Example::

        >>> import pyflink.dataframe as pf
        >>> pf.list_databases()
        ['default_database']

    .. versionadded:: 2.4.0
    """
    table_environment = get_or_create_table_environment()
    # TableEnvironment.list_databases() fails with a bare NoSuchElementException in this case.
    if table_environment.get_current_catalog() is None:
        raise ValueError(
            "A current catalog has not been set. Set one with pf.use_catalog(), or list the "
            "databases of a specific catalog with pf.get_catalog(name).list_databases()."
        )
    return table_environment.list_databases()
