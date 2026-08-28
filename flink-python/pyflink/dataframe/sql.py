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

import inspect
import logging
import warnings
from typing import Any, Dict, List

from py4j.protocol import Py4JJavaError

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.table import Table, TableEnvironment
from pyflink.util.api_stability_decorators import PublicEvolving
from pyflink.util.java_utils import is_instance_of

__all__ = ["sql"]

_LOG = logging.getLogger(__name__)


@PublicEvolving()
def sql(query: str, *, auto_bind: bool = True, **bindings: DataFrame) -> DataFrame:
    """
    Execute a SQL query and return the result as a :class:`DataFrame`.

    The query must be a single statement that returns a result, such as SELECT or
    VALUES (no INSERT / DDL; use :meth:`TableEnvironment.execute_sql` for those).
    The referenced DataFrames are registered as temporary views for the duration of
    the call and dropped afterwards. The result can be further transformed with the
    DataFrame API.

    When ``auto_bind`` is ``True`` (the default), the caller's local and global variables
    are scanned for :class:`DataFrame` objects and each is registered under its Python
    variable name. Auto-binding is best-effort: it warns and skips names that are not
    valid SQL identifiers or that collide with an existing table or view, and it never
    shadows permanent catalog objects.

    Explicit keyword ``bindings`` define the SQL names directly. They are strict
    (invalid names and conflicts with existing temporary views raise
    :class:`ValueError`), take precedence over auto-bind on name collisions, and are
    required to intentionally shadow a permanent catalog table or view.

    The query runs in the :class:`TableEnvironment` of the bound DataFrames: the
    environment shared by the explicit ``bindings`` when given, otherwise the
    environment shared by all valid auto-bound candidates. Explicit bindings from
    different environments raise :class:`ValueError`. Without explicit bindings,
    valid auto-bound candidates from different environments also raise
    :class:`ValueError`; when explicit bindings determine the environment, auto-bound
    candidates from other environments are skipped with a warning. The resolved
    environment is used only for this call and never replaces the global one.

    :param query: The query to execute.
    :param auto_bind: Whether to scan the caller's variables for DataFrames.
    :param bindings: Explicit name to :class:`DataFrame` bindings.
    :return: The query result.
    :raises ValueError: If the query is not a query statement, if an explicit binding
                        is not a valid SQL identifier or conflicts with an existing
                        temporary view, or if explicit bindings belong to different
                        TableEnvironments, or if auto-bound candidates belong to
                        different TableEnvironments when there are no explicit
                        bindings.
    :raises TypeError: If an explicit binding is not a :class:`DataFrame`.

    Example::

        >>> import pyflink.dataframe as pf
        >>> df1 = pf.from_dict({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        >>> df2 = pf.from_dict({"a": [1, 2, 3], "c": ["p", "q", "r"]})
        >>> # Auto-bind: df1 / df2 are registered under their variable names
        >>> joined = pf.sql("SELECT df1.a, b, c FROM df1 JOIN df2 ON df1.a = df2.a")
        >>> # Explicit bindings: pick the SQL names, turn off scanning
        >>> result = pf.sql(
        ...     "SELECT * FROM src WHERE a > 1",
        ...     auto_bind=False,
        ...     src=df1,
        ... )
        >>> # Mix SQL and the DataFrame API
        >>> pf.sql("SELECT a, b FROM df1").filter(pf.col("a") > 1).to_pandas()

    .. versionadded:: 2.4.0
    """
    if not isinstance(query, str):
        raise TypeError("query must be a string")
    auto_bindings: Dict[str, DataFrame] = {}
    if auto_bind:
        frame = inspect.currentframe()
        caller = frame.f_back if frame is not None else None
        try:
            if caller is not None:
                # Locals take precedence over globals.
                namespace = {**caller.f_globals, **caller.f_locals}
                auto_bindings = {
                    name: value
                    for name, value in namespace.items()
                    if isinstance(value, DataFrame)
                }
        finally:
            del frame, caller
    t_env = _resolve_table_environment(bindings, auto_bindings)
    registered: List[str] = []
    try:
        _register_bindings(t_env, bindings, auto_bindings, registered)
        return DataFrame(_execute_query(t_env, query))
    finally:
        for name in registered:
            # Best-effort cleanup: dropping must not mask an exception raised by the
            # query itself, but only names registered by this call are dropped, so a
            # failure is an anomaly the user should hear about.
            try:
                t_env.drop_temporary_view(name)
            except Exception:
                _LOG.warning(
                    "sql() failed to drop temporary view '%s'", name, exc_info=True
                )


def _resolve_table_environment(
    explicit: Dict[str, Any], auto: Dict[str, DataFrame]
) -> TableEnvironment:
    """
    Pick the environment to run the query in: the environment shared by the explicit
    bindings when given, otherwise the environment shared by all auto-bound
    candidates. Explicit bindings from different environments are an error the caller
    must resolve. Auto-bound candidates with invalid identifiers do not participate in
    environment resolution. Mixed valid auto-bound candidates are also ambiguous and
    require explicit bindings, regardless of the configured global environment.
    """
    for name, value in explicit.items():
        if not isinstance(value, DataFrame):
            raise TypeError(
                f"sql() binding '{name}' must be a DataFrame, got {type(value).__name__}"
            )
    # Deduplicate by identity: environments are not comparable by value.
    explicit_envs = {id(v._table._t_env): v._table._t_env for v in explicit.values()}
    if len(explicit_envs) > 1:
        raise ValueError(
            "sql() explicit bindings belong to different TableEnvironments; "
            "bind DataFrames from a single environment"
        )
    if explicit_envs:
        return next(iter(explicit_envs.values()))
    auto_envs = {
        id(value._table._t_env): value._table._t_env
        for name, value in auto.items()
        if _is_simple_sql_identifier(value._table._t_env, name)
    }
    if len(auto_envs) == 1:
        return next(iter(auto_envs.values()))
    if len(auto_envs) > 1:
        raise ValueError(
            "sql() auto-bound DataFrames belong to different TableEnvironments; "
            "set auto_bind=False and pass explicit bindings from a single "
            "TableEnvironment"
        )
    return get_or_create_table_environment()


def _execute_query(t_env: TableEnvironment, query: str) -> Table:
    """
    Run ``query`` through :meth:`TableEnvironment.sql_query`, which parses the statement
    and rejects anything that is not a single query returning a result. Translate that
    rejection into a plain :class:`ValueError`.
    """
    try:
        return t_env.sql_query(query)
    except Py4JJavaError as e:
        if "Unsupported SQL query!" in str(e.java_exception):
            raise ValueError(
                "sql() only supports queries that return a result, such as SELECT "
                "or VALUES (no INSERT / DDL); use TableEnvironment.execute_sql() "
                "for other statements."
            ) from e
        raise


def _is_simple_sql_identifier(t_env: TableEnvironment, name: str) -> bool:
    """
    Whether ``name`` is accepted verbatim as a single-part identifier by the SQL parser,
    i.e. whether registering a temporary view under it can succeed. This is the same
    validation :meth:`TableEnvironment.create_temporary_view` applies to its path, so
    keywords like ``order`` pass (queries reference them with backticks) while names
    that would need quoting or resolve to a different or multi-part path do not.
    """
    try:
        identifier = t_env._j_tenv.getParser().parseIdentifier(name)
    except Py4JJavaError as e:
        if not is_instance_of(
            e.java_exception, "org.apache.flink.table.api.SqlParserException"
        ):
            raise
        return False
    return (
        not identifier.getCatalogName().isPresent()
        and not identifier.getDatabaseName().isPresent()
        and identifier.getObjectName() == name
    )


def _register_bindings(
    t_env: TableEnvironment,
    explicit: Dict[str, DataFrame],
    auto: Dict[str, DataFrame],
    registered: List[str],
) -> None:
    """
    Register explicit and auto-collected bindings as temporary views, appending each
    successful registration to ``registered``. The explicit bindings have already been
    type-checked and share ``t_env`` (see :func:`_resolve_table_environment`).
    """
    temporary_tables = set(t_env.list_temporary_tables())
    # list_tables() covers both permanent and temporary tables and views.
    all_tables = set(t_env.list_tables())

    for name, value in explicit.items():
        if not _is_simple_sql_identifier(t_env, name):
            raise ValueError(f"cannot bind '{name}': it is not a valid SQL identifier")
        if name in temporary_tables:
            raise ValueError(
                f"cannot bind '{name}': a temporary table or view with this name "
                "already exists"
            )
        t_env.create_temporary_view(name, value.to_table())
        registered.append(name)

    for name, value in auto.items():
        if name in explicit:
            # Explicit bindings take precedence on name collisions.
            continue
        if not _is_simple_sql_identifier(t_env, name):
            _warn_skipped(name, "it is not a valid SQL identifier")
            continue
        if value._table._t_env is not t_env:
            _warn_skipped(name, "it belongs to a different TableEnvironment")
            continue
        if name in all_tables:
            _warn_skipped(name, "a table or view with this name already exists")
            continue
        try:
            t_env.create_temporary_view(name, value.to_table())
        except Exception as e:
            _warn_skipped(name, f"registration failed: {e}")
            continue
        registered.append(name)


def _warn_skipped(name: str, reason: str) -> None:
    warnings.warn(
        f"sql() auto-bind skipped '{name}': {reason}.",
        UserWarning,
    )
