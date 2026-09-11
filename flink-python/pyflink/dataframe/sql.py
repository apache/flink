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
from typing import Any, Dict, Iterable, List, Set, Union

from py4j.protocol import Py4JJavaError

from pyflink.dataframe.context import get_or_create_table_environment
from pyflink.dataframe.dataframe import DataFrame
from pyflink.dataframe.udf import _DataFrameUDFWrapper
from pyflink.java_gateway import get_gateway
from pyflink.table import Table, TableEnvironment
from pyflink.util.api_stability_decorators import PublicEvolving
from pyflink.util.java_utils import is_instance_of

__all__ = ["sql"]

_LOG = logging.getLogger(__name__)

_Binding = Union[DataFrame, _DataFrameUDFWrapper]
# TODO: This is only needed for python<=3.9. Once we drop support for it
#       we can check with isinstance(<obj>, _Binding) instead of having to do this.
_BINDABLE_TYPES = (DataFrame, _DataFrameUDFWrapper)


@PublicEvolving()
def sql(query: str, *, auto_bind: bool = True, **bindings: _Binding) -> DataFrame:
    """
    Execute a SQL query and return the result as a :class:`DataFrame`.

    The query must be a single statement that returns a result, such as SELECT or
    VALUES (no INSERT / DDL; use :meth:`TableEnvironment.execute_sql` for those).
    The referenced DataFrames are registered as temporary views and the referenced
    UDFs (created with :func:`~pyflink.dataframe.udf`) as temporary system functions
    for the duration of the call, and both are dropped afterwards. The result can be
    further transformed with the DataFrame API.

    When ``auto_bind`` is ``True`` (the default), the caller's local and global variables
    are scanned for :class:`DataFrame` and UDF objects and each is registered under its
    Python variable name. Auto-binding is best-effort: it warns and skips names that
    are not valid SQL identifiers or that collide with an existing table, view or
    function, and it never shadows permanent catalog objects or built-in functions.

    Explicit keyword ``bindings`` define the SQL names directly. They are strict
    (invalid names and conflicts with existing temporary views or temporary functions
    raise :class:`ValueError`), take precedence over auto-bind on name collisions, and
    are required to intentionally shadow a permanent catalog table, view or function
    or a built-in function.

    The query runs in the :class:`TableEnvironment` of the bound DataFrames: the
    environment shared by the explicit ``bindings`` when given, otherwise the
    environment shared by all valid auto-bound candidates. Explicit bindings from
    different environments raise :class:`ValueError`. Without explicit bindings,
    valid auto-bound candidates from different environments also raise
    :class:`ValueError`; when explicit bindings determine the environment, auto-bound
    candidates from other environments are skipped with a warning. UDF bindings are
    not tied to an environment and do not take part in this resolution. The resolved
    environment is used only for this call and never replaces the global one.

    Function names are case-insensitive in SQL, so a UDF bound as ``addOne`` is
    referenced as ``addOne(...)`` or ``addone(...)`` alike.

    :param query: The query to execute.
    :param auto_bind: Whether to scan the caller's variables for DataFrames and UDFs.
    :param bindings: Explicit name to :class:`DataFrame` or UDF bindings.
    :return: The query result.
    :raises ValueError: If the query is not a query statement, if an explicit binding
                        is not a valid SQL identifier or conflicts with an existing
                        temporary view or temporary function, or if explicit
                        bindings belong to different TableEnvironments, or if
                        auto-bound candidates belong to different TableEnvironments
                        when there are no explicit bindings.
    :raises TypeError: If an explicit binding is neither a :class:`DataFrame` nor a
                       UDF created with :func:`~pyflink.dataframe.udf`.

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
        >>> # UDFs are bound the same way, under their variable or keyword name
        >>> @pf.udf
        ... def add_one(value: int) -> int:
        ...     return value + 1
        >>> pf.sql("SELECT add_one(a) AS a1 FROM df1")
        >>> pf.sql("SELECT inc(a) FROM src", auto_bind=False, src=df1, inc=add_one)
        >>> # Mix SQL and the DataFrame API
        >>> pf.sql("SELECT a, b FROM df1").filter(pf.col("a") > 1).to_pandas()

    .. versionadded:: 2.4.0
    """
    if not isinstance(query, str):
        raise TypeError("query must be a string")

    # Collect auto bindings first.
    variables = {}
    # Gather the variables in the namespace
    if auto_bind:
        if frame := inspect.currentframe():
            if outer_frame := frame.f_back:
                variables = {**outer_frame.f_globals, **outer_frame.f_locals}
            # Suggested by python docs
            del outer_frame
            del frame
    auto_frames = _get_dataframes(variables)
    auto_udfs = _get_udfs(variables)

    # Check that explicit bindings are the correct type first
    for name, value in bindings.items():
        if not isinstance(value, _BINDABLE_TYPES):
            raise TypeError(
                f"sql() binding '{name}' must be a DataFrame or a UDF created with "
                f"pyflink.dataframe.udf, got {type(value).__name__}"
            )
    explicit_frames = _get_dataframes(bindings)
    explicit_udfs = _get_udfs(bindings)

    t_env = _resolve_table_environment(explicit_frames, auto_frames)
    # Each registration step is all-or-nothing: it rolls back its own partial work on
    # failure and only returns names on success, so a step that raises leaves nothing
    # of its own behind and the finally block only drops what earlier steps returned.
    views: List[str] = []
    functions: List[str] = []
    try:
        views = _register_views(t_env, explicit_frames, auto_frames)
        functions = _register_functions(t_env, explicit_udfs, auto_udfs)
        return DataFrame(_execute_query(t_env, query))
    finally:
        _drop_functions(t_env, functions)
        _drop_views(t_env, views)


def _get_dataframes(namespace: Dict[str, Any]) -> Dict[str, DataFrame]:
    return {k: v for k, v in namespace.items() if isinstance(v, DataFrame)}


def _get_udfs(namespace: Dict[str, Any]) -> Dict[str, _DataFrameUDFWrapper]:
    return {k: v for k, v in namespace.items() if isinstance(v, _DataFrameUDFWrapper)}


def _drop_views(t_env: TableEnvironment, names: List[str]) -> None:
    for name in names:
        try:
            t_env.drop_temporary_view(name)
        except Exception:
            _LOG.warning("sql() failed to drop view '%s'", name, exc_info=True)


def _drop_functions(t_env: TableEnvironment, names: List[str]) -> None:
    for name in names:
        try:
            t_env.drop_temporary_system_function(name)
        except Exception:
            _LOG.warning("sql() failed to drop function '%s'", name, exc_info=True)


def _resolve_table_environment(
    explicit: Dict[str, DataFrame], auto: Dict[str, DataFrame]
) -> TableEnvironment:
    """
    Pick the environment to run the query in: the environment shared by the explicit
    bindings when given, otherwise the environment shared by all auto-bound
    candidates. Explicit bindings from different environments are an error the caller
    must resolve. Auto-bound candidates with invalid identifiers do not participate in
    environment resolution. Mixed valid auto-bound candidates are also ambiguous and
    require explicit bindings, regardless of the configured global environment.
    """
    explicit_envs = _distinct_environments(explicit.values())
    if len(explicit_envs) > 1:
        raise ValueError(
            "sql() explicit bindings belong to different TableEnvironments; "
            "bind DataFrames from a single environment"
        )
    if explicit_envs:
        return explicit_envs[0]

    auto_envs = _distinct_environments(
        value
        for name, value in auto.items()
        if _is_simple_sql_identifier(value._table._t_env, name)
    )
    if len(auto_envs) > 1:
        raise ValueError(
            "sql() auto-bound DataFrames belong to different TableEnvironments; "
            "set auto_bind=False and pass explicit bindings from a single "
            "TableEnvironment"
        )
    if auto_envs:
        return auto_envs[0]
    return get_or_create_table_environment()


def _distinct_environments(frames: Iterable[DataFrame]) -> List[TableEnvironment]:
    # Deduplicate by identity: environments are not comparable by value.
    return list({id(f._table._t_env): f._table._t_env for f in frames}.values())


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
    i.e. whether registering a temporary view or function under it can succeed. This is
    the same validation :meth:`TableEnvironment.create_temporary_view` applies, so
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


def _register_views(
    t_env: TableEnvironment,
    explicit: Dict[str, DataFrame],
    auto: Dict[str, DataFrame],
) -> List[str]:
    """
    Register explicit and auto-collected DataFrames as temporary views and return the
    registered names. Registration is all-or-nothing: if anything raises, the views
    registered so far are dropped again before re-raising. The explicit bindings have
    already been type-checked and share ``t_env`` (see
    :func:`_resolve_table_environment`).
    """
    temporary_tables = set(t_env.list_temporary_tables())
    # list_tables() covers both permanent and temporary tables and views.
    all_tables = set(t_env.list_tables())
    registered: List[str] = []

    try:
        for name, value in explicit.items():
            if not _is_simple_sql_identifier(t_env, name):
                raise ValueError(
                    f"cannot bind '{name}': it is not a valid SQL identifier"
                )
            if name in temporary_tables:
                raise ValueError(
                    f"cannot bind '{name}': a temporary table or view with this name "
                    "already exists"
                )
            t_env.create_temporary_view(name, value.to_table())
            registered.append(name)

        # Auto-bound candidates are not expected to raise: problems are reported as
        # warnings. Anything unexpected still rolls back the registered views.
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
    except Exception:
        _drop_views(t_env, registered)
        raise
    return registered


def _register_functions(
    t_env: TableEnvironment,
    explicit: Dict[str, _DataFrameUDFWrapper],
    auto: Dict[str, _DataFrameUDFWrapper],
) -> List[str]:
    """
    Register explicit and auto-collected UDFs as temporary system functions and return
    the registered names. Registration is all-or-nothing: if anything raises, the
    functions registered so far are dropped again before re-raising.

    System functions are looked up by bare name independently of the current catalog
    and database, which matches how a Python name is referenced in the query. Function
    names are case-insensitive: the catalog normalizes them to lower case, so
    collisions are checked case-insensitively. Mirroring views, explicit bindings may
    shadow built-in and permanent catalog functions but never an existing temporary
    function; auto-bind shadows nothing.
    """
    if not explicit and not auto:
        return []
    # Explicit bindings take precedence on name collisions, so only the remaining
    # auto-bound candidates need the built-in function names. list_functions() covers
    # those as well, but is comparatively expensive, so skip it when nothing needs it.
    explicit_names = {name.lower() for name in explicit}
    auto_candidates = {
        name: value for name, value in auto.items() if name.lower() not in explicit_names
    }
    all_functions: Set[str] = set()
    if auto_candidates:
        all_functions = {f.lower() for f in t_env.list_functions()}
    registered: List[str] = []

    try:
        for name, value in explicit.items():
            if not _is_simple_sql_identifier(t_env, name):
                raise ValueError(
                    f"cannot bind '{name}': it is not a valid SQL identifier"
                )
            if _has_temporary_function(t_env, name):
                raise ValueError(
                    f"cannot bind '{name}': a temporary function with this name "
                    "already exists"
                )
            t_env.create_temporary_system_function(name, value._table_udf_wrapper)
            registered.append(name)

        # Auto-bound candidates are not expected to raise: problems are reported as
        # warnings. Anything unexpected still rolls back the registered functions.
        for name, value in auto_candidates.items():
            if not _is_simple_sql_identifier(t_env, name):
                _warn_skipped(name, "it is not a valid SQL identifier")
                continue
            if name.lower() in all_functions or _has_temporary_function(t_env, name):
                _warn_skipped(name, "a function with this name already exists")
                continue
            try:
                t_env.create_temporary_system_function(name, value._table_udf_wrapper)
            except Exception as e:
                _warn_skipped(name, f"registration failed: {e}")
                continue
            registered.append(name)
    except Exception:
        _drop_functions(t_env, registered)
        raise
    return registered


def _has_temporary_function(t_env: TableEnvironment, name: str) -> bool:
    """
    Whether a temporary system function or a temporary catalog function in the current
    catalog and database is registered under ``name``. The environment's function
    listings merge temporary and permanent functions, so a temporary function that
    shares its name with a permanent one cannot be told apart from them; the function
    catalog keeps them separate.
    """
    function_catalog = t_env._j_tenv.getPlanner().getFlinkContext().getFunctionCatalog()
    if function_catalog.hasTemporarySystemFunction(name.lower()):
        return True
    gateway = get_gateway()
    identifier = gateway.jvm.org.apache.flink.table.catalog.ObjectIdentifier.of(
        t_env.get_current_catalog(), t_env.get_current_database(), name
    )
    return function_catalog.hasTemporaryCatalogFunction(identifier)


def _warn_skipped(name: str, reason: str) -> None:
    warnings.warn(
        f"sql() auto-bind skipped '{name}': {reason}.",
        UserWarning,
    )
