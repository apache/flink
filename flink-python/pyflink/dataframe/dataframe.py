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

import datetime
import keyword
from contextlib import ExitStack
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

if TYPE_CHECKING:
    import pandas
    import pyarrow
    from pyflink.dataframe.udf import _DataTypeLike
    from pyflink.dataframe.udtf import _DataFrameUDTFWrapper
    from pyflink.table.table_environment import TableEnvironment
    from pyflink.table.table_schema import TableSchema

from pyflink.common import Row
from pyflink.dataframe.datatype import _INT_MAX, DataType
from pyflink.dataframe.iteration import (
    _BATCH_FORMATS,
    CloseableIterator,
    _build_arrow_schema,
    _iterate_batches,
    _iterate_rows,
    _row_to_dict,
    _rows_to_batch,
    _take_rows,
    _validate_row_kind_field,
)
from pyflink.dataframe.validation import _require_choice, _require_int, _require_number
from pyflink.java_gateway import get_gateway
from pyflink.table.expression import Expression, _get_java_expression
from pyflink.table.expressions import (
    and_,
    call_sql,
    col as table_col,
    if_then_else,
    is_nan,
    lit as table_lit,
)
from pyflink.table.table import Table
from pyflink.table.statement_set import StatementSet
from pyflink.table.types import ArrayType, MapType, MultisetType, RowType
from pyflink.table.table_descriptor import TableDescriptor
from pyflink.util.api_stability_decorators import PublicEvolving
from pyflink.util.java_utils import to_jarray

__all__ = ["DataFrame", "GroupedDataFrame", "col", "lit"]

T = TypeVar("T")


def _validate_row_count(n: int) -> None:
    _require_int(n, "n", 0)
    if n > _INT_MAX:
        raise ValueError(f"n must be less than or equal to {_INT_MAX}")


@PublicEvolving()
def col(name: str) -> Expression:
    """
    Create a column reference expression.

    :param name: Name of the referenced column.
    :return: An expression referencing the column.

    Example::

        >>> import pyflink.dataframe as pf
        >>> df = pf.from_records([{"id": 1, "name": "Alice"}])
        >>> result = df.select(pf.col("name"))

    .. versionadded:: 2.4.0
    """
    return table_col(name)


@PublicEvolving()
def lit(value: Any, data_type: Optional[DataType] = None) -> Expression:
    """
    Create a literal expression.

    The data type is inferred from ``value`` when ``data_type`` is omitted. Otherwise, the
    declared data type is applied during literal construction.

    :param value: Literal value.
    :param data_type: Optional data type for the literal.
    :return: A literal expression.
    :raises TypeError: If ``data_type`` is not a :class:`DataType`.

    Example::

        >>> import pyflink.dataframe as pf
        >>> df = pf.from_records([{"id": 1}])
        >>> result = df.select("id", status=pf.lit("active"))

    .. versionadded:: 2.4.0
    """
    if data_type is None:
        return table_lit(value)
    if not isinstance(data_type, DataType):
        raise TypeError("data_type must be a pyflink.dataframe.DataType")
    table_data_type = data_type._to_table_data_type()
    if value is None:
        return table_lit(value, table_data_type)
    return table_lit(value, table_data_type.not_null())


@PublicEvolving()
class DataFrame:
    """
    A modern DataFrame API for PyFlink.

    DataFrame provides a Pythonic interface for data transformations. It supports fluent chaining
    of operations and provides a familiar DataFrame-style API.

    Example::

        >>> import pyflink.dataframe as pf
        >>> df = pf.from_dict({"id": [1, 2], "name": ["a", "b"]})
        >>> result = df.select("id", "name") \\
        ...              .with_column("id_doubled", pf.col("id") * 2) \\
        ...              .filter(pf.col("id") > 0)

    .. versionadded:: 2.4.0
    """

    def __init__(self, table: Table):
        self._table = table

    # ======================== Core Operations ========================

    @PublicEvolving()
    def filter(
        self,
        *predicates: Union[
            Expression, str, Callable[["DataFrame"], Expression]
        ],
        **constraints: Any,
    ) -> "DataFrame":
        """
        Keep rows that satisfy every predicate and equality constraint.

        Predicates may be boolean expressions, SQL expression strings, or callables that receive
        this DataFrame and return a boolean expression. A constraint value of ``None`` selects
        rows where the corresponding column is null.

        :param predicates: Conditions used to test each row.
        :param constraints: Values keyed by the column names that must equal them.
        :return: A new filtered DataFrame.
        :raises TypeError: If a predicate has an unsupported type or callable result.
        :raises ValueError: If no predicates or constraints are provided.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     {"name": "Alice", "age": 30, "status": "active"},
            ...     {"name": "Bob", "age": 17, "status": "active"},
            ... ])
            >>> adults = df.filter(pf.col("age") >= 18, status="active")
            >>> adults = df.filter(lambda current: current["age"] >= 18)
            >>> missing_status = df.filter(status=None)

        .. versionadded:: 2.4.0
        """
        if not predicates and not constraints:
            raise ValueError(
                "filter() requires at least one predicate or equality constraint"
            )

        conditions: List[Expression] = []
        for predicate in predicates:
            if isinstance(predicate, str):
                conditions.append(call_sql(predicate))
            elif isinstance(predicate, Expression):
                conditions.append(predicate)
            elif callable(predicate) and not isinstance(predicate, type):
                condition = predicate(self)
                if not isinstance(condition, Expression):
                    raise TypeError(
                        "filter() callable predicates must return an Expression"
                    )
                conditions.append(condition)
            else:
                raise TypeError(
                    "predicate must be an Expression, SQL string, or callable"
                )
        for name, value in constraints.items():
            column = table_col(name)
            conditions.append(column.is_null if value is None else column == table_lit(value))

        condition = conditions[0] if len(conditions) == 1 else and_(*conditions)
        return DataFrame(self._table.filter(condition))

    where = filter

    @PublicEvolving()
    def with_column(
        self,
        name: str,
        expr: Union[Expression, Callable[["DataFrame"], Expression]],
    ) -> "DataFrame":
        """
        Add a column, or replace an existing column with the same name.

        ``expr`` may be an expression or a callable that receives this DataFrame and returns an
        expression.

        :param name: Name of the added or replaced column.
        :param expr: Expression or callable used to compute the column value.
        :return: A new DataFrame with the requested column.
        :raises TypeError: If ``name`` is not a string or ``expr`` does not produce an expression.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"left": 1, "right": 2}])

            >>> with_expression = df.with_column(
            ...     "total", lambda current: current["left"] + current["right"]
            ... )

            >>> @pf.udf
            ... def add(left: int, right: int) -> int:
            ...     return left + right

            >>> with_udf = df.with_column(
            ...     "total", add(pf.col("left"), pf.col("right"))
            ... )

        .. versionadded:: 2.4.0
        """
        if not isinstance(name, str):
            raise TypeError("name must be a string")
        if isinstance(expr, Expression):
            expression = expr
        elif callable(expr) and not isinstance(expr, type):
            expression = expr(self)
        else:
            raise TypeError("expr must be an Expression")
        if not isinstance(expression, Expression):
            raise TypeError("expr must be an Expression")
        return DataFrame(self._table.add_or_replace_columns(expression.alias(name)))

    @PublicEvolving()
    def with_columns(
        self,
        *exprs: Expression,
        **named_exprs: Expression,
    ) -> "DataFrame":
        """
        Add or replace multiple columns in one call.

        Positional expressions are applied first and must carry their desired output names. Named
        expressions are appended afterward and are aliased to their keyword names.

        :param exprs: Expressions to add or replace.
        :param named_exprs: Expressions keyed by their output column names.
        :return: A new DataFrame with the requested columns.
        :raises TypeError: If a positional or named value is not an expression.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(2, 3)], schema=["left", "right"])
            >>> df.with_columns(
            ...     (pf.col("left") + 1).alias("left_plus_one"),
            ...     (pf.col("right") + 1).alias("right_plus_one"),
            ... )
            >>> df.with_columns(
            ...     left_plus_one=pf.col("left") + 1,
            ...     right_plus_one=pf.col("right") + 1,
            ... )

        .. versionadded:: 2.4.0
        """
        expressions: List[Expression] = []
        for expression in exprs:
            if not isinstance(expression, Expression):
                raise TypeError("exprs must be expressions")
            expressions.append(expression)

        for name, expression in named_exprs.items():
            if not isinstance(expression, Expression):
                raise TypeError("named_exprs must be expressions")
            expressions.append(expression.alias(name))

        return DataFrame(self._table.add_or_replace_columns(*expressions))

    @PublicEvolving()
    def drop_columns(
        self,
        *columns: Union[str, Expression],
        strict: bool = True,
    ) -> "DataFrame":
        """
        Remove columns from this DataFrame.

        String column names are checked against the current schema. When ``strict`` is ``False``,
        names that are not present are ignored. Expression arguments are validated by the Table
        API.

        :param columns: Column names or expressions to remove.
        :param strict: Whether a missing column name raises an error.
        :return: A new DataFrame without the requested columns, or this DataFrame if no columns
            remain to be dropped.
        :raises TypeError: If ``strict`` is not a boolean or a column has an unsupported type.
        :raises ValueError: If a named column is missing in strict mode.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(1, "debug")], schema=["id", "temporary"])
            >>> result = df.drop_columns("temporary")
            >>> unchanged = df.drop("missing", strict=False)

        .. versionadded:: 2.4.0
        """
        if not isinstance(strict, bool):
            raise TypeError("strict must be a boolean")

        existing_columns = set(self.columns)
        expressions: List[Expression] = []
        for column in columns:
            if isinstance(column, str):
                if column not in existing_columns:
                    if strict:
                        raise ValueError(f"Column '{column}' not found in schema")
                    continue
                expressions.append(table_col(column))
            elif isinstance(column, Expression):
                expressions.append(column)
            else:
                raise TypeError("columns must be strings or expressions")

        if not expressions:
            return self
        return DataFrame(self._table.drop_columns(*expressions))

    drop = drop_columns

    @PublicEvolving()
    def rename_columns(
        self,
        *args: Any,
        mapping: Optional[
            Union[Dict[str, str], Callable[[str], str]]
        ] = None,
    ) -> "DataFrame":
        """
        Rename one or more columns.

        Use exactly one of the following forms:

        * A dictionary defines mappings from existing column names to new names. It can be supplied
          as the only positional argument or through the keyword-only ``mapping`` parameter.
          Entries whose existing column name is not present are ignored.
        * An even number of positional string arguments is interpreted as alternating old and new
          column name pairs.
        * A function or lambda expression is applied to every current column name and must return
          the new name as a string. It can be supplied as the only positional argument or through
          ``mapping``.

        :param args: One dictionary or callable, or an even number of alternating old/new names.
        :param mapping: Keyword-only alternative for passing a dictionary or callable.
        :return: A new DataFrame with renamed columns, or this DataFrame if no names change.
        :raises TypeError: If the mapping, a name, or a callable result has an unsupported type.
        :raises ValueError: If positional pairs are incomplete or ``mapping`` is combined with
            positional arguments.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(1, "Alice")], schema=["id", "name"])
            >>> by_mapping = df.rename_columns({"id": "user_id"})
            >>> by_keyword = df.rename_columns(mapping={"id": "user_id"})
            >>> by_pairs = df.rename("id", "user_id", "name", "user_name")
            >>> by_function = df.rename(str.upper)
            >>> by_lambda = df.rename(lambda name: name.upper())

        .. versionadded:: 2.4.0
        """
        if args and mapping is not None:
            raise ValueError(
                "rename_columns() accepts either positional arguments or mapping, not both"
            )

        rename_spec: Any = mapping
        if len(args) == 1:
            rename_spec = args[0]
        elif args:
            if len(args) % 2 != 0:
                raise ValueError(
                    "rename_columns() positional arguments must be old/new name pairs"
                )
            positional_mapping: Dict[str, str] = {}
            for index in range(0, len(args), 2):
                old_name, new_name = args[index], args[index + 1]
                if not isinstance(old_name, str) or not isinstance(new_name, str):
                    raise TypeError("column names must be strings")
                positional_mapping[old_name] = new_name
            rename_spec = positional_mapping

        current_columns = self.columns
        rename_expressions: List[Expression] = []
        if isinstance(rename_spec, dict):
            for old_name, new_name in rename_spec.items():
                if not isinstance(old_name, str) or not isinstance(new_name, str):
                    raise TypeError("mapping keys and values must be strings")
                if old_name in current_columns and new_name != old_name:
                    rename_expressions.append(table_col(old_name).alias(new_name))
        elif callable(rename_spec):
            for old_name in current_columns:
                new_name = rename_spec(old_name)
                if not isinstance(new_name, str):
                    raise TypeError("rename_columns() callable must return a string")
                if new_name != old_name:
                    rename_expressions.append(table_col(old_name).alias(new_name))
        else:
            raise TypeError("mapping must be a dictionary or callable")

        if not rename_expressions:
            return self
        return DataFrame(self._table.rename_columns(*rename_expressions))

    rename = rename_columns

    @PublicEvolving()
    def select(
        self,
        *columns: Union[
            str,
            Expression,
            List[Union[str, Expression]],
            Tuple[Union[str, Expression], ...],
        ],
        **projections: Expression,
    ) -> "DataFrame":
        """
        Select columns and compute named projections.

        Column names and expressions are included in the supplied order. A list or tuple may be
        used to group column names and expressions. Named projections are appended after the
        positional columns.

        :param columns: Column names and expressions to select.
        :param projections: Expressions keyed by their result column names.
        :return: A new DataFrame containing the selected columns and projections.
        :raises TypeError: If a column or projection is not a supported value.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1, "name": "Alice"}])
            >>> result = df.select(
            ...     ("name", "id"), doubled=pf.col("id") * 2
            ... )

        .. versionadded:: 2.4.0
        """
        expressions: List[Expression] = []
        for column in columns:
            values = column if isinstance(column, (list, tuple)) else [column]
            for value in values:
                if isinstance(value, str):
                    expressions.append(table_col(value))
                elif isinstance(value, Expression):
                    expressions.append(value)
                else:
                    raise TypeError(
                        "columns must be strings, expressions, or lists or tuples of them"
                    )

        for name, projection in projections.items():
            if not isinstance(projection, Expression):
                raise TypeError("projections must be expressions")
            expressions.append(projection.alias(name))

        return DataFrame(self._table.select(*expressions))

    @PublicEvolving()
    def drop_duplicates(
        self,
        subset: Union[str, List[str]] = None,
        *,
        keep: str = "first",
        order_by: Union[str, Expression, List[Union[str, Expression]]] = None,
        nulls_first: Union[bool, List[bool]] = None,
    ) -> "DataFrame":
        """
        Remove duplicate rows.

        When ``subset`` is omitted, fully identical rows are dropped, equivalent to a whole-row
        ``DISTINCT``. When ``subset`` is given, rows are deduplicated by those key columns, keeping
        one row per key; ``order_by`` together with ``keep`` decides which row survives. When
        ``order_by`` is omitted, processing time is used, so ``keep`` keeps the first or last row to
        arrive.

        :param subset: Column name or list of column names that define a duplicate. When omitted,
            all columns are considered.
        :param keep: ``"first"`` keeps the earliest row, ``"last"`` the latest, in ``order_by``
            order. Ignored when ``subset`` is omitted.
        :param order_by: Column name or expression (or a list of them) defining the order in which
            ``keep`` selects the surviving row. When omitted, processing time is used.
        :param nulls_first: Where NULLs rank in ``order_by``: a single boolean applied to every key,
            or a list with one boolean per key. When omitted, the engine default applies. Requires
            ``order_by``.
        :return: A new DataFrame with duplicate rows removed.
        :raises ValueError: If ``keep`` is not ``"first"`` or ``"last"``, if ``order_by`` or
            ``nulls_first`` is combined with an omitted ``subset``, if ``nulls_first`` is given
            without ``order_by`` or with a mismatched length, or if a named column does not exist.
        :raises TypeError: If ``subset``, ``order_by`` or ``nulls_first`` has an unsupported type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1, "ts": 1}, {"id": 1, "ts": 2}])
            >>> unique_rows = df.drop_duplicates()
            >>> latest_per_id = df.drop_duplicates("id", order_by="ts", keep="last")

        .. versionadded:: 2.4.0
        """
        if keep not in ("first", "last"):
            raise ValueError('keep must be "first" or "last"')

        subset_keys = _normalize_subset(subset)
        order_keys = _normalize_order_by(order_by)

        if nulls_first is not None and order_keys is None:
            raise ValueError("nulls_first requires order_by")
        nulls = _normalize_nulls_first(
            nulls_first, len(order_keys) if order_keys else 0
        )

        if subset_keys is None:
            if order_keys is not None:
                raise ValueError(
                    "order_by requires subset; whole-row duplicates cannot be ordered"
                )
            return DataFrame(self._table.distinct())

        columns = self._table.get_resolved_schema().get_column_names()
        for name in subset_keys:
            if name not in columns:
                raise ValueError(
                    "subset column '%s' does not exist, available columns: %s" % (name, columns)
                )

        order_list = order_keys if order_keys is not None else [None]
        descending_flags = [keep == "last"] * len(order_list)
        rank_nulls = nulls if nulls is not None else [None] * len(order_list)
        return DataFrame(
            _build_rank_sql(self._table, subset_keys, order_list, descending_flags, rank_nulls, 1))

    @PublicEvolving()
    def top_n(
        self,
        n: int,
        *,
        partition_by: Union[str, List[str]] = None,
        order_by: Union[str, Expression, List[Union[str, Expression]]] = None,
        descending: Union[bool, List[bool]] = False,
        nulls_first: Union[bool, List[bool]] = None,
    ) -> "DataFrame":
        """
        Keep the top ``n`` rows per group, ordered by ``order_by``.

        With ``partition_by`` the ranking is per group; without it the ranking is global.
        ``top_n(1, ...)`` keeps a single row per group.

        The default ranking direction is ascending (``descending=False``), so ``top_n`` keeps
        the SMALLEST ``n`` rows by ``order_by``. Pass ``descending=True`` to keep the largest.

        :param n: Number of rows to keep per group. Must be >= 1.
        :param partition_by: Column name or list of column names defining the groups.
        :param order_by: Column name or expression (or a list of them) defining the ranking order.
        :param descending: Ranking direction (default ``False``); a bool or one per ``order_by``.
        :param nulls_first: Where NULLs rank in ``order_by``: a single boolean applied to every key,
            or a list with one boolean per key. When omitted, the engine default applies. Requires
            ``order_by``.
        :return: A new DataFrame with the top ``n`` rows per group.
        :raises ValueError: If ``n`` is not an int or is < 1, if a ``descending``
            or ``nulls_first`` list has a length different from ``order_by``, or if a named
            column does not exist.
        :raises TypeError: If an argument has an unsupported type, or ``order_by`` is missing
            or empty.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"cat": "a", "amount": 10}])
            >>> top3 = df.top_n(3, partition_by="cat", order_by="amount", descending=True)

        .. versionadded:: 2.4.0
        """
        if not isinstance(n, int) or n < 1:
            raise ValueError("n must be an integer >= 1")

        if order_by is None or (isinstance(order_by, (list, tuple)) and not order_by):
            raise TypeError("top_n requires a non-empty order_by")
        order_keys = _normalize_order_by(order_by)

        if partition_by is None or (isinstance(partition_by, (list, tuple)) and not partition_by):
            partition_keys = []
        else:
            partition_keys = _normalize_subset(partition_by, "partition_by")

        descending_flags = _normalize_descending(descending, len(order_keys))
        nulls = _normalize_nulls_first(nulls_first, len(order_keys))
        rank_nulls = nulls if nulls is not None else [None] * len(order_keys)
        return DataFrame(
            _build_rank_sql(self._table, partition_keys, order_keys, descending_flags, rank_nulls,
                            n))

    distinct = drop_duplicates
    unique = drop_duplicates

    @PublicEvolving()
    def flat_map(
        self,
        func: Union[Callable[[Dict[str, Any]], Any], Type, "_DataFrameUDTFWrapper"],
        *,
        return_dtype: Optional["_DataTypeLike"] = None,
    ) -> "DataFrame":
        """
        Apply a function to each row, emitting zero or more output rows.

        The function receives a dictionary keyed by column name, including when
        declared with :func:`pyflink.dataframe.udtf`.
        Output column names come from a ``TypedDict`` or an explicit named struct;
        scalar outputs use ``f0``. Multi-field outputs require named fields.

        :param func: Row-based callable, a callable class with a zero-argument constructor,
                     or a declaration created with ``pf.udtf``. Callable classes are
                     instantiated on workers.
        :param return_dtype: Emitted row type, inferred from annotations when omitted.
                             Required if inference is not possible; must be omitted
                             for a UDTF declaration.
        :return: A DataFrame containing only the emitted output columns.

        Example::

            >>> from typing import Any, Dict, Iterator, TypedDict
            >>> import pyflink.dataframe as pf
            >>> class Token(TypedDict):
            ...     word: str
            >>> def split(record: Dict[str, Any]) -> Iterator[Token]:
            ...     for word in record["text"].split():
            ...         yield {"word": word}
            >>> df = pf.from_dict({"text": ["hello world", "flink"]})
            >>> result = df.flat_map(split)
            >>> result.columns
            ['word']

        The decorator is optional for plain callables. Use it to attach reusable
        metadata, such as the output schema, instead of repeating it in each
        ``flat_map`` call::

            >>> @pf.udtf(return_dtype="ROW<word STRING>")
            ... def tokenize(record: Dict[str, Any]):
            ...     yield from record["text"].split()
            >>> result = df.flat_map(tokenize)
            >>> result.columns
            ['word']

        An explicit output type can be supplied for unannotated callables::

            >>> words = df.flat_map(lambda record: record["text"].split(), return_dtype=str)
            >>> words.columns
            ['f0']
            >>> named = df.flat_map(
            ...     lambda record: record["text"].split(),
            ...     return_dtype="ROW<word STRING>")
            >>> named.columns
            ['word']

        Callable classes can be passed directly and are instantiated on workers::

            >>> class SplitWords:
            ...     def __call__(self, record: Dict[str, Any]) -> Iterator[str]:
            ...         yield from record["text"].split()
            >>> words = df.flat_map(SplitWords)

        ``TableFunction`` classes are declared with :func:`pyflink.dataframe.udtf`::

            >>> from pyflink.table.udf import TableFunction
            >>> class SplitWordsFunction(TableFunction):
            ...     def eval(self, record: Dict[str, Any]) -> Iterator[str]:
            ...         yield from record["text"].split()
            >>> words = df.flat_map(pf.udtf(SplitWordsFunction))

        See :func:`pyflink.dataframe.udtf` for more details.

        .. versionadded:: 2.4.0
        """
        from pyflink.dataframe.udtf import _resolve_flat_map_udtf

        expression, output_columns = _resolve_flat_map_udtf(func, return_dtype, self.columns)
        table = self._table.flat_map(expression)
        # Table UDTFs expose positional field names, so restore the declared names.
        return DataFrame(table.alias(output_columns[0], *output_columns[1:]))

    @PublicEvolving()
    def explode(
        self,
        column: Union[str, Expression],
        *,
        output_column: Optional[Union[str, List[str]]] = None,
        ignore_empty_and_null: bool = False,
    ) -> "DataFrame":
        """
        Expand an ARRAY, MAP, or MULTISET into rows, preserving duplicate occurrences.

        A referenced input column is replaced in place by the expanded element. For a computed
        collection expression, all input columns are retained and the expanded element is
        appended. MAP values yield key and value fields. ROW elements remain a single ROW column;
        use :attr:`pyflink.table.Expression.flatten` in a subsequent :meth:`select` to expand its
        fields. Empty and null collections produce a row with null output fields unless
        ``ignore_empty_and_null`` is true.

        .. warning::
            Due to FLINK-40658, actual null ROW elements in an ARRAY are not handled correctly
            and may be silently dropped or cause execution to fail. This limitation is independent
            of ``ignore_empty_and_null``. A non-null ROW whose fields are all null is not affected.

        :param column: Collection column name or row-wise expression to expand.
        :param output_column: Output name or list of names. Required for multiple fields;
            a single field defaults to the selected column name. Names must be unique and must
            not conflict with retained input columns.
        :param ignore_empty_and_null: Whether to drop rows with empty or null collections.
        :return: A new DataFrame with the expanded rows.
        :raises TypeError: If an argument has an unsupported type or the input is not a collection.
        :raises ValueError: If the expression is not row-wise, selects multiple columns,
            or output names are invalid.

        Examples:

        Explode an ARRAY of scalar values::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_dict({"id": [1, 2], "tags": [["a", "b"], []]})
            >>> result = df.explode(
            ...     "tags", output_column="tag", ignore_empty_and_null=True)

        Explode an ARRAY of ROW values, then expand the ROW fields explicitly::

            >>> import pyflink.dataframe as pf
            >>> from typing import NamedTuple
            >>> class Item(NamedTuple):
            ...     name: str
            ...     quantity: int
            >>> orders = pf.from_records(
            ...     [(1, [Item("apple", 2)])], schema=["id", "items"])
            >>> exploded = orders.explode("items", ignore_empty_and_null=True)
            >>> flattened = exploded.select("id", pf.col("items").flatten)
            >>> flattened.columns
            ['id', 'items$name', 'items$quantity']

        .. versionadded:: 2.4.0
        """
        if not isinstance(column, (str, Expression)):
            raise TypeError("column must be a column name or expression")
        if not isinstance(ignore_empty_and_null, bool):
            raise TypeError("ignore_empty_and_null must be a boolean")
        if output_column is not None and not isinstance(output_column, (str, list)):
            raise TypeError("output_column must be a string or list of strings")

        expression = table_col(column) if isinstance(column, str) else column
        selected = self._table.select(expression)
        schema = selected.get_resolved_schema()
        if len(schema.get_column_names()) != 1:
            raise ValueError("column must select a single column")
        projection = selected._j_table.getQueryOperation()
        # Aggregates insert an intermediate operation whose field indexes refer to its result.
        if not projection.getChildren().get(0).equals(self._table._j_table.getQueryOperation()):
            raise ValueError("column must be a row-wise expression, not an aggregation")

        data_type = schema.get_column_data_types()[0]
        element_row_type = None
        element_row_type_sql = None
        if isinstance(data_type, MapType):
            field_count = 2
        elif isinstance(data_type, (ArrayType, MultisetType)):
            element_type = data_type.element_type
            element_row_type = element_type if isinstance(element_type, RowType) else None
            if element_row_type is not None:
                collection_type = schema._j_resolved_schema.getColumnDataTypes().get(0)
                element_row_type_sql = (
                    collection_type.getChildren()
                    .get(0)
                    .getLogicalType()
                    .copy(True)
                    .asSerializableString()
                )
            field_count = 1
        else:
            raise TypeError("column must have an ARRAY, MAP, or MULTISET type")

        if output_column is None:
            if field_count != 1:
                raise ValueError("output_column is required for multiple output fields")
            output_names = [schema.get_column_names()[0]]
        else:
            output_names = [output_column] if isinstance(output_column, str) else output_column
        if not all(isinstance(name, str) for name in output_names):
            raise TypeError("output_column must contain only strings")
        if len(output_names) != field_count:
            raise ValueError("output_column must contain %d name(s)" % field_count)
        if any(not name for name in output_names) or len(set(output_names)) != field_count:
            raise ValueError("output_column names must be non-empty and unique")

        table = self._table
        columns = list(table.get_resolved_schema().get_column_names())
        resolved = projection.getProjectList().get(0)
        # Resolve the input field by index so an alias does not hide the column to remove.
        if resolved.getClass().getSimpleName() == "FieldReferenceExpression":
            output_index = resolved.getFieldIndex()
            collection_name = columns.pop(output_index)
        else:
            output_index = len(columns)
            taken = set(columns) | set(output_names)
            collection_name = _unique_name("__pf_explode", taken)
            table = table.add_columns(expression.alias(collection_name))
        if set(output_names).intersection(columns):
            raise ValueError("output_column names conflict with retained input columns")

        unnest_output_names = output_names
        ordinality_name = None
        if element_row_type is not None:
            # SQL UNNEST expands ROW fields. Reassemble them below, using ordinality to
            # distinguish an outer-join placeholder from a real ROW whose fields are all null.
            taken = set(columns) | set(output_names) | {collection_name}
            unnest_output_names = []
            for index in range(len(element_row_type.fields)):
                name = _unique_name("__pf_explode_field_%d" % index, taken)
                taken.add(name)
                unnest_output_names.append(name)
            if not ignore_empty_and_null:
                ordinality_name = _unique_name("__pf_explode_ordinality", taken)

        if element_row_type is None:
            explode_projection = [
                "expanded." + _quote_identifier(name) for name in unnest_output_names
            ]
        else:
            row_value = "CAST(ROW(%s) AS %s)" % (
                ", ".join(
                    "expanded." + _quote_identifier(name) for name in unnest_output_names
                ),
                element_row_type_sql,
            )
            if ordinality_name is not None:
                row_value = "CASE WHEN expanded.%s IS NULL THEN CAST(NULL AS %s) ELSE %s END" % (
                    _quote_identifier(ordinality_name),
                    element_row_type_sql,
                    row_value,
                )
            explode_projection = [row_value + " AS " + _quote_identifier(output_names[0])]

        projections = ["src." + _quote_identifier(name) for name in columns]
        projections[output_index:output_index] = explode_projection
        query = "SELECT %s FROM %s AS src %s UNNEST(src.%s)%s AS expanded(%s)%s" % (
            ", ".join(projections),
            _quote_identifier(str(table)),
            "CROSS JOIN" if ignore_empty_and_null else "LEFT JOIN",
            _quote_identifier(collection_name),
            " WITH ORDINALITY" if ordinality_name is not None else "",
            ", ".join(
                _quote_identifier(name)
                for name in unnest_output_names
                + ([ordinality_name] if ordinality_name is not None else [])
            ),
            "" if ignore_empty_and_null else " ON TRUE",
        )
        return DataFrame(table._t_env.sql_query(query))

    # ======================== Joins ========================

    @PublicEvolving()
    def join(
        self,
        other: "DataFrame",
        *,
        on=None,
        how: str = "inner",
        left_on=None,
        right_on=None,
    ) -> "DataFrame":
        """
        Join this DataFrame with another DataFrame.

        Use ``on`` when both sides share the same named join keys, or pass a boolean expression as
        the complete join predicate. Use ``left_on`` and ``right_on`` together when the key names
        differ. In streaming mode, a join without equality keys may use singleton distribution
        (a single parallel instance) and can be expensive.
        Shared named keys occur once in the result; other duplicate column names must be renamed
        before joining. ``semi`` and ``anti`` joins return only columns from this DataFrame, while
        ``cross`` performs a Cartesian product and accepts no join keys.

        :param other: DataFrame on the right side of the join.
        :param on: Shared column name, list of shared column names, or a boolean join expression.
        :param how: Join type: ``"inner"``, ``"left"``, ``"right"``, ``"full"``, ``"outer"``,
            ``"semi"``, ``"anti"``, or ``"cross"``.
        :param left_on: Column name, expression, or list of column names from this DataFrame.
        :param right_on: Column name, expression, or list of column names from ``other``.
        :return: A new DataFrame containing the join result.
        :raises TypeError: If an argument has an unsupported type.
        :raises ValueError: If the join type, keys, schemas, or argument combination is invalid.

        Example::

            >>> import pyflink.dataframe as pf
            >>> orders = pf.from_records([(1, 10)], schema=["customer_id", "amount"])
            >>> customers = pf.from_records([(1, "Alice")], schema=["customer_id", "name"])
            >>> matched = orders.join(customers, on="customer_id")
            >>> matched = orders.join(
            ...     customers.rename_columns({"customer_id": "id"}),
            ...     left_on="customer_id",
            ...     right_on="id",
            ...     how="left",
            ... )

        Expression-based predicates support equality, compound conditions, and non-equi joins::

            >>> customers_by_id = customers.rename_columns({"customer_id": "id"})
            >>> matched = orders.join(
            ...     customers_by_id, on=pf.col("customer_id") == pf.col("id"))
            >>> rules = pf.from_records([(1, 5)], schema=["rule_customer_id", "min_amount"])
            >>> matched = orders.join(
            ...     rules,
            ...     on=(pf.col("customer_id") == pf.col("rule_customer_id"))
            ...     & (pf.col("amount") >= pf.col("min_amount")),
            ... )
            >>> matched = orders.join(rules, on=pf.col("amount") >= pf.col("min_amount"))
            >>> unmatched = orders.join(
            ...     customers_by_id,
            ...     on=pf.col("customer_id") == pf.col("id"),
            ...     how="anti",
            ... )

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a pyflink.dataframe.DataFrame")
        if self._table._t_env._j_tenv != other._table._t_env._j_tenv:
            raise ValueError("DataFrames must belong to the same TableEnvironment")

        join_type = _normalize_join_type(how)
        if join_type == "cross":
            if on is not None or left_on is not None or right_on is not None:
                raise ValueError("cross join does not accept on, left_on, or right_on")
            _validate_join_column_conflicts(self.columns, other.columns, set())
            return DataFrame(self._table.join(other._table))

        (
            left_table,
            right_table,
            predicate,
            shared_keys,
        ) = _prepare_join(
            self._table,
            other._table,
            on,
            left_on,
            right_on,
            validate_column_conflicts=join_type not in ("semi", "anti"),
        )

        with _JoinSqlFactory(self._table._t_env) as sql_factory:
            if join_type in ("semi", "anti"):
                return DataFrame(
                    _build_semi_anti_join_sql(
                        left_table,
                        right_table,
                        predicate,
                        self.columns,
                        join_type,
                        sql_factory,
                    )
                )

            return DataFrame(
                _build_regular_join_sql(
                    left_table,
                    right_table,
                    predicate,
                    self.columns,
                    other.columns,
                    shared_keys,
                    join_type,
                    sql_factory,
                )
            )

    # ======================== Filtering & Ordering ========================

    @PublicEvolving()
    def sort(
        self,
        by: Union[str, Expression, List[Union[str, Expression]]],
        *,
        descending: Union[bool, List[bool]] = False,
        nulls_first: Union[bool, List[bool]] = None,
    ) -> "DataFrame":
        """
        Sort rows globally by one or more columns or expressions.

        This method builds a new DataFrame plan without executing a Flink job. The ``by``
        expressions must not already specify ``asc`` or ``desc``; use ``descending`` to control
        their direction. When ``nulls_first`` is omitted, the Table API default is used: NULLs
        are ordered last for ascending keys and first for descending keys.

        The result is globally sorted across all parallel partitions. For unbounded tables, the
        first sort key must be an ascending time attribute unless the sort is followed by
        :meth:`limit`.

        :param by: Column name or expression, or a list of them, used as sort keys.
        :param descending: Whether to sort in descending order, either for all keys or once per
            key.
        :param nulls_first: Whether to place NULLs first, either for all keys or once per key. When
            omitted, the Table API default applies.
        :return: A new sorted DataFrame.
        :raises TypeError: If ``by``, ``descending`` or ``nulls_first`` has an unsupported type.
        :raises ValueError: If ``by`` is empty, option lengths do not match, a column does not
            exist, or an expression already specifies ``asc`` or ``desc``.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records(
            ...     [(2, "b"), (1, "a")], schema=["id", "name"]
            ... )
            >>> ascending = df.sort("id")
            >>> mixed = df.sort(["id", "name"], descending=[False, True])

        .. versionadded:: 2.4.0
        """
        order_keys = _normalize_order_by(by, "by")
        if order_keys is None:
            raise TypeError("by must be a string, an expression, or a list or tuple of them")
        columns = self._table.get_resolved_schema().get_column_names()
        for key in order_keys:
            if isinstance(key, str) and key not in columns:
                raise ValueError(
                    "by column '%s' does not exist, available columns: %s" % (key, columns)
                )
            if isinstance(key, Expression) and _contains_ordering_expression(key):
                raise ValueError(
                    "sort() expressions must not specify asc or desc; use descending instead"
                )

        descending_values = _normalize_descending(descending, len(order_keys))
        nulls_values: List[Optional[bool]] = (
            [None] * len(order_keys)
            if nulls_first is None
            else _normalize_nulls_first(nulls_first, len(order_keys))
        )
        if any(value is not None for value in nulls_values):
            return DataFrame(
                _build_sort_sql(self._table, order_keys, descending_values, nulls_values)
            )

        order_expressions = []
        for key, is_descending in zip(order_keys, descending_values):
            expression = table_col(key) if isinstance(key, str) else key
            order_expressions.append(expression.desc if is_descending else expression.asc)
        return DataFrame(self._table.order_by(*order_expressions))

    # ======================== Set Operations ========================

    @PublicEvolving()
    def union(self, other: "DataFrame") -> "DataFrame":
        """
        Return rows from either DataFrame, removing duplicate rows (SQL ``UNION``).

        This operation is currently supported only in batch mode.

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with compatible types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.

        :param other: The DataFrame to combine with this DataFrame.
        :return: A new DataFrame containing the distinct rows from both inputs.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.union(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.union(other._table))

    @PublicEvolving()
    def union_all(self, other: "DataFrame") -> "DataFrame":
        """
        Return rows from both DataFrames, retaining all duplicates (SQL ``UNION ALL``).

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with compatible types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.

        :param other: The DataFrame to combine with this DataFrame.
        :return: A new DataFrame containing all rows from both inputs.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.union_all(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.union_all(other._table))

    @PublicEvolving()
    def intersect(self, other: "DataFrame") -> "DataFrame":
        """
        Return rows present in both DataFrames, removing duplicates (SQL ``INTERSECT``).

        This operation is currently supported only in batch mode.

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with matching types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.
        Cast differing column types explicitly before calling this method.

        :param other: The DataFrame to intersect with this DataFrame.
        :return: A new DataFrame containing the distinct rows common to both inputs.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.intersect(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.intersect(other._table))

    @PublicEvolving()
    def intersect_all(self, other: "DataFrame") -> "DataFrame":
        """
        Return rows present in both DataFrames, retaining duplicates (SQL ``INTERSECT ALL``).

        This operation is currently supported only in batch mode.

        A row occurring ``n`` times in this DataFrame and ``m`` times in ``other`` is returned
        ``min(n, m)`` times.

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with matching types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.
        Cast differing column types explicitly before calling this method.

        :param other: The DataFrame to intersect with this DataFrame.
        :return: A new DataFrame containing the common rows with their shared multiplicities.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.intersect_all(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.intersect_all(other._table))

    @PublicEvolving()
    def minus(self, other: "DataFrame") -> "DataFrame":
        """
        Return rows absent from ``other``, removing duplicate rows (SQL ``EXCEPT``).

        This operation is currently supported only in batch mode.

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with matching types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.
        Cast differing column types explicitly before calling this method.

        :param other: The DataFrame whose rows are excluded from this DataFrame.
        :return: A new DataFrame containing the distinct rows present only in this DataFrame.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.minus(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.minus(other._table))

    @PublicEvolving()
    def minus_all(self, other: "DataFrame") -> "DataFrame":
        """
        Subtract the occurrences of rows in ``other`` from this DataFrame (SQL ``EXCEPT ALL``).

        This operation is currently supported only in batch mode.

        A row occurring ``n`` times in this DataFrame and ``m`` times in ``other`` is returned
        ``max(n - m, 0)`` times.

        Both DataFrames must belong to the same TableEnvironment and have the same number
        of columns with matching types at each position. Columns are matched by position,
        not by name; the result uses this DataFrame's column names.
        Cast differing column types explicitly before calling this method.

        :param other: The DataFrame whose row occurrences are subtracted.
        :return: A new DataFrame containing the remaining row occurrences.
        :raises TypeError: If ``other`` is not a DataFrame.

        Example::

            >>> result = left.minus_all(right)

        .. versionadded:: 2.4.0
        """
        if not isinstance(other, DataFrame):
            raise TypeError("other must be a DataFrame")
        return DataFrame(self._table.minus_all(other._table))

    # ======================== Windowing ========================

    @PublicEvolving()
    def tumble(
        self,
        *,
        on: Union[str, Expression],
        size: Union["datetime.timedelta", Expression],
    ) -> "DataFrame":
        """
        Assign rows to fixed-size, non-overlapping (tumbling) windows.

        Appends ``window_start``, ``window_end`` and ``window_time`` and returns an ordinary
        DataFrame.

        :param on: An existing event-time or processing-time column.
        :param size: Window length.
        :return: A new DataFrame with the window columns appended.
        :raises TypeError: If ``on`` or ``size`` has an unsupported type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> from datetime import timedelta
            >>> windowed = df.tumble(on="event_time", size=timedelta(minutes=10))

        .. versionadded:: 2.4.0
        """
        time_col = _resolve_window_time_column(on)
        return _window_dataframe(
            self._table, "TUMBLE", time_col, _to_interval_expression(size)
        )

    @PublicEvolving()
    def hop(
        self,
        *,
        on: Union[str, Expression],
        slide: Union["datetime.timedelta", Expression],
        size: Union["datetime.timedelta", Expression],
    ) -> "DataFrame":
        """
        Assign rows to overlapping fixed-size (hopping/sliding) windows of length ``size`` starting
        every ``slide``. Appends ``window_start``/``window_end``/``window_time``.

        :param on: An existing event-time or processing-time column.
        :param slide: Interval between successive window starts.
        :param size: Window length.
        :return: A new DataFrame with the window columns appended.

        Example::

            >>> import pyflink.dataframe as pf
            >>> from datetime import timedelta
            >>> windowed = df.hop(
            ...     on="event_time",
            ...     slide=timedelta(minutes=5),
            ...     size=timedelta(minutes=10),
            ... )

        .. versionadded:: 2.4.0
        """
        time_col = _resolve_window_time_column(on)
        return _window_dataframe(
            self._table,
            "HOP",
            time_col,
            _to_interval_expression(slide),
            _to_interval_expression(size),
        )

    @PublicEvolving()
    def cumulate(
        self,
        *,
        on: Union[str, Expression],
        step: Union["datetime.timedelta", Expression],
        size: Union["datetime.timedelta", Expression],
    ) -> "DataFrame":
        """
        Assign rows to cumulating windows that share a start and grow by ``step`` up to ``size``.
        Appends ``window_start``/``window_end``/``window_time``.

        :param on: An existing event-time or processing-time column.
        :param step: Interval by which each window grows.
        :param size: Maximum window length (a whole multiple of ``step``).
        :return: A new DataFrame with the window columns appended.

        Example::

            >>> import pyflink.dataframe as pf
            >>> from datetime import timedelta
            >>> windowed = df.cumulate(
            ...     on="event_time",
            ...     step=timedelta(minutes=5),
            ...     size=timedelta(minutes=10),
            ... )

        .. versionadded:: 2.4.0
        """
        time_col = _resolve_window_time_column(on)
        return _window_dataframe(
            self._table,
            "CUMULATE",
            time_col,
            _to_interval_expression(step),
            _to_interval_expression(size),
        )

    @PublicEvolving()
    def session(
        self,
        *,
        on: Union[str, Expression],
        gap: Union["datetime.timedelta", Expression],
        partition_by: Optional[
            Union[str, Expression, List[Union[str, Expression]]]
        ] = None,
    ) -> "DataFrame":
        """
        Assign rows to activity-based (session) windows that close after ``gap`` of inactivity.
        Appends ``window_start``/``window_end``/``window_time``.
        When ``partition_by`` is given, sessions are computed independently per key, so a gap in
        one key's activity does not close another key's session.

        :param on: An existing event-time or processing-time column.
        :param gap: Inactivity gap that closes a session.
        :param partition_by: Optional column name or expression or list of column names or
            expressions to compute per-key sessions. ``None`` means a global session.
        :return: A new DataFrame with the window columns appended.
        :raises TypeError: If ``partition_by`` contains an unsupported element type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> from datetime import timedelta
            >>> windowed = df.session(
            ...     on="event_time",
            ...     gap=timedelta(minutes=10),
            ...     partition_by="id",
            ... )

        .. versionadded:: 2.4.0
        """
        time_col = _resolve_window_time_column(on)
        partition_cols = _resolve_partition_columns(partition_by)
        return _window_dataframe(
            self._table,
            "SESSION",
            time_col,
            _to_interval_expression(gap),
            partition_cols=partition_cols,
        )

    # ======================== Slicing ========================

    @PublicEvolving()
    def limit(self, n: int) -> "DataFrame":
        """
        Keep at most the first ``n`` rows.

        This method builds a new DataFrame plan without executing a Flink job. Execution is
        triggered by an action such as :meth:`collect` or :meth:`to_pandas`. Without an explicit
        ordering on the underlying table, the selected rows and their order are unspecified.
        Changes to the underlying table content may also change the result.

        :param n: Maximum number of rows to keep.
        :return: A new DataFrame containing at most ``n`` rows.
        :raises TypeError: If ``n`` is not an integer.
        :raises ValueError: If ``n`` is negative.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1}, {"id": 2}, {"id": 3}])
            >>> first_two = df.limit(2)

        .. versionadded:: 2.4.0
        """
        _validate_row_count(n)
        return DataFrame(self._table.fetch(n))

    @PublicEvolving()
    def offset(self, n: int) -> "DataFrame":
        """
        Skip the first ``n`` rows.

        This method builds a new DataFrame plan without executing a Flink job. Execution is
        triggered by an action such as :meth:`collect` or :meth:`to_pandas`. Without an explicit
        ordering on the underlying table, the skipped rows and their order are unspecified.
        Changes to the underlying table content may also change the result. Combine this method
        with :meth:`limit` for pagination.

        :param n: Number of rows to skip.
        :return: A new DataFrame without the first ``n`` rows.
        :raises TypeError: If ``n`` is not an integer.
        :raises ValueError: If ``n`` is negative.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1}, {"id": 2}, {"id": 3}])
            >>> page = df.offset(1).limit(2)

        .. versionadded:: 2.4.0
        """
        _validate_row_count(n)
        return DataFrame(self._table.offset(n))

    @PublicEvolving()
    def head(self, n: int) -> "DataFrame":
        """
        Keep at most the first ``n`` rows.

        This method builds a new DataFrame plan without executing a Flink job and delegates to
        :meth:`limit`. Execution is triggered by an action such as :meth:`collect` or
        :meth:`to_pandas`. Without an explicit ordering on the underlying table, the selected rows
        and their order are unspecified. Changes to the underlying table content may also change
        the result.

        :param n: Maximum number of rows to keep.
        :return: A new DataFrame containing at most ``n`` rows.
        :raises TypeError: If ``n`` is not an integer.
        :raises ValueError: If ``n`` is negative.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1}, {"id": 2}, {"id": 3}])
            >>> first_two = df.head(2)

        .. versionadded:: 2.4.0
        """
        return self.limit(n)

    # ======================== Aggregation ========================

    @PublicEvolving()
    def group_by(self, *columns: Union[str, Expression]) -> "GroupedDataFrame":
        """
        Group rows by one or more columns for aggregation.

        String column names are converted to column expressions. Grouping keys are retained in
        their supplied order and are included first in the result of
        :meth:`GroupedDataFrame.agg`.

        :param columns: Column names or expressions used as grouping keys.
        :return: A grouped DataFrame that can be aggregated.
        :raises TypeError: If a grouping key is not a string or expression.
        :raises ValueError: If no grouping keys are provided.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     ("engineering", 10),
            ...     ("engineering", 20),
            ...     ("sales", 5),
            ... ], schema=["department", "amount"])
            >>> totals = df.group_by("department").agg(
            ...     pf.col("amount").sum.alias("total_amount"),
            ...     row_count=pf.col("amount").count,
            ... )
            >>> # totals schema: [department: STRING, total_amount: BIGINT,
            >>> #                 row_count: BIGINT NOT NULL]

        .. versionadded:: 2.4.0
        """
        if not columns:
            raise ValueError("group_by() requires at least one grouping key")

        grouping_keys: List[Expression] = []
        for column in columns:
            if isinstance(column, str):
                grouping_keys.append(table_col(column))
            elif isinstance(column, Expression):
                grouping_keys.append(column)
            else:
                raise TypeError(
                    "group_by() grouping keys must be strings or expressions"
                )
        return GroupedDataFrame(self, grouping_keys)

    @PublicEvolving()
    def agg(self, *aggs: Expression, **named_aggs: Expression) -> "DataFrame":
        """
        Aggregate all rows in this DataFrame.

        Positional aggregation expressions are followed by named aggregations in the result.
        Each named aggregation is aliased to its keyword name.

        :param aggs: Aggregation expressions.
        :param named_aggs: Aggregation expressions keyed by their result column names.
        :return: A DataFrame containing the global aggregation results.
        :raises TypeError: If an aggregation is not an expression.
        :raises ValueError: If no aggregations are provided.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     (1, 10), (2, 20)
            ... ], schema=["order_id", "amount"])
            >>> summary = df.agg(
            ...     pf.col("order_id").count.alias("order_count"),
            ...     total_amount=pf.col("amount").sum,
            ... )
            >>> # summary schema: [order_count: BIGINT NOT NULL, total_amount: BIGINT]

        .. versionadded:: 2.4.0
        """
        aggregations = _normalize_aggregations(aggs, named_aggs)
        return DataFrame(self._table.group_by().select(*aggregations))

    # ======================== Special Methods ========================

    @overload
    def __getitem__(self, key: str) -> Expression:
        ...

    @overload
    def __getitem__(
        self, key: List[Union[str, Expression]]
    ) -> "DataFrame":
        ...

    @overload
    def __getitem__(
        self, key: Tuple[Union[str, Expression], ...]
    ) -> "DataFrame":
        ...

    @overload
    def __getitem__(self, key: Expression) -> "DataFrame":
        ...

    @PublicEvolving()
    def __getitem__(
        self,
        key: Union[
            str,
            List[Union[str, Expression]],
            Tuple[Union[str, Expression], ...],
            Expression,
        ],
    ) -> Union["DataFrame", Expression]:
        """
        Select a column, select multiple columns, or filter rows.

        A string returns its column expression, a list or tuple returns a DataFrame containing the
        listed columns, and a boolean expression returns a filtered DataFrame.

        :param key: Column name, list or tuple of columns, or boolean expression.
        :return: A column expression or a new DataFrame.
        :raises TypeError: If ``key`` has an unsupported type.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1, "name": "Alice"}])
            >>> identifier = df["id"]
            >>> selected = df[("id", "name")]
            >>> filtered = df[df["id"] > 0]

        .. versionadded:: 2.4.0
        """
        if isinstance(key, str):
            return table_col(key)
        if isinstance(key, (list, tuple)):
            return self.select(key)
        if isinstance(key, Expression):
            return self.filter(key)
        raise TypeError("key must be a string, list, tuple, or Expression")

    @PublicEvolving()
    def __getattr__(self, name: str) -> Expression:
        """
        Return a column expression for an attribute name.

        The name must be a valid Python identifier, must not start with an underscore, must not be
        a Python keyword, and must identify an existing column. Existing DataFrame attributes take
        precedence over columns. Use ``df["column name"]`` for names that cannot be accessed as
        attributes, or ``df["select"]`` for columns that conflict with existing attributes.

        This method resolves the schema without executing a Flink job.

        :param name: Name of the referenced column.
        :return: An expression referencing the column.
        :raises AttributeError: If the name is invalid, private, or does not identify an existing
            column.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1, "name": "Alice"}])
            >>> selected = df.select(df.name)
            >>> filtered = df.filter(df.id > 0)

        .. versionadded:: 2.4.0
        """
        if (
            name.startswith("_")
            or not name.isidentifier()
            or keyword.iskeyword(name)
            or any(name in cls.__dict__ for cls in type(self).__mro__)
        ):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

        # Avoid re-entering __getattr__ if the underlying table has not been initialized.
        try:
            table = object.__getattribute__(self, "_table")
        except AttributeError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from None

        if name not in table.get_resolved_schema().get_column_names():
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        return table_col(name)

    # ======================== Composition ========================

    @PublicEvolving()
    def pipe(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Apply a function to this DataFrame for reusable functional composition.

        This DataFrame is passed as the first argument, followed by ``args`` and ``kwargs``. The
        function's return value is returned unchanged.

        :param func: Function whose first argument receives this DataFrame.
        :param args: Additional positional arguments passed to ``func``.
        :param kwargs: Additional keyword arguments passed to ``func``.
        :return: The value returned by ``func``.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(1, 2)], schema=["left", "right"])
            >>> result = df.pipe(
            ...     lambda current, name: current.with_column(
            ...         name, pf.col("left") + pf.col("right")
            ...     ),
            ...     "total",
            ... )

        .. versionadded:: 2.4.0
        """
        return func(self, *args, **kwargs)

    # ======================== Missing Value Handling ========================

    def _validate_subset(self, subset: Optional[List[str]]) -> List[str]:
        """
        Validate and normalize the subset parameter.

        :param subset: Column names to validate, or None for all columns.
        :return: Validated list of column names.
        :raises ValueError: If subset is empty or contains invalid column names.
        :raises TypeError: If subset is not a list of strings.
        """
        schema = self._table.get_schema()
        all_columns = schema.get_field_names()

        if subset is None:
            return all_columns

        if not isinstance(subset, list):
            raise TypeError("subset must be a list of strings")

        if not subset:
            raise ValueError("subset cannot be empty")

        # Validate all column names exist
        all_columns_set = set(all_columns)
        invalid_columns = set(subset) - all_columns_set
        if invalid_columns:
            raise ValueError(f"Columns not found in DataFrame: {sorted(invalid_columns)}")

        return subset

    def _fill_values(
        self,
        value: Any,
        subset: Optional[List[str]],
        condition_fn: Callable[[Expression], Expression]
    ) -> "DataFrame":
        """
        Helper method to fill values based on a condition.

        :param value: The value to use as replacement.
        :param subset: Column names to fill, or None for all columns.
        :param condition_fn: Function that takes a column expression and returns
                           a boolean expression indicating when to replace.
        :return: A new DataFrame with values replaced.
        """
        subset = self._validate_subset(subset)
        subset_set = set(subset)

        schema = self._table.get_schema()
        all_columns = schema.get_field_names()

        expressions = []
        for col_name in all_columns:
            col_expr = table_col(col_name)
            if col_name in subset_set:
                col_type = schema.get_field_data_type(col_name)
                typed_value = table_lit(value).cast(col_type)
                filled_expr = if_then_else(
                    condition_fn(col_expr),
                    typed_value,
                    col_expr
                ).alias(col_name)
                expressions.append(filled_expr)
            else:
                expressions.append(col_expr)

        return DataFrame(self._table.select(*expressions))

    @PublicEvolving()
    def drop_null(self, subset: Optional[List[str]] = None) -> "DataFrame":
        """
        Remove rows containing NULL values.

        This method uses three-valued logic: NULL values in the specified columns
        will cause the row to be filtered out. Rows where all checked columns are
        non-NULL will be retained.

        :param subset: Column names to check. If None, checks all columns.
        :return: A new DataFrame with rows containing NULL values removed.
        :raises ValueError: If subset is empty or contains invalid column names.
        :raises TypeError: If subset is not a list of strings.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     {"id": 1, "name": "Alice", "age": 30},
            ...     {"id": 2, "name": None, "age": 25},
            ...     {"id": 3, "name": "Bob", "age": None},
            ... ])
            >>> df.drop_null()  # Drop rows with any NULL
            >>> df.drop_null(subset=["age"])  # Drop rows where "age" is NULL

        .. versionadded:: 2.4.0
        """
        subset = self._validate_subset(subset)
        conditions = [table_col(col_name).is_not_null for col_name in subset]
        condition = and_(*conditions) if len(conditions) > 1 else conditions[0]
        return DataFrame(self._table.filter(condition))

    @PublicEvolving()
    def drop_nan(self, subset: Optional[List[str]] = None) -> "DataFrame":
        """
        Remove rows containing NaN values (for float/double columns).

        This method uses three-valued logic: NaN values in the specified columns
        will cause the row to be filtered out. NULL values are preserved (not
        treated as NaN). Only applies to floating-point numeric types.

        :param subset: Column names to check. If None, checks all columns.
        :return: A new DataFrame with rows containing NaN values removed.
        :raises ValueError: If subset is empty or contains invalid column names.
        :raises TypeError: If subset is not a list of strings.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     {"id": 1, "score": 0.95},
            ...     {"id": 2, "score": float('nan')},
            ... ])
            >>> df.drop_nan()  # Drop rows with any NaN
            >>> df.drop_nan(subset=["score"])  # Drop rows where "score" is NaN

        .. versionadded:: 2.4.0
        """
        subset = self._validate_subset(subset)
        conditions = [table_col(col_name).is_not_nan for col_name in subset]
        condition = and_(*conditions) if len(conditions) > 1 else conditions[0]
        return DataFrame(self._table.filter(condition))

    @PublicEvolving()
    def fill_null(self, value: Any, subset: Optional[List[str]] = None) -> "DataFrame":
        """
        Replace NULL values with a specified value.

        This method uses three-valued logic: NULL values in the specified columns
        are replaced with the provided value, while non-NULL values are preserved.
        The replacement value is automatically cast to match each column's data type.

        :param value: The value to replace NULL with.
        :param subset: Column names to fill. If None, fills all columns.
        :return: A new DataFrame with NULL values replaced.
        :raises ValueError: If subset is empty or contains invalid column names.
        :raises TypeError: If subset is not a list of strings.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     {"id": 1, "name": "Alice", "quantity": 10},
            ...     {"id": 2, "name": None, "quantity": None},
            ... ])
            >>> df.fill_null(0, subset=["quantity"])
            >>> df.fill_null("unknown", subset=["name"])

        .. versionadded:: 2.4.0
        """
        return self._fill_values(value, subset, lambda col: col.is_null)

    @PublicEvolving()
    def fill_nan(self, value: Any, subset: Optional[List[str]] = None) -> "DataFrame":
        """
        Replace NaN values with a specified value (for float/double columns).

        This method uses three-valued logic: NaN values in the specified columns
        are replaced with the provided value, while non-NaN values are preserved.
        NULL values are preserved (not treated as NaN). The replacement value is
        automatically cast to match each column's data type.

        :param value: The value to replace NaN with.
        :param subset: Column names to fill. If None, fills all columns.
        :return: A new DataFrame with NaN values replaced.
        :raises ValueError: If subset is empty or contains invalid column names.
        :raises TypeError: If subset is not a list of strings.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     {"id": 1, "score": 0.95},
            ...     {"id": 2, "score": float('nan')},
            ... ])
            >>> df.fill_nan(0.0, subset=["score"])

        .. versionadded:: 2.4.0
        """
        return self._fill_values(value, subset, lambda col: is_nan(col))

    # ======================== Conversion ========================

    @PublicEvolving()
    def collect(self) -> List[Row]:
        """
        Execute this DataFrame and return all rows.

        The result iterator is always closed before this method returns or propagates an error.

        :return: All result rows in collection order.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1}, {"id": 2}])
            >>> rows = df.collect()

        .. versionadded:: 2.4.0
        """
        with self._table.execute().collect() as rows:
            return list(rows)

    @PublicEvolving()
    def iter_rows(
        self,
        *,
        include_row_kind: bool = False,
        row_kind_field: str = "__row_kind__",
    ) -> CloseableIterator[Dict[str, Any]]:
        """
        Execute this DataFrame and iterate over its rows as they arrive, one dict per row.

        Rows are fetched incrementally, so this also works on unbounded sources. The job runs until
        the iterator is exhausted or closed; use it in a ``with`` block.

        On an updating DataFrame, such as a streaming aggregation, every changelog entry is a
        separate row. Set ``include_row_kind`` to tell insertions, updates and deletions apart.

        TIMESTAMP_LTZ columns are not supported yet: the row transfer shared with :meth:`collect`
        cannot serialize them.

        :param include_row_kind: Whether to add each row's change kind, one of ``"+I"``,
            ``"-U"``, ``"+U"`` and ``"-D"``.
        :param row_kind_field: Key under which the change kind is added. It must not clash with a
            column name.
        :return: An iterator of dicts mapping column names to values.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([{"id": 1}, {"id": 2}])
            >>> with df.iter_rows() as rows:
            ...     for row in rows:
            ...         print(row["id"])

        .. versionadded:: 2.4.0
        """
        columns = self.columns
        _validate_row_kind_field(columns, include_row_kind, row_kind_field)
        return _iterate_rows(self._table, columns, row_kind_field if include_row_kind else None)

    @PublicEvolving()
    def iter_batches(
        self,
        *,
        batch_size: int = 1000,
        batch_format: Literal["pandas", "pyarrow"] = "pandas",
        include_row_kind: bool = False,
        row_kind_field: str = "__row_kind__",
    ) -> CloseableIterator[Union["pandas.DataFrame", "pyarrow.Table"]]:
        """
        Execute this DataFrame and iterate over its rows in batches of ``batch_size``.

        Every batch except possibly the last holds exactly ``batch_size`` rows, so on a slow
        unbounded source a batch is only emitted once enough rows have arrived. The job runs until
        the iterator is exhausted or closed; use it in a ``with`` block.

        TIMESTAMP_LTZ columns are not supported yet: the row transfer shared with :meth:`collect`
        cannot serialize them.

        :param batch_size: Number of rows per batch.
        :param batch_format: ``"pandas"`` for pandas DataFrames or ``"pyarrow"`` for PyArrow
            Tables.
        :param include_row_kind: Whether to add a column with each row's change kind, one of
            ``"+I"``, ``"-U"``, ``"+U"`` and ``"-D"``.
        :param row_kind_field: Name of the change kind column. It must not clash with a column
            name.
        :return: An iterator of batches in the requested format.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.range(10)
            >>> with df.iter_batches(batch_size=4) as batches:
            ...     for batch in batches:
            ...         print(len(batch))

        .. versionadded:: 2.4.0
        """
        _require_int(batch_size, "batch_size", 1)
        _require_choice(batch_format, "batch_format", _BATCH_FORMATS)
        schema = self._table.get_resolved_schema()
        columns = schema.get_column_names()
        data_types = schema.get_column_data_types()
        _validate_row_kind_field(columns, include_row_kind, row_kind_field)
        row_kind_field = row_kind_field if include_row_kind else None
        return _iterate_batches(
            self._table,
            batch_size,
            batch_format,
            data_types,
            _build_arrow_schema(columns, data_types, row_kind_field),
            row_kind_field,
        )

    @PublicEvolving()
    def take(
        self,
        n: int,
        *,
        timeout: Optional[float] = None,
        include_row_kind: bool = False,
        row_kind_field: str = "__row_kind__",
    ) -> List[Dict[str, Any]]:
        """
        Execute this DataFrame and return its first ``n`` rows as dicts.

        The job is cancelled once ``n`` rows have arrived if it is still running, so this is a
        safe way to peek at an unbounded source. Fewer rows are returned if the result ends first
        or ``timeout`` expires.
        The query itself is not limited: on an updating DataFrame the rows are the first ``n``
        changelog entries.

        TIMESTAMP_LTZ columns are not supported yet: the row transfer shared with :meth:`collect`
        cannot serialize them.

        :param n: Maximum number of rows to return.
        :param timeout: Maximum number of seconds to wait for rows once the job is submitted.
            ``None`` waits until ``n`` rows arrive or the result ends.
        :param include_row_kind: Whether to add each row's change kind, one of ``"+I"``,
            ``"-U"``, ``"+U"`` and ``"-D"``.
        :param row_kind_field: Key under which the change kind is added. It must not clash with a
            column name.
        :return: Up to ``n`` dicts mapping column names to values.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.range(100)
            >>> df.take(2)
            [{'id': 0}, {'id': 1}]

        .. versionadded:: 2.4.0
        """
        _validate_row_count(n)
        if timeout is not None:
            _require_number(timeout, "timeout", 0)
        columns = self.columns
        _validate_row_kind_field(columns, include_row_kind, row_kind_field)
        row_kind_field = row_kind_field if include_row_kind else None
        return [
            _row_to_dict(row, columns, row_kind_field)
            for row in _take_rows(self._table, n, timeout)
        ]

    @PublicEvolving()
    def take_batch(
        self,
        n: int,
        *,
        timeout: Optional[float] = None,
        batch_format: Literal["pandas", "pyarrow"] = "pandas",
        include_row_kind: bool = False,
        row_kind_field: str = "__row_kind__",
    ) -> Union["pandas.DataFrame", "pyarrow.Table"]:
        """
        Execute this DataFrame and return its first ``n`` rows as a single batch.

        Behaves like :meth:`take` but returns a pandas DataFrame or PyArrow Table.

        TIMESTAMP_LTZ columns are not supported yet: the row transfer shared with :meth:`collect`
        cannot serialize them.

        :param n: Maximum number of rows to return.
        :param timeout: Maximum number of seconds to wait for rows once the job is submitted.
            ``None`` waits until ``n`` rows arrive or the result ends.
        :param batch_format: ``"pandas"`` for a pandas DataFrame or ``"pyarrow"`` for a PyArrow
            Table.
        :param include_row_kind: Whether to add a column with each row's change kind, one of
            ``"+I"``, ``"-U"``, ``"+U"`` and ``"-D"``.
        :param row_kind_field: Name of the change kind column. It must not clash with a column
            name.
        :return: A batch of up to ``n`` rows in the requested format.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.range(100)
            >>> pdf = df.take_batch(5)

        .. versionadded:: 2.4.0
        """
        _validate_row_count(n)
        if timeout is not None:
            _require_number(timeout, "timeout", 0)
        _require_choice(batch_format, "batch_format", _BATCH_FORMATS)
        schema = self._table.get_resolved_schema()
        columns = schema.get_column_names()
        data_types = schema.get_column_data_types()
        _validate_row_kind_field(columns, include_row_kind, row_kind_field)
        row_kind_field = row_kind_field if include_row_kind else None
        arrow_schema = _build_arrow_schema(columns, data_types, row_kind_field)
        return _rows_to_batch(
            _take_rows(self._table, n, timeout),
            data_types,
            arrow_schema,
            batch_format,
            row_kind_field,
        )

    @PublicEvolving()
    def to_table(self) -> Table:
        """
        Return the underlying PyFlink Table without copying or converting it.

        This method does not trigger job execution.

        :return: The exact Table wrapped by this DataFrame.

        Example::

            >>> import pyflink.dataframe as pf
            >>> table = table_env.from_elements([(1,)], ["id"])
            >>> dataframe = pf.from_table(table)
            >>> dataframe.to_table() is table
            True

        .. versionadded:: 2.4.0
        """
        return self._table

    @PublicEvolving()
    def to_pandas(self) -> "pandas.DataFrame":
        """
        Execute this DataFrame and collect its rows into a pandas DataFrame.

        All results are transferred to the client and must fit in client memory.

        :return: A pandas DataFrame containing all result rows.

        Example::

            >>> import pyflink.dataframe as pf
            >>> dataframe = pf.from_records([{"id": 1}, {"id": 2}])
            >>> pdf = dataframe.to_pandas()

        .. versionadded:: 2.4.0
        """
        return self._table.to_pandas()

    # ======================== Properties ========================

    @property
    @PublicEvolving()
    def schema(self) -> "TableSchema":
        """
        Return this DataFrame's schema.

        :return: The TableSchema exposed by the underlying Table.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(1, "Alice")], schema=["id", "name"])
            >>> df.schema.get_field_names()
            ['id', 'name']

        .. versionadded:: 2.4.0
        """
        return self._table.get_schema()

    @property
    @PublicEvolving()
    def columns(self) -> List[str]:
        """
        Return this DataFrame's column names in schema order.

        :return: A new list containing the column names.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([(1, "Alice")], schema=["id", "name"])
            >>> df.columns
            ['id', 'name']

        .. versionadded:: 2.4.0
        """
        return list(self._table.get_resolved_schema().get_column_names())

    # ======================== I/O ========================

    @PublicEvolving()
    def write_parquet(
        self,
        path: str,
        *,
        mode: str = "append",
        partition_by: Optional[Union[str, List[str]]] = None,
        compression: Optional[str] = None,
        utc_timezone: Optional[bool] = None,
        sink_parallelism: Optional[int] = None,
        sink_shuffle_by_partition: Optional[bool] = None,
        auto_compaction: Optional[bool] = None,
        compaction_file_size: Optional[str] = None,
        rolling_policy_file_size: Optional[str] = None,
        rolling_policy_rollover_interval: Optional[str] = None,
        rolling_policy_inactivity_interval: Optional[str] = None,
        rolling_policy_check_interval: Optional[str] = None,
        partition_commit_trigger: Optional[str] = None,
        partition_commit_delay: Optional[str] = None,
        partition_commit_policy_kind: Optional[str] = None,
        connector_options: Optional[Dict[str, str]] = None,
        format_options: Optional[Dict[str, str]] = None,
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        """
        Write Parquet files using Flink's filesystem connector.

        The filesystem connector and Parquet format must be available to Flink. By default,
        the write is submitted immediately and waits for completion for local or MiniCluster
        execution. Passing ``statement_set`` stages the write without executing it.
        Sink columns are derived from this DataFrame's schema. Writes default to append in both
        batch and streaming execution. Explicit overwrite requires batch execution and is rejected
        by the filesystem connector in streaming execution. Partitioned overwrite replaces only
        partitions present in the input, retaining other partitions.

        Optional connector and format parameters use ``None`` to leave the option unspecified.
        If neither a parameter nor its dictionary option is set, the connector or format factory
        supplies the default.

        Rolling policies apply to streaming sinks. Parquet also rolls files on checkpoints;
        continuous writes require checkpointing to finish files. Partition commit in streaming
        requires ``partition_by`` and a commit policy. ``partition-time`` additionally requires
        upstream watermarks and a partition time extractor, configured via ``connector_options``.
        For a TIMESTAMP_LTZ watermark, set ``sink.partition-commit.watermark-time-zone`` in
        ``connector_options`` to the session time zone; its default is UTC.

        :param path: Output directory URI supported by Flink's filesystem implementations.
        :param mode: ``"append"`` (default) adds files; ``"overwrite"`` replaces existing data
            in batch execution only.
        :param partition_by: Partition column name or non-empty list of names in directory order.
            Values are stored in Hive-style partition paths rather than in the Parquet records.
        :param compression: Parquet compression codec. The format default is currently
            ``"SNAPPY"``.
        :param utc_timezone: Use UTC for Parquet timestamp conversion. The format default is
            currently ``False``, which uses the JVM default time zone, independently of the
            session time zone.
        :param sink_parallelism: Sink parallelism. The connector default is the upstream
            parallelism.
        :param sink_shuffle_by_partition: Shuffle rows by dynamic partition fields before writing.
            This can reduce the number of files but may cause data skew. The connector default
            is currently ``False``.
        :param auto_compaction: Automatically compact files in streaming execution after
            checkpoints complete. Files remain invisible until compaction finishes. The connector
            default is currently ``False``.
        :param compaction_file_size: Target file size for automatic compaction, for example
            ``"128mb"``. The connector default is the rolling policy file size.
        :param rolling_policy_file_size: Part file size threshold for rolling, not a hard upper
            bound. The connector default is currently ``"128mb"``.
        :param rolling_policy_rollover_interval: Part file open-time threshold. The connector
            default is currently ``"30min"``.
        :param rolling_policy_inactivity_interval: Part file inactivity threshold. The connector
            default is currently ``"30min"``.
        :param rolling_policy_check_interval: Interval for checking time-based rolling policies.
            The connector default is currently ``"1min"``.
        :param partition_commit_trigger: Partition commit trigger: ``"process-time"`` or
            ``"partition-time"``. The connector default is currently ``"process-time"``.
        :param partition_commit_delay: Delay before committing a partition. The connector
            default is currently ``"0s"``.
        :param partition_commit_policy_kind: Optional comma-separated policies, such as
            ``"success-file"`` or ``"custom"``. The ``metastore`` policy requires a Hive table.
        :param connector_options: Additional filesystem options with string keys and values.
            The ``connector``, ``path`` and ``format`` keys are reserved. Format options belong in
            ``format_options``. Explicit parameter and dictionary values must agree when both
            are set. Unspecified options are left to the connector factory.
        :param format_options: Parquet options with string values, with or without the
            ``parquet.`` prefix. Duplicate normalized keys are rejected. ``None`` parameters
            leave dictionary values unchanged; conflicting explicit values are rejected.
            For INT64 timestamp encoding, use ``{"write.int64.timestamp": "true",
            "timestamp.time.unit": "micros"}``. The default encoding is INT96.
        :param statement_set: Optional :class:`~pyflink.table.StatementSet` to stage the write in.
            It must use the same TableEnvironment as this DataFrame. Call its ``execute()``
            method to submit all staged writes together.
        :raises TypeError: If an argument has an invalid type.
        :raises ValueError: If the path is empty, the write mode is unsupported, the partition
            specification is empty or contains empty or duplicate names, or options conflict
            or contain reserved, empty or duplicate keys, or the statement set belongs to a
            different TableEnvironment.

        Example::

            >>> import pyflink.dataframe as pf
            >>> _ = pf.config.set("execution.runtime-mode", "batch")
            >>> events = pf.from_records([(1, "login")], schema=["id", "event"])
            >>> events.write_parquet("file:///tmp/events", compression="GZIP")

        .. versionadded:: 2.4.0
        """
        from pyflink.dataframe.io import (
            _boolean_option,
            _build_filesystem_options,
            _parallelism_option,
        )

        options = _build_filesystem_options(
            path,
            "parquet",
            connector_parameters={
                "sink.parallelism": _parallelism_option(sink_parallelism),
                "sink.shuffle-by-partition.enable": _boolean_option(
                    sink_shuffle_by_partition, "sink_shuffle_by_partition"),
                "auto-compaction": _boolean_option(auto_compaction, "auto_compaction"),
                "compaction.file-size": compaction_file_size,
                "sink.rolling-policy.file-size": rolling_policy_file_size,
                "sink.rolling-policy.rollover-interval": rolling_policy_rollover_interval,
                "sink.rolling-policy.inactivity-interval": rolling_policy_inactivity_interval,
                "sink.rolling-policy.check-interval": rolling_policy_check_interval,
                "sink.partition-commit.trigger": partition_commit_trigger,
                "sink.partition-commit.delay": partition_commit_delay,
                "sink.partition-commit.policy.kind": partition_commit_policy_kind,
            },
            format_parameters={
                "compression": compression,
                "utc-timezone": _boolean_option(utc_timezone, "utc_timezone"),
            },
            extra_connector_options=connector_options,
            extra_format_options=format_options,
        )
        self._write(
            "filesystem",
            options,
            mode=mode,
            partition_by=partition_by,
            statement_set=statement_set,
        )

    @PublicEvolving()
    def write_json(
        self,
        path: str,
        *,
        mode: str = "append",
        partition_by: Optional[Union[str, List[str]]] = None,
        timestamp_format: Optional[str] = None,
        ignore_null_fields: Optional[bool] = None,
        decimal_as_plain_number: Optional[bool] = None,
        sink_parallelism: Optional[int] = None,
        sink_shuffle_by_partition: Optional[bool] = None,
        auto_compaction: Optional[bool] = None,
        compaction_file_size: Optional[str] = None,
        rolling_policy_file_size: Optional[str] = None,
        rolling_policy_rollover_interval: Optional[str] = None,
        rolling_policy_inactivity_interval: Optional[str] = None,
        rolling_policy_check_interval: Optional[str] = None,
        partition_commit_trigger: Optional[str] = None,
        partition_commit_delay: Optional[str] = None,
        partition_commit_policy_kind: Optional[str] = None,
        connector_options: Optional[Dict[str, str]] = None,
        format_options: Optional[Dict[str, str]] = None,
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        """
        Write newline-delimited JSON files using Flink's filesystem connector.

        The filesystem connector and JSON format must be available to Flink. By default,
        the write is submitted immediately and waits for completion for local or MiniCluster
        execution. Passing ``statement_set`` stages the write without executing it.
        Sink columns are derived from this DataFrame's schema. Writes default to append in both
        batch and streaming execution. Explicit overwrite requires batch execution and is rejected
        by the filesystem connector in streaming execution. Partitioned overwrite replaces only
        partitions present in the input, retaining other partitions.

        Optional connector and format parameters use ``None`` to leave the option unspecified.
        If neither a parameter nor its dictionary option is set, the connector or format factory
        supplies the default.

        Rolling policies apply to streaming sinks. Continuous writes require both file rolling
        and checkpointing to finish files. With automatic compaction, files also roll on
        checkpoints. Partition commit in streaming requires ``partition_by``
        and a commit policy. ``partition-time`` additionally requires upstream watermarks and a
        partition time extractor, configured via ``connector_options``.
        For a TIMESTAMP_LTZ watermark, set ``sink.partition-commit.watermark-time-zone`` in
        ``connector_options`` to the session time zone; its default is UTC.

        :param path: Output directory URI supported by Flink's filesystem implementations.
        :param mode: ``"append"`` (default) adds files; ``"overwrite"`` replaces existing data
            in batch execution only.
        :param partition_by: Partition column name or non-empty list of names in directory order.
            Values are stored in Hive-style partition paths rather than in the JSON records.
        :param timestamp_format: Timestamp representation, ``"SQL"`` or ``"ISO-8601"``.
            The format default is currently ``"SQL"``.
        :param ignore_null_fields: Omit fields with null values from JSON objects. The format
            default is currently ``False``. This does not control Map entries with null keys;
            configure ``map-null-key.mode`` through ``format_options`` for those entries.
        :param decimal_as_plain_number: Encode DECIMAL values as plain numbers rather than
            scientific notation, retaining JSON numeric values. The format default is currently
            ``False``.
        :param sink_parallelism: Sink parallelism. The connector default is the upstream
            parallelism.
        :param sink_shuffle_by_partition: Shuffle rows by dynamic partition fields before writing.
            This can reduce the number of files but may cause data skew. The connector default
            is currently ``False``.
        :param auto_compaction: Automatically compact files in streaming execution after
            checkpoints complete. Files remain invisible until compaction finishes. The connector
            default is currently ``False``.
        :param compaction_file_size: Target file size for automatic compaction, for example
            ``"128mb"``. The connector default is the rolling policy file size.
        :param rolling_policy_file_size: Part file size threshold for rolling, not a hard upper
            bound. The connector default is currently ``"128mb"``.
        :param rolling_policy_rollover_interval: Part file open-time threshold. The connector
            default is currently ``"30min"``.
        :param rolling_policy_inactivity_interval: Part file inactivity threshold. The connector
            default is currently ``"30min"``.
        :param rolling_policy_check_interval: Interval for checking time-based rolling policies.
            The connector default is currently ``"1min"``.
        :param partition_commit_trigger: Partition commit trigger: ``"process-time"`` or
            ``"partition-time"``. The connector default is currently ``"process-time"``.
        :param partition_commit_delay: Delay before committing a partition. The connector
            default is currently ``"0s"``.
        :param partition_commit_policy_kind: Optional comma-separated policies, such as
            ``"success-file"`` or ``"custom"``. The ``metastore`` policy requires a Hive table.
        :param connector_options: Additional filesystem options with string keys and values.
            The ``connector``, ``path`` and ``format`` keys are reserved. Format options belong in
            ``format_options``. Explicit parameter and dictionary values must agree when both
            are set. Unspecified options are left to the connector factory.
        :param format_options: JSON format options with string values. Keys may include or omit
            the ``json.`` prefix, for example ``{"timestamp-format.standard": "ISO-8601"}``.
            ``None`` parameters leave dictionary values unchanged; conflicting explicit values
            are rejected.
        :param statement_set: Optional :class:`~pyflink.table.StatementSet` to stage the write in.
            It must use the same TableEnvironment as this DataFrame. Call its ``execute()``
            method to submit all staged writes together.
        :raises TypeError: If an argument has an invalid type.
        :raises ValueError: If the path is empty, the write mode is unsupported, the partition
            specification is empty or contains empty or duplicate names, or options conflict
            or contain reserved, empty or duplicate keys, or the statement set belongs to a
            different TableEnvironment.

        Example::

            >>> import pyflink.dataframe as pf
            >>> events = pf.from_records([(1, "login")], schema=["id", "event"])
            >>> events.write_json("file:///tmp/events")

        .. versionadded:: 2.4.0
        """
        from pyflink.dataframe.io import (
            _boolean_option,
            _build_filesystem_options,
            _parallelism_option,
        )

        options = _build_filesystem_options(
            path,
            "json",
            connector_parameters={
                "sink.parallelism": _parallelism_option(sink_parallelism),
                "sink.shuffle-by-partition.enable": _boolean_option(
                    sink_shuffle_by_partition, "sink_shuffle_by_partition"),
                "auto-compaction": _boolean_option(auto_compaction, "auto_compaction"),
                "compaction.file-size": compaction_file_size,
                "sink.rolling-policy.file-size": rolling_policy_file_size,
                "sink.rolling-policy.rollover-interval": rolling_policy_rollover_interval,
                "sink.rolling-policy.inactivity-interval": rolling_policy_inactivity_interval,
                "sink.rolling-policy.check-interval": rolling_policy_check_interval,
                "sink.partition-commit.trigger": partition_commit_trigger,
                "sink.partition-commit.delay": partition_commit_delay,
                "sink.partition-commit.policy.kind": partition_commit_policy_kind,
            },
            format_parameters={
                "timestamp-format.standard": timestamp_format,
                "encode.ignore-null-fields": _boolean_option(
                    ignore_null_fields, "ignore_null_fields"),
                "encode.decimal-as-plain-number": _boolean_option(
                    decimal_as_plain_number, "decimal_as_plain_number"),
            },
            extra_connector_options=connector_options,
            extra_format_options=format_options,
        )
        self._write(
            "filesystem",
            options,
            mode=mode,
            partition_by=partition_by,
            statement_set=statement_set,
        )

    @PublicEvolving()
    def write_generic(
        self,
        connector: str,
        *,
        options: Dict[str, str],
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        """
        Write this DataFrame using a connector and its raw Table connector options.

        The connector must be available through Flink's factory discovery mechanism. By default,
        the write is submitted immediately and waits for completion for local or MiniCluster
        execution. Passing ``statement_set`` stages the write without executing it.
        Sink columns are derived from this DataFrame's output schema.

        :param connector: Factory identifier used as the ``connector`` Table option.
        :param options: Connector options, excluding the reserved ``connector`` option.
        :param statement_set: Optional :class:`~pyflink.table.StatementSet` to stage the write in.
            It must use the same TableEnvironment as this DataFrame. Call its ``execute()``
            method to submit all staged writes together.
        :raises TypeError: If an argument has an invalid type.
        :raises ValueError: If the connector or an option key is empty, if ``options`` contains
            the reserved ``connector`` key, if Flink rejects the connector or its options, or
            if the statement set belongs to a different TableEnvironment.

        Example::

            >>> import pyflink.dataframe as pf
            >>> events = pf.from_records([(1, "login")], schema=["id", "event"])
            >>> events.write_generic(
            ...     "filesystem",
            ...     options={
            ...         "path": "file:///tmp/events",
            ...         "format": "csv",
            ...     },
            ... )

        .. versionadded:: 2.4.0
        """
        self._write(connector, options, statement_set=statement_set)

    def _write(
        self,
        connector: str,
        options: Dict[str, str],
        *,
        mode: str = "append",
        partition_by: Optional[Union[str, List[str]]] = None,
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        from pyflink.dataframe.errors import _raise_as_value_error
        from pyflink.dataframe.io import _build_generic_descriptor

        if not isinstance(mode, str):
            raise TypeError("mode must be a string")
        if mode not in ("append", "overwrite"):
            raise ValueError("mode must be 'append' or 'overwrite'")
        descriptor = _build_generic_descriptor(connector, options, partition_by=partition_by)
        try:
            self._execute_insert(descriptor, mode == "overwrite", statement_set=statement_set)
        except Exception as error:
            _raise_as_value_error(error)

    @PublicEvolving()
    def write_catalog_table(
        self,
        path: str,
        *,
        overwrite: bool = False,
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        """
        Write this DataFrame to a table registered in a catalog.

        ``path`` is ``table_name``, ``db_name.table_name``, or ``catalog_name.db_name.table_name``.
        Missing parts are resolved against the current catalog and database, see
        :func:`~pyflink.dataframe.use_catalog` and :func:`~pyflink.dataframe.use_database`. The
        write runs right away by default. On a local or MiniCluster setup the call blocks until
        the write is done. Passing ``statement_set`` stages the write without executing it.

        :param path: Path of the catalog table.
        :param overwrite: Whether existing data should be replaced, like ``INSERT OVERWRITE``.
            Not every connector supports overwriting.
        :param statement_set: Optional :class:`~pyflink.table.StatementSet` to stage the write in.
            It must use the same TableEnvironment as this DataFrame. Call its ``execute()``
            method to submit all staged writes together.
        :raises TypeError: If ``path`` is not a string, ``overwrite`` is not a bool, or
            ``statement_set`` is neither a StatementSet nor ``None``.
        :raises ValueError: If ``path`` is empty, malformed, or does not name a table, or if the
            DataFrame's columns do not match the table, or the statement set belongs to a
            different TableEnvironment.

        Example::

            >>> import pyflink.dataframe as pf
            >>> events = pf.from_records([(1, "login")], schema=["id", "event"])
            >>> events.write_catalog_table("my_catalog.my_database.events")
            >>> pf.use_catalog("my_catalog")
            >>> events.write_catalog_table("my_database.events", overwrite=True)

        .. versionadded:: 2.4.0
        """
        from pyflink.dataframe.catalog import _validate_name
        from pyflink.dataframe.errors import _raise_as_value_error

        _validate_name(path, "path")
        if not isinstance(overwrite, bool):
            raise TypeError("overwrite must be a bool")
        try:
            self._execute_insert(path, overwrite, statement_set=statement_set)
        except Exception as error:
            _raise_as_value_error(error)

    def _execute_insert(
        self,
        target: Union[str, TableDescriptor],
        overwrite: bool = False,
        *,
        statement_set: Optional[StatementSet] = None,
    ) -> None:
        if statement_set is not None:
            if not isinstance(statement_set, StatementSet):
                raise TypeError("statement_set must be a StatementSet or None")
            if self._table._t_env._j_tenv != statement_set._t_env._j_tenv:
                raise ValueError(
                    "DataFrame and statement_set must belong to the same TableEnvironment"
                )
            statement_set.add_insert(target, self._table, overwrite=overwrite)
            return

        result = self._table.execute_insert(target, overwrite=overwrite)
        execution_target = self._table._t_env.get_config().get(
            "execution.target", None
        )
        if execution_target in ("local", "minicluster"):
            result.wait()


@PublicEvolving()
class GroupedDataFrame:
    """
    A DataFrame grouped by one or more keys and ready for aggregation.

    Instances are created by :meth:`DataFrame.group_by`.

    .. versionadded:: 2.4.0
    """

    def __init__(self, dataframe: DataFrame, grouping_keys: List[Expression]):
        self._dataframe = dataframe
        self._grouping_keys = grouping_keys

    @PublicEvolving()
    def agg(self, *aggs: Expression, **named_aggs: Expression) -> DataFrame:
        """
        Aggregate the rows in each group.

        Grouping keys are included first in their supplied order, followed by positional
        aggregation expressions and then named aggregations. Each named aggregation is aliased to
        its keyword name.

        :param aggs: Aggregation expressions.
        :param named_aggs: Aggregation expressions keyed by their result column names.
        :return: A DataFrame containing the grouping keys and aggregation results.
        :raises TypeError: If an aggregation is not an expression.
        :raises ValueError: If no aggregations are provided.

        Example::

            >>> import pyflink.dataframe as pf
            >>> df = pf.from_records([
            ...     ("engineering", 10),
            ...     ("engineering", 20),
            ...     ("sales", 5),
            ... ], schema=["department", "amount"])
            >>> totals = df.group_by("department").agg(
            ...     pf.col("amount").sum.alias("total_amount"),
            ...     row_count=pf.col("amount").count,
            ... )
            >>> # totals schema: [department: STRING, total_amount: BIGINT,
            >>> #                 row_count: BIGINT NOT NULL]

        .. versionadded:: 2.4.0
        """
        aggregations = _normalize_aggregations(aggs, named_aggs)
        grouped_table = self._dataframe._table.group_by(*self._grouping_keys)
        return DataFrame(grouped_table.select(*self._grouping_keys, *aggregations))


# ======================== Internal Helpers ========================


def _normalize_join_type(how: str) -> str:
    if not isinstance(how, str):
        raise TypeError("how must be a string")
    aliases = {
        "inner": "inner",
        "left": "left",
        "right": "right",
        "full": "full",
        "outer": "full",
        "semi": "semi",
        "anti": "anti",
        "cross": "cross",
    }
    if how not in aliases:
        raise ValueError(
            'how must be one of "inner", "left", "right", "full", "outer", '
            '"semi", "anti", or "cross"'
        )
    return aliases[how]


def _normalize_join_keys(value, parameter_name: str) -> List[Union[str, Expression]]:
    if isinstance(value, (str, Expression)):
        return [value]
    if isinstance(value, list):
        if not value:
            raise ValueError("%s must not be empty" % parameter_name)
        if not all(isinstance(key, str) for key in value):
            raise TypeError(
                "%s must be a string, an expression, or a list of strings" % parameter_name
            )
        if len(set(value)) != len(value):
            raise ValueError("%s must not contain duplicate column names" % parameter_name)
        return value
    raise TypeError(
        "%s must be a string, an expression, or a list of strings" % parameter_name
    )


def _validate_join_columns(
    keys: List[Union[str, Expression]], columns: List[str], parameter_name: str
) -> None:
    for key in keys:
        if isinstance(key, str) and key not in columns:
            raise ValueError(
                "%s column '%s' does not exist, available columns: %s"
                % (parameter_name, key, columns)
            )


def _validate_join_column_conflicts(
    left_columns: List[str], right_columns: List[str], shared_keys: Set[str]
) -> None:
    conflicts = sorted((set(left_columns) & set(right_columns)) - shared_keys)
    if conflicts:
        raise ValueError(
            "join() found duplicate non-key columns %s; rename them with rename_columns() "
            "before joining" % conflicts
        )


def _prepare_join(
    left_table: Table,
    right_table: Table,
    on,
    left_on,
    right_on,
    *,
    validate_column_conflicts: bool,
) -> Tuple[Table, Table, Expression, Dict[str, str]]:
    left_columns = list(left_table.get_resolved_schema().get_column_names())
    right_columns = list(right_table.get_resolved_schema().get_column_names())

    if on is not None:
        if left_on is not None or right_on is not None:
            raise ValueError("on cannot be combined with left_on or right_on")
        if isinstance(on, Expression):
            if validate_column_conflicts:
                _validate_join_column_conflicts(left_columns, right_columns, set())
            return left_table, right_table, on, {}
        left_keys = _normalize_join_keys(on, "on")
        right_keys = list(left_keys)
        _validate_join_columns(left_keys, left_columns, "on")
        _validate_join_columns(right_keys, right_columns, "on")
    else:
        if left_on is None and right_on is None:
            raise ValueError("join() requires on or both left_on and right_on")
        if left_on is None or right_on is None:
            raise ValueError("left_on and right_on must be provided together")
        left_keys = _normalize_join_keys(left_on, "left_on")
        right_keys = _normalize_join_keys(right_on, "right_on")
        if len(left_keys) != len(right_keys):
            raise ValueError("left_on and right_on must have the same number of keys")
        _validate_join_columns(left_keys, left_columns, "left_on")
        _validate_join_columns(right_keys, right_columns, "right_on")

    shared_names = {
        left_key
        for left_key, right_key in zip(left_keys, right_keys)
        if isinstance(left_key, str)
        and isinstance(right_key, str)
        and left_key == right_key
    }
    if validate_column_conflicts:
        _validate_join_column_conflicts(left_columns, right_columns, shared_names)

    taken = set(left_columns) | set(right_columns)
    shared_keys: Dict[str, str] = {}
    right_rename_expressions: List[Expression] = []
    for name in left_columns:
        if name in shared_names:
            temporary_name = _unique_name("__pf_join_right_%s" % name, taken)
            taken.add(temporary_name)
            shared_keys[name] = temporary_name
            right_rename_expressions.append(table_col(name).alias(temporary_name))
    left_key_names: List[str] = []
    right_key_names: List[str] = []
    left_computed_keys: List[Expression] = []
    right_computed_keys: List[Expression] = []
    for index, (left_key, right_key) in enumerate(zip(left_keys, right_keys)):
        if isinstance(left_key, str):
            left_key_names.append(left_key)
        else:
            temporary_name = _unique_name("__pf_join_left_key_%d" % index, taken)
            taken.add(temporary_name)
            left_key_names.append(temporary_name)
            left_computed_keys.append(left_key.alias(temporary_name))

        if isinstance(right_key, str):
            right_key_names.append(shared_keys.get(right_key, right_key))
        else:
            temporary_name = _unique_name("__pf_join_right_key_%d" % index, taken)
            taken.add(temporary_name)
            right_key_names.append(temporary_name)
            right_computed_keys.append(right_key.alias(temporary_name))

    if left_computed_keys:
        left_table = left_table.add_columns(*left_computed_keys)
    if right_computed_keys:
        right_table = right_table.add_columns(*right_computed_keys)
    if right_rename_expressions:
        right_table = right_table.rename_columns(*right_rename_expressions)

    conditions = [
        table_col(left_name) == table_col(right_name)
        for left_name, right_name in zip(left_key_names, right_key_names)
    ]
    predicate = conditions[0] if len(conditions) == 1 else and_(*conditions)
    return (
        left_table,
        right_table,
        predicate,
        shared_keys,
    )


class _JoinSqlFactory:
    """Keep inline UDFs registered until the join SQL has been resolved."""

    def __init__(self, t_env: "TableEnvironment"):
        self._t_env = t_env
        self._functions: Dict[Any, str] = {}
        self._taken_names: Optional[Set[str]] = None
        self._cleanup = ExitStack()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return self._cleanup.__exit__(*exc_info)

    def serializeInlineFunction(self, definition):
        if definition not in self._functions:
            if self._taken_names is None:
                self._taken_names = {name.lower() for name in self._t_env.list_functions()}
            name = _unique_name("__pf_join_udf", self._taken_names)
            self._t_env._j_tenv.createTemporarySystemFunction(name, definition)
            self._cleanup.callback(self._t_env.drop_temporary_system_function, name)
            self._taken_names.add(name)
            self._functions[definition] = name
        return _quote_identifier(self._functions[definition])

    class Java:
        implements = ["org.apache.flink.table.expressions.SqlFactory"]


def _serialize_join_predicate(
    left_table: Table,
    right_table: Table,
    predicate: Expression,
    sql_factory: _JoinSqlFactory,
) -> Tuple[str, str, str]:
    left_alias, right_alias = "__pf_join_left", "__pf_join_right"
    operation_tree_builder = (
        left_table._j_table.getTableEnvironment().getOperationTreeBuilder()
    )
    gateway = get_gateway()
    query_operations = to_jarray(
        gateway.jvm.org.apache.flink.table.operations.QueryOperation,
        [
            left_table._j_table.getQueryOperation(),
            right_table._j_table.getQueryOperation(),
        ],
    )
    resolved_predicate = operation_tree_builder.resolveExpression(
        _get_java_expression(predicate), query_operations
    )

    aliases = gateway.jvm.java.util.HashMap()
    aliases.put(0, left_alias)
    aliases.put(1, right_alias)
    operation_expression_utils = (
        gateway.jvm.org.apache.flink.table.operations.utils.OperationExpressionsUtils
    )
    predicate_sql = operation_expression_utils.scopeReferencesWithAlias(
        aliases, resolved_predicate
    ).asSerializableString(sql_factory)
    return left_alias, right_alias, predicate_sql


def _build_regular_join_sql(
    left_table: Table,
    right_table: Table,
    predicate: Expression,
    left_output_columns: List[str],
    right_output_columns: List[str],
    shared_keys: Dict[str, str],
    join_type: str,
    sql_factory: _JoinSqlFactory,
) -> Table:
    left_alias, right_alias, predicate_sql = _serialize_join_predicate(
        left_table, right_table, predicate, sql_factory
    )
    left_alias_sql = _quote_identifier(left_alias)
    right_alias_sql = _quote_identifier(right_alias)

    projections = []
    for name in left_output_columns:
        left_field = "%s.%s" % (left_alias_sql, _quote_identifier(name))
        if name in shared_keys and join_type in ("right", "full"):
            right_field = "%s.%s" % (
                right_alias_sql,
                _quote_identifier(shared_keys[name]),
            )
            expression = (
                right_field
                if join_type == "right"
                else "COALESCE(%s, %s)" % (left_field, right_field)
            )
        else:
            expression = left_field
        projections.append("%s AS %s" % (expression, _quote_identifier(name)))
    projections.extend(
        "%s.%s AS %s"
        % (right_alias_sql, _quote_identifier(name), _quote_identifier(name))
        for name in right_output_columns
        if name not in shared_keys
    )

    join_keyword = {
        "inner": "INNER JOIN",
        "left": "LEFT OUTER JOIN",
        "right": "RIGHT OUTER JOIN",
        "full": "FULL OUTER JOIN",
    }[join_type]
    query = (
        "SELECT %s FROM %s AS %s %s %s AS %s ON %s"
        % (
            ", ".join(projections),
            _quote_identifier(str(left_table)),
            left_alias_sql,
            join_keyword,
            _quote_identifier(str(right_table)),
            right_alias_sql,
            predicate_sql,
        )
    )
    return left_table._t_env.sql_query(query)


def _build_semi_anti_join_sql(
    left_table: Table,
    right_table: Table,
    predicate: Expression,
    output_columns: List[str],
    join_type: str,
    sql_factory: _JoinSqlFactory,
) -> Table:
    left_alias, right_alias, predicate_sql = _serialize_join_predicate(
        left_table, right_table, predicate, sql_factory
    )
    left_alias_sql = _quote_identifier(left_alias)
    right_alias_sql = _quote_identifier(right_alias)
    select_list = ", ".join(
        "%s.%s" % (left_alias_sql, _quote_identifier(name)) for name in output_columns
    )
    left_source_sql = _quote_identifier(str(left_table))
    right_source_sql = _quote_identifier(str(right_table))
    existence_predicate = {"semi": "EXISTS", "anti": "NOT EXISTS"}[join_type]
    query = (
        "SELECT %s FROM %s AS %s WHERE %s ("
        "SELECT 1 FROM %s AS %s WHERE %s)"
        % (
            select_list,
            left_source_sql,
            left_alias_sql,
            existence_predicate,
            right_source_sql,
            right_alias_sql,
            predicate_sql,
        )
    )
    return left_table._t_env.sql_query(query)


def _normalize_subset(
    subset: Union[str, List[str], None], parameter_name: str = "subset"
) -> Optional[List[str]]:
    if subset is None:
        return None
    if isinstance(subset, str):
        return [subset]
    if isinstance(subset, (list, tuple)):
        if not subset:
            raise ValueError("%s must not be empty" % parameter_name)
        for name in subset:
            if not isinstance(name, str):
                raise TypeError(
                    "%s must be a string or a list of strings" % parameter_name
                )
        return list(subset)
    raise TypeError("%s must be a string or a list of strings" % parameter_name)


def _normalize_order_by(
    order_by: Union[str, Expression, List[Union[str, Expression]], None],
    parameter_name: str = "order_by",
) -> Optional[List[Union[str, Expression]]]:
    if order_by is None:
        return None

    values = order_by if isinstance(order_by, (list, tuple)) else [order_by]
    keys: List[Union[str, Expression]] = []
    for value in values:
        if isinstance(value, (str, Expression)):
            keys.append(value)
        else:
            raise TypeError(
                "%s must be a string, an expression, or a list or tuple of them" % parameter_name
            )

    if not keys:
        raise ValueError("%s must not be empty" % parameter_name)

    return keys


def _contains_ordering_expression(expression: Expression) -> bool:
    gateway = get_gateway()
    api_expression_utils = gateway.jvm.org.apache.flink.table.expressions.ApiExpressionUtils
    built_in_functions = gateway.jvm.org.apache.flink.table.functions.BuiltInFunctionDefinitions

    def contains_ordering(j_expression) -> bool:
        if api_expression_utils.isFunction(
            j_expression, built_in_functions.ORDER_ASC
        ) or api_expression_utils.isFunction(j_expression, built_in_functions.ORDER_DESC):
            return True
        return any(contains_ordering(child) for child in j_expression.getChildren())

    return contains_ordering(expression._j_expr.toExpr())


def _normalize_nulls_first(
    nulls_first: Union[bool, List[bool], None], order_len: int
) -> Optional[List[bool]]:
    if nulls_first is None:
        return None

    if isinstance(nulls_first, bool):
        values = [nulls_first] * order_len
    elif isinstance(nulls_first, (list, tuple)):
        for value in nulls_first:
            if not isinstance(value, bool):
                raise TypeError("nulls_first must be a boolean or a list of booleans")
        values = list(nulls_first)
    else:
        raise TypeError("nulls_first must be a boolean or a list of booleans")

    if len(values) != order_len:
        raise ValueError("nulls_first must have the same length as the sort keys")

    return values


def _normalize_descending(descending, order_len):
    if isinstance(descending, bool):
        return [descending] * order_len
    if isinstance(descending, (list, tuple)):
        if len(descending) != order_len:
            raise ValueError("descending must have the same length as the sort keys")
        if not all(isinstance(v, bool) for v in descending):
            raise TypeError("descending must be a boolean or a list of booleans")
        return list(descending)
    raise TypeError("descending must be a boolean or a list of booleans")


def _build_sort_sql(table, order_keys, descending_flags, nulls) -> Table:
    columns = table.get_resolved_schema().get_column_names()
    taken = set(columns)
    order_terms = []
    for index, key in enumerate(order_keys):
        direction = "DESC" if descending_flags[index] else "ASC"
        if isinstance(key, str):
            if key not in columns:
                raise ValueError(
                    "by column '%s' does not exist, available columns: %s" % (key, columns)
                )
            expression_sql = _quote_identifier(key)
        else:
            name = _unique_name("__pf_order_%d" % index, taken)
            taken.add(name)
            table = table.add_columns(key.alias(name))
            expression_sql = _quote_identifier(name)
        term = "%s %s" % (expression_sql, direction)
        if nulls[index] is not None:
            term += " NULLS FIRST" if nulls[index] else " NULLS LAST"
        order_terms.append(term)

    select_list = ", ".join(_quote_identifier(name) for name in columns)
    query = "SELECT %s FROM %s ORDER BY %s" % (
        select_list,
        _quote_identifier(str(table)),
        ", ".join(order_terms),
    )
    return table._t_env.sql_query(query)


def _build_rank_sql(table, partition_keys, order_keys, descending_flags, nulls, n) -> Table:
    columns = table.get_resolved_schema().get_column_names()
    for name in partition_keys:
        if name not in columns:
            raise ValueError(
                "partition_by column '%s' does not exist, available columns: %s" % (name, columns))
    taken = set(columns)

    order_terms = []
    for index, key in enumerate(order_keys):
        direction = "DESC" if descending_flags[index] else "ASC"
        if key is None:
            expr_sql = "PROCTIME()"
        elif isinstance(key, str):
            if key not in columns:
                raise ValueError(
                    "order_by column '%s' does not exist, available columns: %s" % (key, columns))
            expr_sql = _quote_identifier(key)
        else:
            name = _unique_name("__pf_order_%d" % index, taken)
            taken.add(name)
            table = table.add_columns(key.alias(name))
            expr_sql = _quote_identifier(name)
        term = expr_sql + " " + direction
        if nulls is not None and nulls[index] is not None:
            term += " NULLS FIRST" if nulls[index] else " NULLS LAST"
        order_terms.append(term)

    rank_column = _quote_identifier(_unique_name("__pf_row_number", taken))
    source = _quote_identifier(str(table))
    select_list = ", ".join(_quote_identifier(name) for name in columns)
    over_clause = "ORDER BY " + ", ".join(order_terms)
    if partition_keys:
        over_clause = (
            "PARTITION BY %s " % ", ".join(_quote_identifier(k) for k in partition_keys)
        ) + over_clause
    rank_filter = "= 1" if n == 1 else "<= %d" % n
    query = (
        "SELECT %s FROM (\n"
        "  SELECT *, ROW_NUMBER() OVER (%s) AS %s\n"
        "  FROM %s\n"
        ") WHERE %s %s"
        % (select_list, over_clause, rank_column, source, rank_column, rank_filter)
    )
    return table._t_env.sql_query(query)


def _unique_name(base: str, taken: Set[str]) -> str:
    name = base
    while name in taken:
        name += "_"
    return name


def _quote_identifier(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _resolve_window_time_column(on: Union[str, Expression]) -> Expression:
    if isinstance(on, str):
        return table_col(on)
    if isinstance(on, Expression):
        return on
    raise TypeError("on must be a column name or expression")


def _resolve_partition_columns(
    partition_by: Optional[Union[str, Expression, List[Union[str, Expression]]]]
) -> List[Expression]:
    if partition_by is None:
        return []
    if isinstance(partition_by, (str, Expression)):
        candidates: List[Union[str, Expression]] = [partition_by]
    else:
        candidates = partition_by
    columns: List[Expression] = []
    for candidate in candidates:
        if isinstance(candidate, str):
            columns.append(table_col(candidate))
        elif isinstance(candidate, Expression):
            columns.append(candidate)
        else:
            raise TypeError(
                "partition_by must be a column name, expression, or a list of them"
            )
    return columns


def _window_dataframe(
    table: Table,
    window_kind: str,
    time_col: Expression,
    *interval_exprs: Any,
    partition_cols: List[Expression] = [],
) -> "DataFrame":
    jvm = get_gateway().jvm
    kind = getattr(
        jvm.org.apache.flink.table.operations.WindowTableFunctionQueryOperation.WindowKind,
        window_kind,
    )
    intervals = jvm.java.util.ArrayList()
    for interval in interval_exprs:
        intervals.add(interval)
    partition_list = jvm.java.util.ArrayList()
    for partition_col in partition_cols:
        partition_list.add(partition_col._j_expr)
    operation_tree_builder = table._t_env._j_tenv.getOperationTreeBuilder()
    window_op = operation_tree_builder.windowTableFunction(
        kind,
        time_col._j_expr,
        intervals,
        partition_list,
        table._j_table.getQueryOperation(),
    )
    j_table = table._t_env._j_tenv.createTable(window_op)
    return DataFrame(Table(j_table, table._t_env))


def _to_interval_expression(value: Union["datetime.timedelta", Expression]) -> Any:
    if isinstance(value, datetime.timedelta):
        millis = value // datetime.timedelta(milliseconds=1)
        return (
            get_gateway()
            .jvm.org.apache.flink.table.expressions.ApiExpressionUtils.intervalOfMillis(
                millis
            )
        )
    if isinstance(value, Expression):
        return value._j_expr
    raise TypeError("interval must be a datetime.timedelta or Expression")


def _normalize_aggregations(
    aggs: Tuple[Expression, ...], named_aggs: Dict[str, Expression]
) -> List[Expression]:
    if not aggs and not named_aggs:
        raise ValueError("agg() requires at least one aggregation")

    aggregations: List[Expression] = []
    for aggregation in aggs:
        if not isinstance(aggregation, Expression):
            raise TypeError("agg() aggregations must be expressions")
        aggregations.append(aggregation)
    for name, aggregation in named_aggs.items():
        if not isinstance(aggregation, Expression):
            raise TypeError("agg() aggregations must be expressions")
        aggregations.append(aggregation.alias(name))
    return aggregations
