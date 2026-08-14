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

from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    TypeVar,
    Union,
    overload,
)

if TYPE_CHECKING:
    import pandas
    from pyflink.table.table_schema import TableSchema

from pyflink.common import Row
from pyflink.dataframe.datatype import DataType
from pyflink.table.expression import Expression
from pyflink.table.expressions import (
    and_,
    call_sql,
    col as table_col,
    if_then_else,
    is_nan,
    lit as table_lit,
)
from pyflink.table.table import Table
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["DataFrame", "GroupedDataFrame", "col", "lit"]

T = TypeVar("T")


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
            >>> result = df.with_column(
            ...     "total", lambda current: current["left"] + current["right"]
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
    def write_generic(self, connector: str, *, options: Dict[str, str]) -> None:
        """
        Write this DataFrame using a connector and its raw Table connector options.

        The connector must be available through Flink's factory discovery mechanism. The write is
        submitted immediately and waits for completion when using local or MiniCluster execution.
        Sink columns are derived from this DataFrame's output schema.

        :param connector: Factory identifier used as the ``connector`` Table option.
        :param options: Connector options, excluding the reserved ``connector`` option.
        :raises TypeError: If an argument has an invalid type.
        :raises ValueError: If the connector or an option key is empty, or if ``options`` contains
            the reserved ``connector`` key.

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
        from pyflink.dataframe.io import _build_generic_descriptor

        descriptor = _build_generic_descriptor(connector, options)
        result = self._table.execute_insert(descriptor)
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
