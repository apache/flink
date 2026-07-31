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

from typing import Any, Callable, List, Optional, Tuple, Union, overload

from pyflink.common import Row
from pyflink.dataframe.datatype import DataType
from pyflink.table.expression import Expression
from pyflink.table.expressions import (
    and_,
    call_sql,
    col as table_col,
    lit as table_lit,
)
from pyflink.table.table import Table
from pyflink.table.types import DataTypes as TableDataTypes
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ["DataFrame", "col", "lit"]


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
    if (
        table_data_type.nullable() == TableDataTypes.BIGINT()
        and isinstance(value, int)
        and not isinstance(value, bool)
        and -(1 << 31) <= value < (1 << 31)
    ):
        # Py4J sends Python integers in this range as java.lang.Integer, but a typed BIGINT
        # literal requires java.lang.Long. Match BIGINT independently of its nullability, then
        # cast a typed INT literal to the originally declared BIGINT type.
        return table_lit(value, TableDataTypes.INT().not_null()).cast(table_data_type)
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
