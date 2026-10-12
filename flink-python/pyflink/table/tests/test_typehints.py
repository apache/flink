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
import decimal
import sys
import unittest
from typing import Any, Awaitable, Coroutine, List, Optional, TypedDict, Union

import typing_extensions

from pyflink.table.types import DataTypes
from pyflink.table.typehints import _from_python_type, _unwrap_awaitable


class _Point(TypedDict):
    x: int
    y: int


class _Nested(TypedDict):
    name: str
    point: _Point


class _WithOptional(TypedDict):
    id: int
    label: Optional[str]


class _Partial(TypedDict, total=False):
    x: int
    y: int


# typing.TypedDict ignores theese markers when computing optional keys prior to python 3.11
class _WithNotRequired(TypedDict):
    id: int
    label: typing_extensions.NotRequired[str]
    note: typing_extensions.NotRequired[Optional[str]]


class _ExtensionsWithNotRequired(typing_extensions.TypedDict):
    id: int
    label: typing_extensions.NotRequired[str]
    note: typing_extensions.NotRequired[Optional[str]]


class _PartialWithRequired(TypedDict, total=False):
    id: typing_extensions.Required[int]
    label: str


class _ExtensionsPartialWithRequired(typing_extensions.TypedDict, total=False):
    id: typing_extensions.Required[int]
    label: str


class _LinkedNode(TypedDict):
    value: int
    next: Optional["_LinkedNode"]


class _TreeNode(TypedDict):
    # get_type_hints leaves forward references inside builtin generics unresolved before 3.10.
    children: List["_TreeNode"]


class _Employee(TypedDict):
    department: Optional["_Department"]


class _Department(TypedDict):
    manager: Optional[_Employee]


class FromPythonTypeTests(unittest.TestCase):

    def test_basic_types_infer_as_not_null(self):
        expected = {
            bool: DataTypes.BOOLEAN(),
            int: DataTypes.BIGINT(),
            float: DataTypes.DOUBLE(),
            str: DataTypes.STRING(),
            bytes: DataTypes.BYTES(),
            bytearray: DataTypes.BYTES(),
            decimal.Decimal: DataTypes.DECIMAL(38, 18),
            datetime.date: DataTypes.DATE(),
            datetime.time: DataTypes.TIME(3),
            datetime.datetime: DataTypes.TIMESTAMP(6),
        }
        for hint, data_type in expected.items():
            with self.subTest(hint=hint):
                self.assertEqual(_from_python_type(hint), data_type.not_null())

    def test_any_infers_as_nullable_string(self):
        self.assertEqual(_from_python_type(Any), DataTypes.STRING())

    def test_optional_widens_to_nullable(self):
        self.assertEqual(_from_python_type(Optional[int]), DataTypes.BIGINT())
        self.assertEqual(_from_python_type(Optional[str]), DataTypes.STRING())

    @unittest.skipIf(
        sys.version_info < (3, 10), "PEP 604 union types require Python 3.10 or later"
    )
    def test_pep_604_optional_widens_to_nullable(self):
        self.assertEqual(_from_python_type(int | None), DataTypes.BIGINT())

    def test_list_is_not_null_with_not_null_element(self):
        self.assertEqual(
            _from_python_type(list[int]),
            DataTypes.ARRAY(DataTypes.BIGINT().not_null()).not_null(),
        )

    def test_list_of_optional_has_nullable_element(self):
        self.assertEqual(
            _from_python_type(list[Optional[int]]),
            DataTypes.ARRAY(DataTypes.BIGINT()).not_null(),
        )

    def test_optional_list_is_nullable_with_not_null_element(self):
        self.assertEqual(
            _from_python_type(Optional[list[int]]),
            DataTypes.ARRAY(DataTypes.BIGINT().not_null()),
        )

    def test_dict_is_not_null_with_not_null_key_and_value(self):
        self.assertEqual(
            _from_python_type(dict[str, float]),
            DataTypes.MAP(
                DataTypes.STRING().not_null(), DataTypes.DOUBLE().not_null()
            ).not_null(),
        )

    def test_typed_dict_maps_to_not_null_row(self):
        self.assertEqual(
            _from_python_type(_Point),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("x", DataTypes.BIGINT().not_null()),
                    DataTypes.FIELD("y", DataTypes.BIGINT().not_null()),
                ]
            ).not_null(),
        )

    def test_nested_typed_dict_field_is_not_null_row(self):
        self.assertEqual(
            _from_python_type(_Nested),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("name", DataTypes.STRING().not_null()),
                    DataTypes.FIELD(
                        "point",
                        DataTypes.ROW(
                            [
                                DataTypes.FIELD("x", DataTypes.BIGINT().not_null()),
                                DataTypes.FIELD("y", DataTypes.BIGINT().not_null()),
                            ]
                        ).not_null(),
                    ),
                ]
            ).not_null(),
        )

    def test_typed_dict_optional_field_is_nullable(self):
        self.assertEqual(
            _from_python_type(_WithOptional),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT().not_null()),
                    DataTypes.FIELD("label", DataTypes.STRING()),
                ]
            ).not_null(),
        )

    def test_typed_dict_not_required_field_is_nullable(self):
        for hint in (_WithNotRequired, _ExtensionsWithNotRequired):
            with self.subTest(hint=hint):
                self.assertEqual(
                    _from_python_type(hint),
                    DataTypes.ROW(
                        [
                            DataTypes.FIELD("id", DataTypes.BIGINT().not_null()),
                            DataTypes.FIELD("label", DataTypes.STRING()),
                            DataTypes.FIELD("note", DataTypes.STRING()),
                        ]
                    ).not_null(),
                )

    def test_non_total_typed_dict_fields_are_nullable(self):
        self.assertEqual(
            _from_python_type(_Partial),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("x", DataTypes.BIGINT()),
                    DataTypes.FIELD("y", DataTypes.BIGINT()),
                ]
            ).not_null(),
        )

    def test_required_field_in_non_total_typed_dict_is_not_null(self):
        for hint in (_PartialWithRequired, _ExtensionsPartialWithRequired):
            with self.subTest(hint=hint):
                self.assertEqual(
                    _from_python_type(hint),
                    DataTypes.ROW(
                        [
                            DataTypes.FIELD("id", DataTypes.BIGINT().not_null()),
                            DataTypes.FIELD("label", DataTypes.STRING()),
                        ]
                    ).not_null(),
                )

    def test_typed_dict_resolves_in_container_position(self):
        self.assertEqual(
            _from_python_type(list[_Point]),
            DataTypes.ARRAY(
                DataTypes.ROW(
                    [
                        DataTypes.FIELD("x", DataTypes.BIGINT().not_null()),
                        DataTypes.FIELD("y", DataTypes.BIGINT().not_null()),
                    ]
                ).not_null()
            ).not_null(),
        )

    def test_self_referencing_typed_dict_raises(self):
        for hint, name in (
            (_LinkedNode, "_LinkedNode"),
            (_TreeNode, "_TreeNode"),
            (_Employee, "_Employee"),
            (list[_Department], "_Department"),
        ):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(TypeError, f"'{name}'.*references itself"):
                    _from_python_type(hint)

    def test_ambiguous_union_raises(self):
        with self.assertRaises(TypeError):
            _from_python_type(Union[int, str])

    def test_unsupported_hints_raise(self):
        for hint in (complex, list, dict[str]):
            with self.subTest(hint=hint):
                with self.assertRaises(TypeError):
                    _from_python_type(hint)


class UnwrapAwaitableTests(unittest.TestCase):

    def test_unwraps_coroutine_and_awaitable(self):
        self.assertIs(_unwrap_awaitable(Coroutine[Any, Any, int]), int)
        self.assertIs(_unwrap_awaitable(Awaitable[str]), str)

    def test_passes_through_non_awaitable(self):
        self.assertIs(_unwrap_awaitable(int), int)
        self.assertEqual(_unwrap_awaitable(Optional[int]), Optional[int])


if __name__ == "__main__":
    unittest.main()
