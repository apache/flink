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

import collections
import datetime
import decimal
import sys
import unittest
from dataclasses import InitVar, dataclass, field
from typing import (
    Any,
    Awaitable,
    ClassVar,
    Coroutine,
    Generic,
    List,
    NamedTuple,
    Optional,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

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


class _EmptyTypedDict(TypedDict):
    pass


@dataclass
class _PointDataclass:
    x: int
    y: int


@dataclass
class _DataclassWithDefaults:
    id: int = 0
    label: Optional[str] = None


@dataclass
class _BaseDataclass:
    id: int


@dataclass
class _DerivedDataclassWithNonFields(_BaseDataclass):
    registry: ClassVar[int] = 0
    seed: InitVar[int] = 0
    label: str = field(default="", init=False)


class _PointNamedTuple(NamedTuple):
    x: int
    y: int


class _NamedTupleWithDefaults(NamedTuple):
    id: int = 0
    label: Optional[str] = None


@dataclass
class _QuotedAnnotationsDataclass:
    point: "_PointNamedTuple"
    scores: "list[int]"
    label: "Optional[str]"


@dataclass
class _Team:
    lead: Optional["_Member"]


class _Member(TypedDict):
    team: Optional[_Team]


@dataclass
class _EmptyDataclass:
    pass


@dataclass
class _ClassVarOnlyDataclass:
    registry: ClassVar[int] = 0


class _EmptyNamedTuple(NamedTuple):
    pass


_UntypedNamedTuple = collections.namedtuple("_UntypedNamedTuple", ["x", "y"])

_T = TypeVar("_T")


@dataclass
class _GenericBox(Generic[_T]):
    item: _T


@dataclass
class _IntBox(_GenericBox[int]):
    pass


def _point_row():
    return DataTypes.ROW(
        [
            DataTypes.FIELD("x", DataTypes.BIGINT().not_null()),
            DataTypes.FIELD("y", DataTypes.BIGINT().not_null()),
        ]
    ).not_null()


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

    def test_composites_map_to_not_null_row(self):
        for hint in (_Point, _PointDataclass, _PointNamedTuple):
            with self.subTest(hint=hint):
                self.assertEqual(_from_python_type(hint), _point_row())

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
            _from_python_type(list[_Point]), DataTypes.ARRAY(_point_row()).not_null()
        )

    def test_self_referencing_composite_raises(self):
        for hint, name in (
            (_LinkedNode, "_LinkedNode"),
            (_TreeNode, "_TreeNode"),
            (_Employee, "_Employee"),
            (list[_Department], "_Department"),
            (_Team, "_Team"),
        ):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(TypeError, f"'{name}'.*refers to itself"):
                    _from_python_type(hint)

    def test_composite_reused_in_sibling_fields_is_not_self_reference(self):
        self.assertEqual(
            _from_python_type(Tuple[_PointDataclass, _PointDataclass]),
            DataTypes.ROW(
                [DataTypes.FIELD("_1", _point_row()), DataTypes.FIELD("_2", _point_row())]
            ).not_null(),
        )

    def test_only_optional_widens_composite_field_to_nullable(self):
        expected = DataTypes.ROW(
            [
                DataTypes.FIELD("id", DataTypes.BIGINT().not_null()),
                DataTypes.FIELD("label", DataTypes.STRING()),
            ]
        ).not_null()
        for hint in (_DataclassWithDefaults, _NamedTupleWithDefaults):
            with self.subTest(hint=hint):
                self.assertEqual(_from_python_type(hint), expected)

    def test_dataclass_row_follows_dataclass_fields(self):
        self.assertEqual(
            _from_python_type(_DerivedDataclassWithNonFields),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("id", DataTypes.BIGINT().not_null()),
                    DataTypes.FIELD("label", DataTypes.STRING().not_null()),
                ]
            ).not_null(),
        )

    def test_dataclass_quoted_field_annotations_resolve(self):
        self.assertEqual(
            _from_python_type(_QuotedAnnotationsDataclass),
            DataTypes.ROW(
                [
                    DataTypes.FIELD("point", _point_row()),
                    DataTypes.FIELD(
                        "scores", DataTypes.ARRAY(DataTypes.BIGINT().not_null()).not_null()
                    ),
                    DataTypes.FIELD("label", DataTypes.STRING()),
                ]
            ).not_null(),
        )

    def test_unresolved_forward_reference_raises(self):
        for hint, advises_typing_list in (
            (List["_PointDataclass"], False),
            (list["_PointDataclass"], sys.version_info < (3, 11)),
        ):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(
                    TypeError, "unresolved forward reference '_PointDataclass'"
                ) as raised:
                    _from_python_type(hint)
                self.assertEqual("typing.List" in str(raised.exception), advises_typing_list)

    def test_unresolvable_field_annotation_raises_type_error_naming_composite(self):
        @dataclass
        class LocalInner:
            x: int

        @dataclass
        class LocalOuter:
            inner: "LocalInner"

        with self.assertRaisesRegex(TypeError, "'LocalOuter'.*LocalInner"):
            _from_python_type(LocalOuter)

    def test_untyped_named_tuple_raises(self):
        with self.assertRaisesRegex(TypeError, "'_UntypedNamedTuple'.*typing.NamedTuple"):
            _from_python_type(_UntypedNamedTuple)

    def test_tuple_maps_to_row_with_positional_field_names(self):
        expected = DataTypes.ROW(
            [
                DataTypes.FIELD("_1", DataTypes.BIGINT().not_null()),
                DataTypes.FIELD("_2", DataTypes.STRING()),
            ]
        ).not_null()
        for hint in (tuple[int, Optional[str]], Tuple[int, Optional[str]]):
            with self.subTest(hint=hint):
                self.assertEqual(_from_python_type(hint), expected)

    def test_tuple_without_fixed_element_types_raises(self):
        for hint, message in (
            (tuple, "tuple without type arguments"),
            (Tuple, "tuple without type arguments"),
            (tuple[int, ...], "fixed number of elements"),
            (Tuple[int, ...], "fixed number of elements"),
        ):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(TypeError, message):
                    _from_python_type(hint)

    def test_zero_field_composite_raises(self):
        for hint in (
            tuple[()],
            Tuple[()],
            _EmptyDataclass,
            _ClassVarOnlyDataclass,
            _EmptyNamedTuple,
            _EmptyTypedDict,
        ):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(TypeError, "has no fields"):
                    _from_python_type(hint)

    def test_generic_composite_raises(self):
        for hint in (_GenericBox, _GenericBox[int]):
            with self.subTest(hint=hint):
                with self.assertRaisesRegex(
                    TypeError, "_GenericBox.*Generic composite types are not supported"
                ):
                    _from_python_type(hint)

    def test_type_variable_field_raises(self):
        with self.assertRaisesRegex(TypeError, "type variable '~_T'"):
            _from_python_type(_IntBox)

    def test_dataclass_instance_raises(self):
        with self.assertRaisesRegex(TypeError, "Cannot infer DataType from type hint"):
            _from_python_type(_PointDataclass(1, 2))

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
