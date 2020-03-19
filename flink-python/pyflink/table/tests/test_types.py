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

import array
import ctypes
import datetime
import pickle
import sys
import tempfile
import unittest

from pyflink.serializers import BatchedSerializer, PickleSerializer

from pyflink.java_gateway import get_gateway
from pyflink.table.types import (_infer_schema_from_data, _infer_type,
                                 _array_signed_int_typecode_ctype_mappings,
                                 _array_unsigned_int_typecode_ctype_mappings,
                                 _array_type_mappings, _merge_type,
                                 _create_type_verifier, UserDefinedType, DataTypes, Row, RowField,
                                 RowType, ArrayType, BigIntType, VarCharType, MapType, DataType,
                                 _to_java_type, _from_java_type, ZonedTimestampType,
                                 LocalZonedTimestampType)


class ExamplePointUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sql_type(cls):
        return DataTypes.ARRAY(DataTypes.DOUBLE(False))

    @classmethod
    def module(cls):
        return 'pyflink.table.tests.test_types'

    @classmethod
    def java_udt(cls):
        return 'org.apache.flink.table.types.python.ExamplePointUserDefinedType'

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return ExamplePoint(datum[0], datum[1])


class ExamplePoint:
    """
    An example class to demonstrate UDT in Java, and Python.
    """

    __UDT__ = ExamplePointUDT()

    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __repr__(self):
        return "ExamplePoint(%s,%s)" % (self.x, self.y)

    def __str__(self):
        return "(%s,%s)" % (self.x, self.y)

    def __eq__(self, other):
        return isinstance(other, self.__class__) and other.x == self.x and other.y == self.y


class PythonOnlyUDT(UserDefinedType):
    """
    User-defined type (UDT) for ExamplePoint.
    """

    @classmethod
    def sql_type(cls):
        return DataTypes.ARRAY(DataTypes.DOUBLE(False))

    @classmethod
    def module(cls):
        return '__main__'

    def serialize(self, obj):
        return [obj.x, obj.y]

    def deserialize(self, datum):
        return PythonOnlyPoint(datum[0], datum[1])


class PythonOnlyPoint(ExamplePoint):
    """
    An example class to demonstrate UDT in only Python
    """
    __UDT__ = PythonOnlyUDT()


class UTCOffsetTimezone(datetime.tzinfo):
    """
    Specifies timezone in UTC offset
    """

    def __init__(self, offset=0):
        self.OFFSET = datetime.timedelta(hours=offset)

    def utcoffset(self, dt):
        return self.OFFSET

    def dst(self, dt):
        return self.OFFSET


class TypesTests(unittest.TestCase):

    def test_infer_schema(self):
        from decimal import Decimal

        class A(object):
            def __init__(self):
                self.a = 1

        from collections import namedtuple
        Point = namedtuple('Point', 'x y')

        data = [
            True,
            1,
            "a",
            u"a",
            datetime.date(1970, 1, 1),
            datetime.time(0, 0, 0),
            datetime.datetime(1970, 1, 1, 0, 0),
            1.0,
            array.array("d", [1]),
            [1],
            (1,),
            Point(1.0, 5.0),
            {"a": 1},
            bytearray(1),
            Decimal(1),
            Row(a=1),
            Row("a")(1),
            A(),
        ]

        expected = [
            'BooleanType(true)',
            'BigIntType(true)',
            'VarCharType(2147483647, true)',
            'VarCharType(2147483647, true)',
            'DateType(true)',
            'TimeType(0, true)',
            'LocalZonedTimestampType(6, true)',
            'DoubleType(true)',
            "ArrayType(DoubleType(false), true)",
            "ArrayType(BigIntType(true), true)",
            'RowType(RowField(_1, BigIntType(true), ...))',
            'RowType(RowField(x, DoubleType(true), ...),RowField(y, DoubleType(true), ...))',
            'MapType(VarCharType(2147483647, false), BigIntType(true), true)',
            'VarBinaryType(2147483647, true)',
            'DecimalType(38, 18, true)',
            'RowType(RowField(a, BigIntType(true), ...))',
            'RowType(RowField(a, BigIntType(true), ...))',
            'RowType(RowField(a, BigIntType(true), ...))',
        ]

        schema = _infer_schema_from_data([data])
        self.assertEqual(expected, [repr(f.data_type) for f in schema.fields])

    def test_infer_schema_nulltype(self):
        elements = [Row(c1=[], c2={}, c3=None),
                    Row(c1=[Row(a=1, b='s')], c2={"key": Row(c=1.0, d="2")}, c3="")]
        schema = _infer_schema_from_data(elements)
        self.assertTrue(isinstance(schema, RowType))
        self.assertEqual(3, len(schema.fields))

        # first column is array
        self.assertTrue(isinstance(schema.fields[0].data_type, ArrayType))

        # element type of first column is struct
        self.assertTrue(isinstance(schema.fields[0].data_type.element_type, RowType))

        self.assertTrue(isinstance(schema.fields[0].data_type.element_type.fields[0].data_type,
                                   BigIntType))
        self.assertTrue(isinstance(schema.fields[0].data_type.element_type.fields[1].data_type,
                                   VarCharType))

        # second column is map
        self.assertTrue(isinstance(schema.fields[1].data_type, MapType))
        self.assertTrue(isinstance(schema.fields[1].data_type.key_type, VarCharType))
        self.assertTrue(isinstance(schema.fields[1].data_type.value_type, RowType))

        # third column is varchar
        self.assertTrue(isinstance(schema.fields[2].data_type, VarCharType))

    def test_infer_schema_not_enough_names(self):
        schema = _infer_schema_from_data([["a", "b"]], ["col1"])
        self.assertTrue(schema.names, ['col1', '_2'])

    def test_infer_schema_fails(self):
        with self.assertRaises(TypeError):
            _infer_schema_from_data([[1, 1], ["x", 1]], names=["a", "b"])

    def test_infer_nested_schema(self):
        NestedRow = Row("f1", "f2")
        data1 = [NestedRow([1, 2], {"row1": 1.0}), NestedRow([2, 3], {"row2": 2.0})]
        schema1 = _infer_schema_from_data(data1)
        expected1 = [
            'ArrayType(BigIntType(true), true)',
            'MapType(VarCharType(2147483647, false), DoubleType(true), true)'
        ]
        self.assertEqual(expected1, [repr(f.data_type) for f in schema1.fields])

        data2 = [NestedRow([[1, 2], [2, 3]], [1, 2]), NestedRow([[2, 3], [3, 4]], [2, 3])]
        schema2 = _infer_schema_from_data(data2)
        expected2 = [
            'ArrayType(ArrayType(BigIntType(true), true), true)',
            'ArrayType(BigIntType(true), true)'
        ]
        self.assertEqual(expected2, [repr(f.data_type) for f in schema2.fields])

    def test_convert_row_to_dict(self):
        row = Row(l=[Row(a=1, b='s')], d={"key": Row(c=1.0, d="2")})
        self.assertEqual(1, row.as_dict()['l'][0].a)
        self.assertEqual(1.0, row.as_dict()['d']['key'].c)

    def test_udt(self):
        p = ExamplePoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), ExamplePointUDT())
        _create_type_verifier(ExamplePointUDT())(ExamplePoint(1.0, 2.0))
        self.assertRaises(ValueError, lambda: _create_type_verifier(ExamplePointUDT())([1.0, 2.0]))

        p = PythonOnlyPoint(1.0, 2.0)
        self.assertEqual(_infer_type(p), PythonOnlyUDT())
        _create_type_verifier(PythonOnlyUDT())(PythonOnlyPoint(1.0, 2.0))
        self.assertRaises(ValueError, lambda: _create_type_verifier(PythonOnlyUDT())([1.0, 2.0]))

    def test_nested_udt_in_df(self):
        expected_schema = DataTypes.ROW() \
            .add("_1", DataTypes.BIGINT()).add("_2", DataTypes.ARRAY(PythonOnlyUDT()))
        data = (1, [PythonOnlyPoint(float(1), float(2))])
        self.assertEqual(expected_schema, _infer_type(data))

        expected_schema = DataTypes.ROW().add("_1", DataTypes.BIGINT()).add(
            "_2", DataTypes.MAP(DataTypes.BIGINT(False), PythonOnlyUDT()))
        p = (1, {1: PythonOnlyPoint(1, float(2))})
        self.assertEqual(expected_schema, _infer_type(p))

    def test_struct_type(self):
        row1 = DataTypes.ROW().add("f1", DataTypes.STRING(nullable=True)) \
            .add("f2", DataTypes.STRING(nullable=True))
        row2 = DataTypes.ROW([DataTypes.FIELD("f1", DataTypes.STRING(nullable=True)),
                              DataTypes.FIELD("f2", DataTypes.STRING(nullable=True), None)])
        self.assertEqual(row1.field_names(), row2.names)
        self.assertEqual(row1, row2)

        row1 = DataTypes.ROW().add("f1", DataTypes.STRING(nullable=True)) \
            .add("f2", DataTypes.STRING(nullable=True))
        row2 = DataTypes.ROW([DataTypes.FIELD("f1", DataTypes.STRING(nullable=True))])
        self.assertNotEqual(row1.field_names(), row2.names)
        self.assertNotEqual(row1, row2)

        row1 = (DataTypes.ROW().add(DataTypes.FIELD("f1", DataTypes.STRING(nullable=True)))
                .add("f2", DataTypes.STRING(nullable=True)))
        row2 = DataTypes.ROW([DataTypes.FIELD("f1", DataTypes.STRING(nullable=True)),
                              DataTypes.FIELD("f2", DataTypes.STRING(nullable=True))])
        self.assertEqual(row1.field_names(), row2.names)
        self.assertEqual(row1, row2)

        row1 = (DataTypes.ROW().add(DataTypes.FIELD("f1", DataTypes.STRING(nullable=True)))
                .add("f2", DataTypes.STRING(nullable=True)))
        row2 = DataTypes.ROW([DataTypes.FIELD("f1", DataTypes.STRING(nullable=True))])
        self.assertNotEqual(row1.field_names(), row2.names)
        self.assertNotEqual(row1, row2)

        # Catch exception raised during improper construction
        self.assertRaises(ValueError, lambda: DataTypes.ROW().add("name"))

        row1 = DataTypes.ROW().add("f1", DataTypes.STRING(nullable=True)) \
            .add("f2", DataTypes.STRING(nullable=True))
        for field in row1:
            self.assertIsInstance(field, RowField)

        row1 = DataTypes.ROW().add("f1", DataTypes.STRING(nullable=True)) \
            .add("f2", DataTypes.STRING(nullable=True))
        self.assertEqual(len(row1), 2)

        row1 = DataTypes.ROW().add("f1", DataTypes.STRING(nullable=True)) \
            .add("f2", DataTypes.STRING(nullable=True))
        self.assertIs(row1["f1"], row1.fields[0])
        self.assertIs(row1[0], row1.fields[0])
        self.assertEqual(row1[0:1], DataTypes.ROW(row1.fields[0:1]))
        self.assertRaises(KeyError, lambda: row1["f9"])
        self.assertRaises(IndexError, lambda: row1[9])
        self.assertRaises(TypeError, lambda: row1[9.9])

    def test_infer_bigint_type(self):
        longrow = [Row(f1='a', f2=100000000000000)]
        schema = _infer_schema_from_data(longrow)
        self.assertEqual(DataTypes.BIGINT(), schema.fields[1].data_type)
        self.assertEqual(DataTypes.BIGINT(), _infer_type(1))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 10))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 20))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 31 - 1))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 31))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 61))
        self.assertEqual(DataTypes.BIGINT(), _infer_type(2 ** 71))

    def test_merge_type(self):
        self.assertEqual(_merge_type(DataTypes.BIGINT(), DataTypes.NULL()), DataTypes.BIGINT())
        self.assertEqual(_merge_type(DataTypes.NULL(), DataTypes.BIGINT()), DataTypes.BIGINT())

        self.assertEqual(_merge_type(DataTypes.BIGINT(), DataTypes.BIGINT()), DataTypes.BIGINT())

        self.assertEqual(_merge_type(
            DataTypes.ARRAY(DataTypes.BIGINT()),
            DataTypes.ARRAY(DataTypes.BIGINT())
        ), DataTypes.ARRAY(DataTypes.BIGINT()))
        with self.assertRaises(TypeError):
            _merge_type(DataTypes.ARRAY(DataTypes.BIGINT()), DataTypes.ARRAY(DataTypes.DOUBLE()))

        self.assertEqual(_merge_type(
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())
        ), DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.BIGINT()))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT()),
                DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE()))

        self.assertEqual(_merge_type(
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.BIGINT()),
                           DataTypes.FIELD('f2', DataTypes.STRING())]),
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.BIGINT()),
                           DataTypes.FIELD('f2', DataTypes.STRING())])
        ), DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.BIGINT()),
                          DataTypes.FIELD('f2', DataTypes.STRING())]))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.BIGINT()),
                               DataTypes.FIELD('f2', DataTypes.STRING())]),
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.DOUBLE()),
                               DataTypes.FIELD('f2', DataTypes.STRING())]))

        self.assertEqual(_merge_type(
            DataTypes.ROW([DataTypes.FIELD(
                'f1', DataTypes.ROW([DataTypes.FIELD('f2', DataTypes.BIGINT())]))]),
            DataTypes.ROW([DataTypes.FIELD(
                'f1', DataTypes.ROW([DataTypes.FIELD('f2', DataTypes.BIGINT())]))])
        ), DataTypes.ROW([DataTypes.FIELD(
            'f1', DataTypes.ROW([DataTypes.FIELD('f2', DataTypes.BIGINT())]))]))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ROW(
                    [DataTypes.FIELD('f2', DataTypes.BIGINT())]))]),
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ROW(
                    [DataTypes.FIELD('f2', DataTypes.STRING())]))]))

        self.assertEqual(_merge_type(
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(DataTypes.BIGINT())),
                           DataTypes.FIELD('f2', DataTypes.STRING())]),
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(DataTypes.BIGINT())),
                           DataTypes.FIELD('f2', DataTypes.STRING())])
        ), DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(DataTypes.BIGINT())),
                          DataTypes.FIELD('f2', DataTypes.STRING())]))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.ROW([
                    DataTypes.FIELD('f1', DataTypes.ARRAY(DataTypes.BIGINT())),
                    DataTypes.FIELD('f2', DataTypes.STRING())]),
                DataTypes.ROW([
                    DataTypes.FIELD('f1', DataTypes.ARRAY(DataTypes.DOUBLE())),
                    DataTypes.FIELD('f2', DataTypes.STRING())]))

        self.assertEqual(_merge_type(
            DataTypes.ROW([
                DataTypes.FIELD('f1', DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                DataTypes.FIELD('f2', DataTypes.STRING())]),
            DataTypes.ROW([
                DataTypes.FIELD('f1', DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                DataTypes.FIELD('f2', DataTypes.STRING())])
        ), DataTypes.ROW([
            DataTypes.FIELD('f1', DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
            DataTypes.FIELD('f2', DataTypes.STRING())]))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.ROW([
                    DataTypes.FIELD('f1', DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                    DataTypes.FIELD('f2', DataTypes.STRING())]),
                DataTypes.ROW([
                    DataTypes.FIELD('f1', DataTypes.MAP(DataTypes.STRING(), DataTypes.DOUBLE())),
                    DataTypes.FIELD('f2', DataTypes.STRING())]))

        self.assertEqual(_merge_type(
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())))]),
            DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(
                DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())))])
        ), DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(
            DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())))]))
        with self.assertRaises(TypeError):
            _merge_type(
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(
                    DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())))]),
                DataTypes.ROW([DataTypes.FIELD('f1', DataTypes.ARRAY(
                    DataTypes.MAP(DataTypes.DOUBLE(), DataTypes.BIGINT())))])
            )

    def test_array_types(self):
        # This test need to make sure that the Scala type selected is at least
        # as large as the python's types. This is necessary because python's
        # array types depend on C implementation on the machine. Therefore there
        # is no machine independent correspondence between python's array types
        # and Scala types.
        # See: https://docs.python.org/2/library/array.html

        def assert_collect_success(typecode, value, element_type):
            self.assertEqual(element_type,
                             str(_infer_type(array.array(typecode, [value])).element_type))

        # supported string types
        #
        # String types in python's array are "u" for Py_UNICODE and "c" for char.
        # "u" will be removed in python 4, and "c" is not supported in python 3.
        supported_string_types = []
        if sys.version_info[0] < 4:
            supported_string_types += ['u']
            # test unicode
            assert_collect_success('u', u'a', 'CHAR')

        # supported float and double
        #
        # Test max, min, and precision for float and double, assuming IEEE 754
        # floating-point format.
        supported_fractional_types = ['f', 'd']
        assert_collect_success('f', ctypes.c_float(1e+38).value, 'FLOAT')
        assert_collect_success('f', ctypes.c_float(1e-38).value, 'FLOAT')
        assert_collect_success('f', ctypes.c_float(1.123456).value, 'FLOAT')
        assert_collect_success('d', sys.float_info.max, 'DOUBLE')
        assert_collect_success('d', sys.float_info.min, 'DOUBLE')
        assert_collect_success('d', sys.float_info.epsilon, 'DOUBLE')

        def get_int_data_type(size):
            if size <= 8:
                return "TINYINT"
            if size <= 16:
                return "SMALLINT"
            if size <= 32:
                return "INT"
            if size <= 64:
                return "BIGINT"

        # supported signed int types
        #
        # The size of C types changes with implementation, we need to make sure
        # that there is no overflow error on the platform running this test.
        supported_signed_int_types = list(
            set(_array_signed_int_typecode_ctype_mappings.keys()).intersection(
                set(_array_type_mappings.keys())))
        for t in supported_signed_int_types:
            ctype = _array_signed_int_typecode_ctype_mappings[t]
            max_val = 2 ** (ctypes.sizeof(ctype) * 8 - 1)
            assert_collect_success(t, max_val - 1, get_int_data_type(ctypes.sizeof(ctype) * 8))
            assert_collect_success(t, -max_val, get_int_data_type(ctypes.sizeof(ctype) * 8))

        # supported unsigned int types
        #
        # JVM does not have unsigned types. We need to be very careful to make
        # sure that there is no overflow error.
        supported_unsigned_int_types = list(
            set(_array_unsigned_int_typecode_ctype_mappings.keys()).intersection(
                set(_array_type_mappings.keys())))
        for t in supported_unsigned_int_types:
            ctype = _array_unsigned_int_typecode_ctype_mappings[t]
            max_val = 2 ** (ctypes.sizeof(ctype) * 8 - 1)
            assert_collect_success(t, max_val, get_int_data_type(ctypes.sizeof(ctype) * 8 + 1))

        # all supported types
        #
        # Make sure the types tested above:
        # 1. are all supported types
        # 2. cover all supported types
        supported_types = (supported_string_types +
                           supported_fractional_types +
                           supported_signed_int_types +
                           supported_unsigned_int_types)
        self.assertEqual(set(supported_types), set(_array_type_mappings.keys()))

        # all unsupported types
        #
        # Keys in _array_type_mappings is a complete list of all supported types,
        # and types not in _array_type_mappings are considered unsupported.
        all_types = set(array.typecodes)
        unsupported_types = all_types - set(supported_types)
        # test unsupported types
        for t in unsupported_types:
            with self.assertRaises(TypeError):
                _infer_schema_from_data([Row(myarray=array.array(t))])

    def test_data_type_eq(self):
        lt = DataTypes.BIGINT()
        lt2 = pickle.loads(pickle.dumps(DataTypes.BIGINT()))
        self.assertEqual(lt, lt2)

    def test_decimal_type(self):
        t1 = DataTypes.DECIMAL(10, 0)
        t2 = DataTypes.DECIMAL(10, 2)
        self.assertTrue(t2 is not t1)
        self.assertNotEqual(t1, t2)

    def test_datetype_equal_zero(self):
        dt = DataTypes.DATE()
        self.assertEqual(dt.from_sql_type(0), datetime.date(1970, 1, 1))

    def test_timestamp_microsecond(self):
        tst = DataTypes.TIMESTAMP()
        self.assertEqual(tst.to_sql_type(datetime.datetime.max) % 1000000, 999999)

    def test_local_zoned_timestamp_type(self):
        lztst = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE()
        ts = datetime.datetime(1970, 1, 1, 0, 0, 0, 0000)
        self.assertEqual(0, lztst.to_sql_type(ts))

        import pytz
        # suppose the timezone of the data is +9:00
        timezone = pytz.timezone("Asia/Tokyo")
        orig_epoch = LocalZonedTimestampType.EPOCH_ORDINAL
        try:
            # suppose the local timezone is +8:00
            LocalZonedTimestampType.EPOCH_ORDINAL = 28800000000
            ts_tokyo = timezone.localize(ts)
            self.assertEqual(-3600000000, lztst.to_sql_type(ts_tokyo))
        finally:
            LocalZonedTimestampType.EPOCH_ORDINAL = orig_epoch

        if sys.version_info >= (3, 6):
            ts2 = lztst.from_sql_type(0)
            self.assertEqual(ts.astimezone(), ts2.astimezone())

    def test_zoned_timestamp_type(self):
        ztst = ZonedTimestampType()
        ts = datetime.datetime(1970, 1, 1, 0, 0, 0, 0000, tzinfo=UTCOffsetTimezone(1))
        self.assertEqual((0, 3600), ztst.to_sql_type(ts))

        ts2 = ztst.from_sql_type((0, 3600))
        self.assertEqual(ts, ts2)

    def test_day_time_inteval_type(self):
        ymt = DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND())
        td = datetime.timedelta(days=1, seconds=10)
        self.assertEqual(86410000000, ymt.to_sql_type(td))

        td2 = ymt.from_sql_type(86410000000)
        self.assertEqual(td, td2)

    def test_empty_row(self):
        row = Row()
        self.assertEqual(len(row), 0)

    def test_invalid_create_row(self):
        row_class = Row("c1", "c2")
        self.assertRaises(ValueError, lambda: row_class(1, 2, 3))

    def test_nullable(self):
        t = DataType(nullable=False)

        self.assertEqual(t._nullable, False)
        t_nullable = t.nullable()
        self.assertEqual(t_nullable._nullable, True)

    def test_not_null(self):
        t = DataType(nullable=True)

        self.assertEqual(t._nullable, True)
        t_notnull = t.not_null()
        self.assertEqual(t_notnull._nullable, False)


class DataTypeVerificationTests(unittest.TestCase):

    def test_verify_type_exception_msg(self):
        self.assertRaises(
            ValueError,
            lambda: _create_type_verifier(
                DataTypes.STRING(nullable=False), name="test_name")(None))

        schema = DataTypes.ROW(
            [DataTypes.FIELD('a', DataTypes.ROW([DataTypes.FIELD('b', DataTypes.INT())]))])
        self.assertRaises(
            TypeError,
            lambda: _create_type_verifier(schema)([["data"]]))

    def test_verify_type_ok_nullable(self):
        obj = None
        types = [DataTypes.INT(), DataTypes.FLOAT(), DataTypes.STRING(), DataTypes.ROW([])]
        for data_type in types:
            try:
                _create_type_verifier(data_type)(obj)
            except (TypeError, ValueError):
                self.fail("verify_type(%s, %s, nullable=True)" % (obj, data_type))

    def test_verify_type_not_nullable(self):
        import array
        import datetime
        import decimal

        schema = DataTypes.ROW([
            DataTypes.FIELD('s', DataTypes.STRING(nullable=False)),
            DataTypes.FIELD('i', DataTypes.INT(True))])

        class MyObj:
            def __init__(self, **kwargs):
                for k, v in kwargs.items():
                    setattr(self, k, v)

        # obj, data_type
        success_spec = [
            # String
            ("", DataTypes.STRING()),
            (u"", DataTypes.STRING()),

            # UDT
            (ExamplePoint(1.0, 2.0), ExamplePointUDT()),

            # Boolean
            (True, DataTypes.BOOLEAN()),

            # TinyInt
            (-(2 ** 7), DataTypes.TINYINT()),
            (2 ** 7 - 1, DataTypes.TINYINT()),

            # SmallInt
            (-(2 ** 15), DataTypes.SMALLINT()),
            (2 ** 15 - 1, DataTypes.SMALLINT()),

            # Int
            (-(2 ** 31), DataTypes.INT()),
            (2 ** 31 - 1, DataTypes.INT()),

            # BigInt
            (2 ** 64, DataTypes.BIGINT()),

            # Float & Double
            (1.0, DataTypes.FLOAT()),
            (1.0, DataTypes.DOUBLE()),

            # Decimal
            (decimal.Decimal("1.0"), DataTypes.DECIMAL(10, 0)),

            # Binary
            (bytearray([1]), DataTypes.BINARY(1)),

            # Date/Time/Timestamp
            (datetime.date(2000, 1, 2), DataTypes.DATE()),
            (datetime.datetime(2000, 1, 2, 3, 4), DataTypes.DATE()),
            (datetime.time(1, 1, 2), DataTypes.TIME()),
            (datetime.datetime(2000, 1, 2, 3, 4), DataTypes.TIMESTAMP()),

            # Array
            ([], DataTypes.ARRAY(DataTypes.INT())),
            (["1", None], DataTypes.ARRAY(DataTypes.STRING(nullable=True))),
            ([1, 2], DataTypes.ARRAY(DataTypes.INT())),
            ((1, 2), DataTypes.ARRAY(DataTypes.INT())),
            (array.array('h', [1, 2]), DataTypes.ARRAY(DataTypes.INT())),

            # Map
            ({}, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
            ({"a": 1}, DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
            ({"a": None}, DataTypes.MAP(DataTypes.STRING(nullable=False), DataTypes.INT(True))),

            # Struct
            ({"s": "a", "i": 1}, schema),
            ({"s": "a", "i": None}, schema),
            ({"s": "a"}, schema),
            ({"s": "a", "f": 1.0}, schema),
            (Row(s="a", i=1), schema),
            (Row(s="a", i=None), schema),
            (Row(s="a", i=1, f=1.0), schema),
            (["a", 1], schema),
            (["a", None], schema),
            (("a", 1), schema),
            (MyObj(s="a", i=1), schema),
            (MyObj(s="a", i=None), schema),
            (MyObj(s="a"), schema),
        ]

        # obj, data_type, exception class
        failure_spec = [
            # Char/VarChar (match anything but None)
            (None, DataTypes.VARCHAR(1), ValueError),
            (None, DataTypes.CHAR(1), ValueError),

            # VarChar (length exceeds maximum length)
            ("abc", DataTypes.VARCHAR(1), ValueError),
            # Char (length exceeds length)
            ("abc", DataTypes.CHAR(1), ValueError),

            # UDT
            (ExamplePoint(1.0, 2.0), PythonOnlyUDT(), ValueError),

            # Boolean
            (1, DataTypes.BOOLEAN(), TypeError),
            ("True", DataTypes.BOOLEAN(), TypeError),
            ([1], DataTypes.BOOLEAN(), TypeError),

            # TinyInt
            (-(2 ** 7) - 1, DataTypes.TINYINT(), ValueError),
            (2 ** 7, DataTypes.TINYINT(), ValueError),
            ("1", DataTypes.TINYINT(), TypeError),
            (1.0, DataTypes.TINYINT(), TypeError),

            # SmallInt
            (-(2 ** 15) - 1, DataTypes.SMALLINT(), ValueError),
            (2 ** 15, DataTypes.SMALLINT(), ValueError),

            # Int
            (-(2 ** 31) - 1, DataTypes.INT(), ValueError),
            (2 ** 31, DataTypes.INT(), ValueError),

            # Float & Double
            (1, DataTypes.FLOAT(), TypeError),
            (1, DataTypes.DOUBLE(), TypeError),

            # Decimal
            (1.0, DataTypes.DECIMAL(10, 0), TypeError),
            (1, DataTypes.DECIMAL(10, 0), TypeError),
            ("1.0", DataTypes.DECIMAL(10, 0), TypeError),

            # Binary
            (1, DataTypes.BINARY(1), TypeError),
            # VarBinary (length exceeds maximum length)
            (bytearray([1, 2]), DataTypes.VARBINARY(1), ValueError),
            # Char (length exceeds length)
            (bytearray([1, 2]), DataTypes.BINARY(1), ValueError),

            # Date/Time/Timestamp
            ("2000-01-02", DataTypes.DATE(), TypeError),
            ("10:01:02", DataTypes.TIME(), TypeError),
            (946811040, DataTypes.TIMESTAMP(), TypeError),

            # Array
            (["1", None], DataTypes.ARRAY(DataTypes.VARCHAR(1, nullable=False)), ValueError),
            ([1, "2"], DataTypes.ARRAY(DataTypes.INT()), TypeError),

            # Map
            ({"a": 1}, DataTypes.MAP(DataTypes.INT(), DataTypes.INT()), TypeError),
            ({"a": "1"}, DataTypes.MAP(DataTypes.VARCHAR(1), DataTypes.INT()), TypeError),
            ({"a": None}, DataTypes.MAP(DataTypes.VARCHAR(1), DataTypes.INT(False)), ValueError),

            # Struct
            ({"s": "a", "i": "1"}, schema, TypeError),
            (Row(s="a"), schema, ValueError),  # Row can't have missing field
            (Row(s="a", i="1"), schema, TypeError),
            (["a"], schema, ValueError),
            (["a", "1"], schema, TypeError),
            (MyObj(s="a", i="1"), schema, TypeError),
            (MyObj(s=None, i="1"), schema, ValueError),
        ]

        # Check success cases
        for obj, data_type in success_spec:
            try:
                _create_type_verifier(data_type.not_null())(obj)
            except (TypeError, ValueError):
                self.fail("verify_type(%s, %s, nullable=False)" % (obj, data_type))

        # Check failure cases
        for obj, data_type, exp in failure_spec:
            msg = "verify_type(%s, %s, nullable=False) == %s" % (obj, data_type, exp)
            with self.assertRaises(exp, msg=msg):
                _create_type_verifier(data_type.not_null())(obj)


class DataTypeConvertTests(unittest.TestCase):

    def test_basic_type(self):
        test_types = [DataTypes.STRING(),
                      DataTypes.BOOLEAN(),
                      DataTypes.BYTES(),
                      DataTypes.TINYINT(),
                      DataTypes.SMALLINT(),
                      DataTypes.INT(),
                      DataTypes.BIGINT(),
                      DataTypes.FLOAT(),
                      DataTypes.DOUBLE(),
                      DataTypes.DATE(),
                      DataTypes.TIME(),
                      DataTypes.TIMESTAMP(3)]

        java_types = [_to_java_type(item) for item in test_types]

        converted_python_types = [_from_java_type(item) for item in java_types]

        self.assertEqual(test_types, converted_python_types)

    def test_atomic_type_with_data_type_with_parameters(self):
        gateway = get_gateway()
        JDataTypes = gateway.jvm.DataTypes
        java_types = [JDataTypes.TIME(3).notNull(),
                      JDataTypes.TIMESTAMP(3).notNull(),
                      JDataTypes.VARBINARY(100).notNull(),
                      JDataTypes.BINARY(2).notNull(),
                      JDataTypes.VARCHAR(30).notNull(),
                      JDataTypes.CHAR(50).notNull(),
                      JDataTypes.DECIMAL(20, 10).notNull()]

        converted_python_types = [_from_java_type(item) for item in java_types]

        expected = [DataTypes.TIME(3, False),
                    DataTypes.TIMESTAMP(3).not_null(),
                    DataTypes.VARBINARY(100, False),
                    DataTypes.BINARY(2, False),
                    DataTypes.VARCHAR(30, False),
                    DataTypes.CHAR(50, False),
                    DataTypes.DECIMAL(20, 10, False)]
        self.assertEqual(converted_python_types, expected)

        # Legacy type tests
        Types = gateway.jvm.org.apache.flink.table.api.Types
        BlinkBigDecimalTypeInfo = \
            gateway.jvm.org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo

        java_types = [Types.STRING(),
                      Types.DECIMAL(),
                      BlinkBigDecimalTypeInfo(12, 5)]

        converted_python_types = [_from_java_type(item) for item in java_types]

        expected = [DataTypes.VARCHAR(2147483647),
                    DataTypes.DECIMAL(10, 0),
                    DataTypes.DECIMAL(12, 5)]
        self.assertEqual(converted_python_types, expected)

    def test_array_type(self):
        # nullable/not_null flag will be lost during the conversion.
        test_types = [DataTypes.ARRAY(DataTypes.BIGINT()),
                      DataTypes.ARRAY(DataTypes.BIGINT()),
                      DataTypes.ARRAY(DataTypes.STRING()),
                      DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.BIGINT())),
                      DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))]

        java_types = [_to_java_type(item) for item in test_types]

        converted_python_types = [_from_java_type(item) for item in java_types]

        self.assertEqual(test_types, converted_python_types)

    def test_multiset_type(self):
        test_types = [DataTypes.MULTISET(DataTypes.BIGINT()),
                      DataTypes.MULTISET(DataTypes.STRING()),
                      DataTypes.MULTISET(DataTypes.MULTISET(DataTypes.BIGINT())),
                      DataTypes.MULTISET(DataTypes.MULTISET(DataTypes.STRING()))]

        java_types = [_to_java_type(item) for item in test_types]

        converted_python_types = [_from_java_type(item) for item in java_types]

        self.assertEqual(test_types, converted_python_types)

    def test_map_type(self):
        test_types = [DataTypes.MAP(DataTypes.BIGINT(), DataTypes.BIGINT()),
                      DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()),
                      DataTypes.MAP(DataTypes.STRING(),
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.BIGINT())),
                      DataTypes.MAP(DataTypes.STRING(),
                                    DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()))]

        java_types = [_to_java_type(item) for item in test_types]

        converted_python_types = [_from_java_type(item) for item in java_types]

        self.assertEqual(test_types, converted_python_types)

    def test_row_type(self):
        test_types = [DataTypes.ROW([DataTypes.FIELD("a", DataTypes.INT()),
                                     DataTypes.FIELD("b",
                                                     DataTypes.ROW(
                                                         [DataTypes.FIELD("c",
                                                                          DataTypes.STRING())]))])]

        java_types = [_to_java_type(item) for item in test_types]

        converted_python_types = [_from_java_type(item) for item in java_types]

        self.assertEqual(test_types, converted_python_types)


class DataSerializerTests(unittest.TestCase):

    def test_java_pickle_deserializer(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
        serializer = PickleSerializer()
        data = [(1, 2), (3, 4), (5, 6), (7, 8)]

        try:
            serializer.dump_to_stream(data, temp_file)
        finally:
            temp_file.close()

        gateway = get_gateway()
        result = [tuple(int_pair) for int_pair in
                  list(gateway.jvm.PythonBridgeUtils.readPythonObjects(temp_file.name, False))]

        self.assertEqual(result, [(1, 2), (3, 4), (5, 6), (7, 8)])

    def test_java_batch_deserializer(self):
        temp_file = tempfile.NamedTemporaryFile(delete=False, dir=tempfile.mkdtemp())
        serializer = BatchedSerializer(PickleSerializer(), 2)
        data = [(1, 2), (3, 4), (5, 6), (7, 8)]

        try:
            serializer.dump_to_stream(data, temp_file)
        finally:
            temp_file.close()

        gateway = get_gateway()
        result = [tuple(int_pair) for int_pair in
                  list(gateway.jvm.PythonBridgeUtils.readPythonObjects(temp_file.name, True))]

        self.assertEqual(result, [(1, 2), (3, 4), (5, 6), (7, 8)])


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
