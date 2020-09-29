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

from pyflink.common.typeinfo import Types, RowTypeInfo, TupleTypeInfo, _from_java_type
from pyflink.testing.test_case_utils import PyFlinkTestCase


class TypeInfoTests(PyFlinkTestCase):

    def test_row_type(self):
        self.assertEqual(RowTypeInfo([Types.STRING(), Types.STRING()])
                         .get_field_names(), ['f0', 'f1'])
        self.assertEqual(RowTypeInfo([Types.STRING(), Types.STRING()],
                                     ['a', 'b']).get_field_names(), ['a', 'b'])

        self.assertEqual(RowTypeInfo([Types.STRING(), Types.STRING()],
                                     ['a', 'b']) == RowTypeInfo([Types.STRING(),
                                                                 Types.STRING()], ['a', 'b']), True)
        self.assertEqual(RowTypeInfo([Types.STRING(),
                                      Types.STRING()],
                                     ['a', 'b']) == RowTypeInfo([Types.STRING(),
                                                                Types.INT()],
                                                                ['a', 'b']), False)
        self.assertEqual(RowTypeInfo([Types.STRING(),
                                      Types.STRING()],
                                     ['a', 'b']).__str__(), "RowTypeInfo(a: String, b: String)")

        self.assertEqual(Types.ROW([Types.STRING(), Types.STRING()]),
                         RowTypeInfo([Types.STRING(), Types.STRING()]), True)

        self.assertEqual(Types.ROW_NAMED(['a', 'b'], [Types.STRING(), Types.STRING()])
                         .get_field_names(), ['a', 'b'], True)

        self.assertEqual(Types.ROW_NAMED(['a', 'b'], [Types.STRING(), Types.STRING()])
                         .get_field_types(), [Types.STRING(), Types.STRING()], True)

    def test_tuple_type(self):
        self.assertEqual(TupleTypeInfo([Types.STRING(), Types.INT()]),
                         TupleTypeInfo([Types.STRING(), Types.INT()]), True)

        self.assertEqual(TupleTypeInfo([Types.STRING(), Types.INT()]).__str__(),
                         "TupleTypeInfo(String, Integer)")

        self.assertNotEqual(TupleTypeInfo([Types.STRING(), Types.INT()]),
                            TupleTypeInfo([Types.STRING(), Types.BOOLEAN()]))

        self.assertEqual(Types.TUPLE([Types.STRING(), Types.INT()]),
                         TupleTypeInfo([Types.STRING(), Types.INT()]))

        self.assertEqual(Types.TUPLE([Types.STRING(), Types.INT()]).get_field_types(),
                         [Types.STRING(), Types.INT()])

    def test_from_java_type(self):
        basic_int_type_info = Types.INT()
        self.assertEqual(basic_int_type_info,
                         _from_java_type(basic_int_type_info.get_java_type_info()))

        basic_short_type_info = Types.SHORT()
        self.assertEqual(basic_short_type_info,
                         _from_java_type(basic_short_type_info.get_java_type_info()))

        basic_long_type_info = Types.LONG()
        self.assertEqual(basic_long_type_info,
                         _from_java_type(basic_long_type_info.get_java_type_info()))

        basic_float_type_info = Types.FLOAT()
        self.assertEqual(basic_float_type_info,
                         _from_java_type(basic_float_type_info.get_java_type_info()))

        basic_double_type_info = Types.DOUBLE()
        self.assertEqual(basic_double_type_info,
                         _from_java_type(basic_double_type_info.get_java_type_info()))

        basic_char_type_info = Types.CHAR()
        self.assertEqual(basic_char_type_info,
                         _from_java_type(basic_char_type_info.get_java_type_info()))

        basic_byte_type_info = Types.BYTE()
        self.assertEqual(basic_byte_type_info,
                         _from_java_type(basic_byte_type_info.get_java_type_info()))

        basic_big_int_type_info = Types.BIG_INT()
        self.assertEqual(basic_big_int_type_info,
                         _from_java_type(basic_big_int_type_info.get_java_type_info()))

        basic_big_dec_type_info = Types.BIG_DEC()
        self.assertEqual(basic_big_dec_type_info,
                         _from_java_type(basic_big_dec_type_info.get_java_type_info()))

        basic_sql_date_type_info = Types.SQL_DATE()
        self.assertEqual(basic_sql_date_type_info,
                         _from_java_type(basic_sql_date_type_info.get_java_type_info()))

        basic_sql_time_type_info = Types.SQL_TIME()
        self.assertEqual(basic_sql_time_type_info,
                         _from_java_type(basic_sql_time_type_info.get_java_type_info()))

        basic_sql_timestamp_type_info = Types.SQL_TIMESTAMP()
        self.assertEqual(basic_sql_timestamp_type_info,
                         _from_java_type(basic_sql_timestamp_type_info.get_java_type_info()))

        row_type_info = Types.ROW([Types.INT(), Types.STRING()])
        self.assertEqual(row_type_info, _from_java_type(row_type_info.get_java_type_info()))

        tuple_type_info = Types.TUPLE([Types.CHAR(), Types.INT()])
        self.assertEqual(tuple_type_info, _from_java_type(tuple_type_info.get_java_type_info()))

        primitive_int_array_type_info = Types.PRIMITIVE_ARRAY(Types.INT())
        self.assertEqual(primitive_int_array_type_info,
                         _from_java_type(primitive_int_array_type_info.get_java_type_info()))

        pickled_byte_array_type_info = Types.PICKLED_BYTE_ARRAY()
        self.assertEqual(pickled_byte_array_type_info,
                         _from_java_type(pickled_byte_array_type_info.get_java_type_info()))

        sql_date_type_info = Types.SQL_DATE()
        self.assertEqual(sql_date_type_info,
                         _from_java_type(sql_date_type_info.get_java_type_info()))
