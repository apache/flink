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

import unittest

from pyflink.common.typeinfo import Types, RowTypeInfo, TupleTypeInfo, _from_java_type


class TypeInfoTests(unittest.TestCase):

    def test_types(self):
        self.assertEqual(True, Types.BOOLEAN().is_basic_type())
        self.assertEqual(True, Types.BYTE().is_basic_type())
        self.assertEqual(True, Types.SHORT().is_basic_type())
        self.assertEqual(True, Types.INT().is_basic_type())
        self.assertEqual(True, Types.LONG().is_basic_type())
        self.assertEqual(True, Types.FLOAT().is_basic_type())
        self.assertEqual(True, Types.DOUBLE().is_basic_type())
        self.assertEqual(True, Types.CHAR().is_basic_type())
        self.assertEqual(True, Types.BIG_INT().is_basic_type())
        self.assertEqual(True, Types.BIG_DEC().is_basic_type())
        self.assertEqual(False, Types.PICKLED_BYTE_ARRAY().is_basic_type())

        self.assertEqual(False, Types.STRING().is_tuple_type())
        self.assertEqual(1, Types.STRING().get_total_fields())
        self.assertEqual(1, Types.STRING().get_arity())

        self.assertEqual(False, Types.SQL_DATE().is_basic_type())
        self.assertEqual(False, Types.SQL_TIME().is_basic_type())
        self.assertEqual(False, Types.SQL_TIMESTAMP().is_basic_type())

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
                                     ['a', 'b']).__str__(), "Row(a: String, b: String)")

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
                         "Java Tuple2<String, Integer>")

        self.assertNotEqual(TupleTypeInfo([Types.STRING(), Types.INT()]),
                            TupleTypeInfo([Types.STRING(), Types.BOOLEAN()]))

        self.assertEqual(Types.TUPLE([Types.STRING(), Types.INT()]),
                         TupleTypeInfo([Types.STRING(), Types.INT()]))

        self.assertEqual(Types.TUPLE([Types.STRING(), Types.INT()]).get_field_types(),
                         [Types.STRING(), Types.INT()])

    def test_primitive_array_types(self):
        primitive_int_array_type_info = Types.PRIMITIVE_ARRAY(Types.INT())
        self.assertEqual(primitive_int_array_type_info.is_basic_type(), False)

    def test_from_java_type(self):
        basic_int_type_info = Types.INT()
        self.assertEqual(basic_int_type_info,
                         _from_java_type(basic_int_type_info.get_java_type_info()))

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



