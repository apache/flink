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

"""Tests common to all coder implementations."""
import logging
import unittest

from pyflink.fn_execution.coders import BigIntCoder, TinyIntCoder, BooleanCoder, \
    SmallIntCoder, IntCoder, FloatCoder, DoubleCoder, BinaryCoder, CharCoder, DateCoder


class CodersTest(unittest.TestCase):

    def check_coder(self, coder, *values):
        for v in values:
            if isinstance(v, float):
                from pyflink.table.tests.test_udf import float_equal
                assert float_equal(v, coder.decode(coder.encode(v)), 1e-6)
            else:
                self.assertEqual(v, coder.decode(coder.encode(v)))

    # decide whether two floats are equal
    @staticmethod
    def float_equal(a, b, rel_tol=1e-09, abs_tol=0.0):
        return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)

    def test_bigint_coder(self):
        coder = BigIntCoder()
        self.check_coder(coder, 1, 100, -100, -1000)

    def test_tinyint_coder(self):
        coder = TinyIntCoder()
        self.check_coder(coder, 1, 10, 127)

    def test_boolean_coder(self):
        coder = BooleanCoder()
        self.check_coder(coder, True, False)

    def test_smallint_coder(self):
        coder = SmallIntCoder()
        self.check_coder(coder, 32767, -32768, 0)

    def test_int_coder(self):
        coder = IntCoder()
        self.check_coder(coder, -2147483648, 2147483647)

    def test_float_coder(self):
        coder = FloatCoder()
        self.check_coder(coder, 1.02, 1.32)

    def test_double_coder(self):
        coder = DoubleCoder()
        self.check_coder(coder, -12.02, 1.98932)

    def test_binary_coder(self):
        coder = BinaryCoder()
        self.check_coder(coder, b'pyflink')

    def test_char_coder(self):
        coder = CharCoder()
        self.check_coder(coder, 'flink')

    def test_date_coder(self):
        import datetime
        coder = DateCoder()
        self.check_coder(coder, datetime.date(2019, 9, 10))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
