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

from pyflink.table import DataTypes
from pyflink.table.expression import TimeIntervalUnit, TimePointUnit, JsonExistsOnError, \
    JsonValueOnEmptyOrError
from pyflink.table.expressions import (col, lit, range_, and_, or_, current_date,
                                       current_time, current_timestamp, local_time,
                                       local_timestamp, temporal_overlaps, date_format,
                                       timestamp_diff, array, row, map_, row_interval, pi, e,
                                       rand, rand_integer, atan2, negative, concat, concat_ws, uuid,
                                       null_of, log, if_then_else, with_columns, call,
                                       to_timestamp_ltz)
from pyflink.testing.test_case_utils import PyFlinkTestCase


class PyFlinkBatchExpressionTests(PyFlinkTestCase):

    def test_expression(self):
        expr1 = col('a')
        expr2 = col('b')
        expr3 = col('c')
        expr4 = col('d')
        expr5 = lit(10)

        # comparison functions
        self.assertEqual('equals(a, b)', str(expr1 == expr2))
        self.assertEqual('mod(2, b)', str(2 % expr2))
        self.assertEqual('notEquals(a, b)', str(expr1 != expr2))
        self.assertEqual('lessThan(a, b)', str(expr1 < expr2))
        self.assertEqual('lessThanOrEqual(a, b)', str(expr1 <= expr2))
        self.assertEqual('greaterThan(a, b)', str(expr1 > expr2))
        self.assertEqual('greaterThanOrEqual(a, b)', str(expr1 >= expr2))

        # logic functions
        self.assertEqual('and(a, b)', str(expr1 & expr2))
        self.assertEqual('or(a, b)', str(expr1 | expr2))
        self.assertEqual('isNotTrue(a)', str(expr1.is_not_true))
        self.assertEqual('isNotTrue(a)', str(~expr1))

        # arithmetic functions
        self.assertEqual('plus(a, b)', str(expr1 + expr2))
        self.assertEqual('plus(2, b)', str(2 + expr2))
        self.assertEqual('plus(cast(b, DATE), 2)', str(expr2.to_date + 2))
        self.assertEqual('minus(a, b)', str(expr1 - expr2))
        self.assertEqual('minus(cast(b, DATE), 2)', str(expr2.to_date - 2))
        self.assertEqual('times(a, b)', str(expr1 * expr2))
        self.assertEqual('divide(a, b)', str(expr1 / expr2))
        self.assertEqual('mod(a, b)', str(expr1 % expr2))
        self.assertEqual('power(a, b)', str(expr1 ** expr2))
        self.assertEqual('minusPrefix(a)', str(-expr1))

        self.assertEqual('exp(a)', str(expr1.exp))
        self.assertEqual('log10(a)', str(expr1.log10))
        self.assertEqual('log2(a)', str(expr1.log2))
        self.assertEqual('ln(a)', str(expr1.ln))
        self.assertEqual('log(a)', str(expr1.log()))
        self.assertEqual('cosh(a)', str(expr1.cosh))
        self.assertEqual('sinh(a)', str(expr1.sinh))
        self.assertEqual('sin(a)', str(expr1.sin))
        self.assertEqual('cos(a)', str(expr1.cos))
        self.assertEqual('tan(a)', str(expr1.tan))
        self.assertEqual('cot(a)', str(expr1.cot))
        self.assertEqual('asin(a)', str(expr1.asin))
        self.assertEqual('acos(a)', str(expr1.acos))
        self.assertEqual('atan(a)', str(expr1.atan))
        self.assertEqual('tanh(a)', str(expr1.tanh))
        self.assertEqual('degrees(a)', str(expr1.degrees))
        self.assertEqual('radians(a)', str(expr1.radians))
        self.assertEqual('sqrt(a)', str(expr1.sqrt))
        self.assertEqual('abs(a)', str(expr1.abs))
        self.assertEqual('abs(a)', str(abs(expr1)))
        self.assertEqual('sign(a)', str(expr1.sign))
        self.assertEqual('round(a, b)', str(expr1.round(expr2)))
        self.assertEqual('between(a, b, c)', str(expr1.between(expr2, expr3)))
        self.assertEqual('notBetween(a, b, c)', str(expr1.not_between(expr2, expr3)))
        self.assertEqual('ifThenElse(a, b, c)', str(expr1.then(expr2, expr3)))

        self.assertEqual('isNull(a)', str(expr1.is_null))
        self.assertEqual('isNotNull(a)', str(expr1.is_not_null))
        self.assertEqual('isTrue(a)', str(expr1.is_true))
        self.assertEqual('isFalse(a)', str(expr1.is_false))
        self.assertEqual('isNotTrue(a)', str(expr1.is_not_true))
        self.assertEqual('isNotFalse(a)', str(expr1.is_not_false))
        self.assertEqual('distinct(a)', str(expr1.distinct))
        self.assertEqual('sum(a)', str(expr1.sum))
        self.assertEqual('sum0(a)', str(expr1.sum0))
        self.assertEqual('min(a)', str(expr1.min))
        self.assertEqual('max(a)', str(expr1.max))
        self.assertEqual('count(a)', str(expr1.count))
        self.assertEqual('avg(a)', str(expr1.avg))
        self.assertEqual('stddevPop(a)', str(expr1.stddev_pop))
        self.assertEqual('stddevSamp(a)', str(expr1.stddev_samp))
        self.assertEqual('varPop(a)', str(expr1.var_pop))
        self.assertEqual('varSamp(a)', str(expr1.var_samp))
        self.assertEqual('collect(a)', str(expr1.collect))
        self.assertEqual("as(a, 'a', 'b', 'c')", str(expr1.alias('a', 'b', 'c')))
        self.assertEqual('cast(a, INT)', str(expr1.cast(DataTypes.INT())))
        self.assertEqual('asc(a)', str(expr1.asc))
        self.assertEqual('desc(a)', str(expr1.desc))
        self.assertEqual('in(a, b, c, d)', str(expr1.in_(expr2, expr3, expr4)))
        self.assertEqual('start(a)', str(expr1.start))
        self.assertEqual('end(a)', str(expr1.end))
        self.assertEqual('bin(a)', str(expr1.bin))
        self.assertEqual('hex(a)', str(expr1.hex))
        self.assertEqual('truncate(a, 3)', str(expr1.truncate(3)))

        # string functions
        self.assertEqual('substring(a, b, 3)', str(expr1.substring(expr2, 3)))
        self.assertEqual("trim(true, false, ' ', a)", str(expr1.trim_leading()))
        self.assertEqual("trim(false, true, ' ', a)", str(expr1.trim_trailing()))
        self.assertEqual("trim(true, true, ' ', a)", str(expr1.trim()))
        self.assertEqual('replace(a, b, c)', str(expr1.replace(expr2, expr3)))
        self.assertEqual('charLength(a)', str(expr1.char_length))
        self.assertEqual('upper(a)', str(expr1.upper_case))
        self.assertEqual('lower(a)', str(expr1.lower_case))
        self.assertEqual('initCap(a)', str(expr1.init_cap))
        self.assertEqual("like(a, 'Jo_n%')", str(expr1.like('Jo_n%')))
        self.assertEqual("similar(a, 'A+')", str(expr1.similar('A+')))
        self.assertEqual('position(a, b)', str(expr1.position(expr2)))
        self.assertEqual('lpad(a, 4, b)', str(expr1.lpad(4, expr2)))
        self.assertEqual('rpad(a, 4, b)', str(expr1.rpad(4, expr2)))
        self.assertEqual('overlay(a, b, 6, 2)', str(expr1.overlay(expr2, 6, 2)))
        self.assertEqual("regexpReplace(a, b, 'abc')", str(expr1.regexp_replace(expr2, 'abc')))
        self.assertEqual('regexpExtract(a, b, 3)', str(expr1.regexp_extract(expr2, 3)))
        self.assertEqual('fromBase64(a)', str(expr1.from_base64))
        self.assertEqual('toBase64(a)', str(expr1.to_base64))
        self.assertEqual('ltrim(a)', str(expr1.ltrim))
        self.assertEqual('rtrim(a)', str(expr1.rtrim))
        self.assertEqual('repeat(a, 3)', str(expr1.repeat(3)))
        self.assertEqual("over(a, 'w')", str(expr1.over('w')))

        # temporal functions
        self.assertEqual('cast(a, DATE)', str(expr1.to_date))
        self.assertEqual('cast(a, TIME(0))', str(expr1.to_time))
        self.assertEqual('cast(a, TIMESTAMP(3))', str(expr1.to_timestamp))
        self.assertEqual('extract(YEAR, a)', str(expr1.extract(TimeIntervalUnit.YEAR)))
        self.assertEqual('floor(a, YEAR)', str(expr1.floor(TimeIntervalUnit.YEAR)))
        self.assertEqual('ceil(a)', str(expr1.ceil()))

        # advanced type helper functions
        self.assertEqual("get(a, 'col')", str(expr1.get('col')))
        self.assertEqual('flatten(a)', str(expr1.flatten))
        self.assertEqual('at(a, 0)', str(expr1.at(0)))
        self.assertEqual('cardinality(a)', str(expr1.cardinality))
        self.assertEqual('element(a)', str(expr1.element))

        # time definition functions
        self.assertEqual('rowtime(a)', str(expr1.rowtime))
        self.assertEqual('proctime(a)', str(expr1.proctime))
        self.assertEqual('120', str(expr5.year))
        self.assertEqual('120', str(expr5.years))
        self.assertEqual('30', str(expr5.quarter))
        self.assertEqual('30', str(expr5.quarters))
        self.assertEqual('10', str(expr5.month))
        self.assertEqual('10', str(expr5.months))
        self.assertEqual('6048000000', str(expr5.week))
        self.assertEqual('6048000000', str(expr5.weeks))
        self.assertEqual('864000000', str(expr5.day))
        self.assertEqual('864000000', str(expr5.days))
        self.assertEqual('36000000', str(expr5.hour))
        self.assertEqual('36000000', str(expr5.hours))
        self.assertEqual('600000', str(expr5.minute))
        self.assertEqual('600000', str(expr5.minutes))
        self.assertEqual('10000', str(expr5.second))
        self.assertEqual('10000', str(expr5.seconds))
        self.assertEqual('10', str(expr5.milli))
        self.assertEqual('10', str(expr5.millis))

        # hash functions
        self.assertEqual('md5(a)', str(expr1.md5))
        self.assertEqual('sha1(a)', str(expr1.sha1))
        self.assertEqual('sha224(a)', str(expr1.sha224))
        self.assertEqual('sha256(a)', str(expr1.sha256))
        self.assertEqual('sha384(a)', str(expr1.sha384))
        self.assertEqual('sha512(a)', str(expr1.sha512))
        self.assertEqual('sha2(a, 224)', str(expr1.sha2(224)))

        # json functions
        self.assertEqual("JSON_EXISTS('{}', '$.x')", str(lit('{}').json_exists('$.x')))
        self.assertEqual("JSON_EXISTS('{}', '$.x', FALSE)",
                         str(lit('{}').json_exists('$.x', JsonExistsOnError.FALSE)))

        self.assertEqual("JSON_VALUE('{}', '$.x', STRING, NULL, null, NULL, null)",
                         str(lit('{}').json_value('$.x')))
        self.assertEqual("JSON_VALUE('{}', '$.x', INT, DEFAULT, 42, ERROR, null)",
                         str(lit('{}').json_value('$.x', DataTypes.INT(),
                                                  JsonValueOnEmptyOrError.DEFAULT, 42,
                                                  JsonValueOnEmptyOrError.ERROR, None)))

    def test_expressions(self):
        expr1 = col('a')
        expr2 = col('b')
        expr3 = col('c')

        self.assertEqual('10', str(lit(10, DataTypes.INT(False))))
        self.assertEqual('rangeTo(1, 2)', str(range_(1, 2)))
        self.assertEqual('and(a, b, c)', str(and_(expr1, expr2, expr3)))
        self.assertEqual('or(a, b, c)', str(or_(expr1, expr2, expr3)))

        from pyflink.table.expressions import UNBOUNDED_ROW, UNBOUNDED_RANGE, CURRENT_ROW, \
            CURRENT_RANGE
        self.assertEqual('unboundedRow()', str(UNBOUNDED_ROW))
        self.assertEqual('unboundedRange()', str(UNBOUNDED_RANGE))
        self.assertEqual('currentRow()', str(CURRENT_ROW))
        self.assertEqual('currentRange()', str(CURRENT_RANGE))

        self.assertEqual('currentDate()', str(current_date()))
        self.assertEqual('currentTime()', str(current_time()))
        self.assertEqual('currentTimestamp()', str(current_timestamp()))
        self.assertEqual('localTime()', str(local_time()))
        self.assertEqual('localTimestamp()', str(local_timestamp()))
        self.assertEqual('toTimestampLtz(123, 0)', str(to_timestamp_ltz(123, 0)))
        self.assertEqual("temporalOverlaps(cast('2:55:00', TIME(0)), 3600000, "
                         "cast('3:30:00', TIME(0)), 7200000)",
                         str(temporal_overlaps(
                             lit("2:55:00").to_time,
                             lit(1).hours,
                             lit("3:30:00").to_time,
                             lit(2).hours)))
        self.assertEqual("dateFormat(time, '%Y, %d %M')",
                         str(date_format(col("time"), "%Y, %d %M")))
        self.assertEqual("timestampDiff(DAY, cast('2016-06-15', DATE), cast('2016-06-18', DATE))",
                         str(timestamp_diff(
                             TimePointUnit.DAY,
                             lit("2016-06-15").to_date,
                             lit("2016-06-18").to_date)))
        self.assertEqual('array(1, 2, 3)', str(array(1, 2, 3)))
        self.assertEqual("row('key1', 1)", str(row("key1", 1)))
        self.assertEqual("map('key1', 1, 'key2', 2, 'key3', 3)",
                         str(map_("key1", 1, "key2", 2, "key3", 3)))
        self.assertEqual('4', str(row_interval(4)))
        self.assertEqual('pi()', str(pi()))
        self.assertEqual('e()', str(e()))
        self.assertEqual('rand(4)', str(rand(4)))
        self.assertEqual('randInteger(4)', str(rand_integer(4)))
        self.assertEqual('atan2(1, 2)', str(atan2(1, 2)))
        self.assertEqual('minusPrefix(a)', str(negative(expr1)))
        self.assertEqual('concat(a, b, c)', str(concat(expr1, expr2, expr3)))
        self.assertEqual("concat_ws(', ', b, c)", str(concat_ws(', ', expr2, expr3)))
        self.assertEqual('uuid()', str(uuid()))
        self.assertEqual('null', str(null_of(DataTypes.BIGINT())))
        self.assertEqual('log(a)', str(log(expr1)))
        self.assertEqual('ifThenElse(a, b, c)', str(if_then_else(expr1, expr2, expr3)))
        self.assertEqual('withColumns(a, b, c)', str(with_columns(expr1, expr2, expr3)))
        self.assertEqual('a.b.c(a)', str(call('a.b.c', expr1)))


if __name__ == "__main__":
    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
