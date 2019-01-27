/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo._
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, Types}
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo
import org.junit.{Before, Test}
import org.apache.flink.table.util.DateTimeTestUtil.{UTCDate, UTCTimestamp}

import scala.collection.Seq

/**
  * This class includes cases of type coercion.
  */
class TypeCoercionITCase extends BatchTestBase {
  @Before
  def before(): Unit = {
    registerCollection(
      "t4",
      Seq(row(1, "abc", "123".getBytes(), true, "t",
        java.math.BigDecimal.valueOf(3404045.5044003))),
      new RowTypeInfo(
        INT_TYPE_INFO,
        STRING_TYPE_INFO,
        BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
        BOOLEAN_TYPE_INFO,
        STRING_TYPE_INFO,
        BIG_DEC_TYPE_INFO),
      "x,y,z,j,k,m")
    registerCollection(
      "t5",
      Seq(row(null, "abc d".getBytes, "123 4".getBytes,
        128L, 32768L, 2147483648L, new java.math.BigDecimal("9223372036854775808"),
        -129L, -32769L, -2147483649L, new java.math.BigDecimal("-9223372036854775809"),
        1.1f, 1.1, UTCTimestamp("2018-06-08 00:00:00"), UTCDate("1996-11-10"), 123L, 123L)),
      new RowTypeInfo(
        INT_TYPE_INFO,
        BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
        BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
        LONG_TYPE_INFO,
        LONG_TYPE_INFO,
        LONG_TYPE_INFO,
        BIG_DEC_TYPE_INFO,
        LONG_TYPE_INFO,
        LONG_TYPE_INFO,
        LONG_TYPE_INFO,
        BIG_DEC_TYPE_INFO,
        FLOAT_TYPE_INFO,
        DOUBLE_TYPE_INFO,
        Types.SQL_TIMESTAMP,
        Types.SQL_DATE,
        TimeIndicatorTypeInfo.PROCTIME_INDICATOR,
        TimeIndicatorTypeInfo.ROWTIME_INDICATOR),
    "x,y,z," +
      "tinyintMax,smallintMax,intMax,bigintMax," +
      "tinyintMin,smallintMin,intMin,bigintMin," +
      "floatField,doubleField,a,b," +
      "procIndicator,rowIndicator",
      Seq(true, true, true, true, true, true, true,
        true, true, true, true, true, true, true, true, false, false)
    )
    registerCollection(
      "t6",
      Seq(row(1, 1), row(1, 2),
        row(2, 2), row(2, 3), row(2, 4),
        row(3, 1)
      ),
      new RowTypeInfo(
        INT_TYPE_INFO,
        INT_TYPE_INFO),
      "key,v")
    registerCollection(
      "t7",
      Seq(row(1E99),
        row(-1234567890.1234567890),
        row(1234567890.1234567890),
        row(0.9999999999999999999999999),
        row(-1.0)),
      new RowTypeInfo(
        DOUBLE_TYPE_INFO
      ),
      "v"
    )
    registerCollection(
      "t8",
      Seq(row(0.99),
        row(1.09),
        row(1.0),
        row(1D),
        row(22D),
        row(-0.99),
        row(-1.0),
        row(-1D),
        row(-22D)),
      new RowTypeInfo(
        DOUBLE_TYPE_INFO
      ),
      "v"
    )
    registerCollection(
      "t9",
      Seq(row(null)),
      new RowTypeInfo(
        BIG_DEC_TYPE_INFO
      ),
      "v",
      Seq(true)
    )
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    env.setParallelism(Math.min(env.getParallelism, 8))
  }

  @Test
  def testUnionWithMisMatchType(): Unit = {
    checkResult(
      """
        |SELECT cast(1 as tinyint) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as smallint) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as int) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as bigint) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as float) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1.0"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as double) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1.0"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as decimal(2, 0)) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("1"), row("2")))
    checkResult(
      """
        |SELECT cast(1 as boolean) FROM t4 UNION SELECT cast(2 as varchar) FROM t4
      """.stripMargin,
      Seq(row("true"), row("2")))
    // Using ReturnTypes.LEAST_RESTRICTIVE type infer.
    checkResult(
      """
        |SELECT timestamp '2018-06-06 16:26:00.0' FROM t4 UNION SELECT date '2018-06-07' FROM t4
      """.stripMargin,
      Seq(row("2018-06-06"), row("2018-06-07")))
    checkResult(
      """
        |SELECT timestamp '2018-06-06 16:26:00.0' FROM t4 UNION SELECT '2018-06-07' FROM t4
      """.stripMargin,
      Seq(row("2018-06-06 16:26:00.000"), row("2018-06-07")))
    checkResult(
      """
        |SELECT date '2018-06-06' FROM t4 UNION SELECT '2018-06-07' FROM t4
      """.stripMargin,
      Seq(row("2018-06-06"), row("2018-06-07")))
  }

  @Test
  def testIfFunc(): Unit = {
    checkResult(
      """
        |select if(1>0, cast(1 as decimal(2, 1)), cast(0 as decimal(3, 2))) from t4
      """.stripMargin, Seq(row("1.00")))
    checkResult(
      """
        |select if(1>0, '2', cast(0 as decimal(3, 2))) from t4
      """.stripMargin, Seq(row("2")))
    checkResult(
      """
        |select if(1>0, date '2018-06-08', timestamp '2018-06-07 17:00:00.0') from t4
      """.stripMargin, Seq(row("2018-06-08")))
    checkResult(
      """
        |select if(1>0, 'a', cast(0 as decimal(3, 2))) from t4
      """.stripMargin, Seq(row("a")))
    checkResult(
      """
        |select if(1<0, 'a', cast(0 as decimal(3, 2))) from t4
      """.stripMargin, Seq(row("0.00")))
  }

  @Test
  def testIfFuncNumericArgs(): Unit = {
    val numericTypes = Seq(
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "decimal(5, 3)",
      "float",
      "double"
    )

    for (t1 <- numericTypes) {
      for (t2 <- numericTypes) {
        val result =
          if (t1 == "float" || t1 == "double" || t2 == "float" || t2 == "double") "1.0"
          else if (t1 == "decimal(5, 3)" || t2 == "decimal(5, 3)") "1.000"
          else "1"
        checkResult(s"select if(1 > 0, cast(1 as $t1), cast(0 as $t2)) from t4",
          Seq(row(result)))
      }
    }
  }

  @Test
  def testIfFuncDecimalArgs(): Unit = {
    checkResult(
      "select if(1 > 0, cast(111 as decimal(5, 2)), cast(0.222 as decimal(3, 3))) from t4",
      Seq(row("111.000")))
    checkResult(
      "select if(1 < 0, cast(111 as decimal(5, 2)), cast(0.222 as decimal(3, 3))) from t4",
      Seq(row("0.222")))
    checkResult(
      "select if(1 > 0, cast(-111 as decimal(5, 2)), cast(0.222 as decimal(3, 3))) from t4",
      Seq(row("-111.000")))
    checkResult(
      "select if(1 < 0, cast(111 as decimal(5, 2)), cast(-0.222 as decimal(3, 3))) from t4",
      Seq(row("-0.222")))
  }

  @Test
  def testBinaryTypeCast(): Unit = {
    checkResult(
      """
        |SELECT cast('123' as binary) FROM t4
      """.stripMargin, Seq(row(Array(49, 50, 51))))
    checkResult(
      """
        |SELECT ' ' = X'0020' FROM t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |SELECT ' ' > X'001F' FROM t4
      """.stripMargin, Seq(row(true)))
    checkResult(
      """
        |SELECT ' ' >= X'001F' FROM t4
      """.stripMargin, Seq(row(true)))
    checkResult(
      """
        |SELECT ' ' < X'001F' FROM t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |SELECT ' ' <= X'001F' FROM t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |SELECT ' ' <> X'001F' FROM t4
      """.stripMargin, Seq(row(true)))
    checkResult(
      """
        |SELECT ' ' = z FROM t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |SELECT y = z FROM t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |SELECT '123' = z FROM t4
      """.stripMargin, Seq(row(true)))
  }

  @Test
  def testDecimalEqualToBoolean(): Unit = {
    checkResult(
      """
        |SELECT true = cast(1 as decimal(10, 0)) FROM t4
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast(1 as decimal(10, 0)) = true FROM t4
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT false = cast(0 as decimal(10, 0)) FROM t4
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast(0 as decimal(10, 0)) = false FROM t4
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT true = cast(2 as decimal(10, 0)) FROM t4
      """.stripMargin, Seq(row(false))
    )
    checkResult(
      """
        |SELECT true = cast(-1 as decimal(10, 0)) FROM t4
      """.stripMargin, Seq(row(false))
    )
  }

  @Test
  def testVarcharToBoolean(): Unit = {
    // Will do implicit cast from varchar -> boolean.
    // Keep sync with SPARK, "t", "true", "y", "yes", "1" will equals to true
    // "f", "false", "n", "no", "0" will equals to false.
    val trueStrings = Seq("'t'", "'true'", "'y'", "'yes'", "'1'")
    val falseStrings = Seq("'f'", "'false'", "'n'", "'no'", "'0'")
    trueStrings.foreach(v => {
      checkResult(s"select true = $v from t4", Seq(row(true)))
    })
    falseStrings.foreach(v => {
      checkResult(s"select false = $v from t4", Seq(row(true)))
    })
    checkResult(
      """
        |SELECT k = j FROM t4
      """.stripMargin, Seq(row(true)))
    checkResult(
      """
        |SELECT cast(1 as boolean) = '1' FROM t4
      """.stripMargin, Seq(row(true)))
    checkResult(
      """
        |SELECT y = true FROM t4
      """.stripMargin, Seq(row(null)))
  }

  @Test
  def testCaseWhenWithDateTime(): Unit = {
    // Calcite use leastRestrictive type here, which is date type.
    checkResult(
      """
        |SELECT CASE WHEN true THEN cast('2017-12-12 09:30:00.0' as timestamp)
        | ELSE cast('2017-12-11 09:30:00' as date) END FROM t4
      """.stripMargin, Seq(row("2017-12-12")))
  }

  @Test
  def testVarcharToDate(): Unit = {
    checkResult(
      """
        |select cast('2017-12-12 09:30:00.0' as date)
      """.stripMargin, Seq(row("2017-12-12")))
    checkResult(
      """
        |select cast('2017-11-12 09:30:00.000' as date)
      """.stripMargin, Seq(row("2017-11-12")))
  }

  @Test
  def testBooleanToNumeric(): Unit = {
    checkResult(
      """
        |select cast(true as decimal(2, 0)) from t4
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |select cast(true as float) from t4
      """.stripMargin, Seq(row(1.0)))
    checkResult(
      """
        |select cast(true as double) from t4
      """.stripMargin, Seq(row(1.0)))
    checkResult(
      """
        |select cast(true as tinyint) from t4
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |select cast(true as smallint) from t4
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |select cast(true as int) from t4
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |select cast(true as bigint) from t4
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |select cast(false as decimal(2, 0)) from t4
      """.stripMargin, Seq(row(0)))
    checkResult(
      """
        |select cast(false as float) from t4
      """.stripMargin, Seq(row(0.0)))
    checkResult(
      """
        |select cast(false as double) from t4
      """.stripMargin, Seq(row(0.0)))
    checkResult(
      """
        |select cast(false as tinyint) from t4
      """.stripMargin, Seq(row(0)))
    checkResult(
      """
        |select cast(false as smallint) from t4
      """.stripMargin, Seq(row(0)))
    checkResult(
      """
        |select cast(false as int) from t4
      """.stripMargin, Seq(row(0)))
    checkResult(
      """
        |select cast(false as bigint) from t4
      """.stripMargin, Seq(row(0)))
  }

  @Test
  def testVarcharToNumeric(): Unit = {
    checkResult(
      """
        |select 1>0, 1>1, 1>2, 1>'1', 2>'1.0', 2>'2.0', 2>'2.2', '1.5'>0.5 from t4
      """.stripMargin, Seq(row(true, false, false, false, true, false, false, true)))
    // The 1. format is invalid but Double.parseDouble supports it.
    checkResult(
      """
        |select 1>'1.' from t4
      """.stripMargin, Seq(row(false)))
    checkResult(
      """
        |select 1>'1.abc' from t4
      """.stripMargin, Seq(row(null)))
    checkResult(
      """
        |SELECT CAST('1.23' AS tinyint)
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |SELECT CAST('1.23' AS smallint)
      """.stripMargin, Seq(row(1L)))
    checkResult(
      """
        |SELECT CAST('1.23' AS int)
      """.stripMargin, Seq(row(1)))
    checkResult(
      """
        |SELECT CAST('1.23' AS bigint)
      """.stripMargin, Seq(row(1L)))
    checkResult(
      """
        |SELECT CAST('-4.56' AS int)
      """.stripMargin, Seq(row(-4)))
    checkResult(
      """
        |SELECT CAST('-4.56' AS bigint)
      """.stripMargin, Seq(row(-4L)))
    checkResult(
      """
        |SELECT CAST('  -4.56' AS bigint)
      """.stripMargin, Seq(row(-4)))
  }

  @Test
  def testInvalidVarcharToDecimal(): Unit = {
    // test invalid varchar toBigDecimalSlow
    checkResult(
      """
        |SELECT CAST('abc' as decimal(38, 18))
      """.stripMargin, Seq(row(null)))
    // test whitespaces to decimal
    checkResult(
      """
        |SELECT CAST('   ' as decimal(10, 0))
      """.stripMargin, Seq(row(null)))
    // test all prefix plus/minus
    checkResult(
      """
        |SELECT CAST('+++' as decimal(10, 0)), CAST('---' as decimal(10, 0))
      """.stripMargin, Seq(row(null, null)))
    // test more than one  decimal mark
    checkResult(
      """
        |SELECT CAST('12..34' as decimal(10, 0))
      """.stripMargin, Seq(row(null)))
    // test invalid scientific notation
    checkResult(
      """
        |SELECT CAST('e+006' as decimal(10, 0)), CAST('e-006' as decimal(10, 0)),
        |  CAST('e---' as decimal(10, 0)), CAST('e+++' as decimal(10, 0))
      """.stripMargin, Seq(row(0, 0, null, null)))
    // test cast to max precision/scale decimal
    checkResult(
      """
        |SELECT CAST('123\n' as decimal(38, 18)), CAST('123\t' as decimal(38, 18)),
        |  CAST('123 ' as decimal(38, 18)), CAST('123a' as decimal(38, 18))
      """.stripMargin, Seq(row(null, null, "123.000000000000000000", null)))
    // test cast to decimal overflow
    checkResult(
      """
        |SELECT CAST('123345678678878678578567868678' as decimal(10, 0))
      """.stripMargin, Seq(row(null)))
  }

  @Test
  def testVarCharToDateNotEquals(): Unit = {
    checkResult(
      """
        |SELECT cast('1996-9-10' as date) <> '1996-09-09' FROM t4
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT date '1996-9-10' <> '1996-09-10' FROM t4
      """.stripMargin, Seq(row(false))
    )
  }

  @Test
  def testTimestampToDecimal(): Unit = {
    //have accuracy error
    checkResult(
      """
        |SELECT cast(cast('2012-12-19 11:12:19' as timestamp) as decimal(30,0))
      """.stripMargin, Seq(row(BigDecimal("1355915539")))
    )
    checkResult(
      """
        |SELECT cast(cast('2012-12-19 11:12:19.123456' as timestamp) as decimal(30,8))
      """.stripMargin, Seq(row(BigDecimal("1355915539.12300000")))
    )
  }

  @Test
  def testDateEqualString(): Unit = {
    //add timezone in string
    checkResult(
      """
        |SELECT cast('2018-07-10' as date) = '2018-07-10'
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast('2018-07-10' as date) = '2018-07-09'
      """.stripMargin, Seq(row(false))
    )
    checkResult(
      """
        |SELECT cast('2018-06-08 00:00:00' as timestamp) = '2018-06-08 00:00:00'
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast('2018-06-08 00:00:00' as timestamp) = '2018-06-08 08:00:00-08:00'
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast('2018-06-08 00:00:00' as timestamp) = '2018-06-07 16:00:00+08:00'
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT cast('2018-06-08 00:00:00' as timestamp) = '2018-06-07 16:00:00 + 08:00'
      """.stripMargin, Seq(row(true))
    )
  }

  @Test
  def testCastStringToNumericWithSpace(): Unit = {
    checkResult(
      """
        |SELECT CAST(' 1 ' as tinyint)
      """.stripMargin, Seq(row(1))
    )
    checkResult(
      """
        |SELECT CAST(' 1 ' as smallint)
      """.stripMargin, Seq(row(1))
    )
    checkResult(
      """
        |SELECT CAST(' 1 ' as int)
      """.stripMargin, Seq(row(1))
    )
    checkResult(
      """
        |SELECT CAST(' 1 ' as bigint)
      """.stripMargin, Seq(row(1))
    )
    checkResult(
      """
        |SELECT CAST(' 1.2 ' as float)
      """.stripMargin, Seq(row(1.2))
    )
    checkResult(
      """
        |SELECT CAST(' 1.2 ' as double)
      """.stripMargin, Seq(row(1.2))
    )
    checkResult(
      """
        |SELECT CAST(' 1.23 ' as decimal(10,2))
      """.stripMargin, Seq(row(BigDecimal("1.23")))
    )
    checkResult(
      """
        |SELECT CAST(' 1 ' as decimal(10,2))
      """.stripMargin, Seq(row(BigDecimal("1.00")))
    )
    checkResult(
      """
        |SELECT CAST(' -1.23 ' as decimal(10,2))
      """.stripMargin, Seq(row(BigDecimal("-1.23")))
    )
    checkResult(
      """
        |SELECT CAST(' -1 ' as decimal(10,2))
      """.stripMargin, Seq(row(BigDecimal("-1.00")))
    )
  }

  @Test
  def testBinaryToString(): Unit = {
    checkResult(
      """
        |SELECT CAST('123' AS BINARY)
      """.stripMargin, Seq(row("[49, 50, 51]"))
    )
    checkResult(
      """
        |SELECT IF(3<5, y, z) FROM t5
      """.stripMargin, Seq(row("[97, 98, 99, 32, 100]"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('123' AS BINARY) AS VARCHAR)
      """.stripMargin, Seq(row("123"))
    )
    checkResult(
      """
        |SELECT CAST('123' AS BINARY) UNION SELECT '2'
      """.stripMargin, Seq(row("123"), row("2"))
    )
  }

  @Test
  def testDecimalToTimestamp() :Unit = {
    checkResult(
      """
        |SELECT CAST(CAST(17.29 as decimal(10,2)) as timestamp)
      """.stripMargin, Seq(row("1970-01-01 00:00:17.29"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(17.12345 as decimal(10,5)) as timestamp)
      """.stripMargin, Seq(row("1970-01-01 00:00:17.123"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-17.29 as decimal(10,2)) as timestamp)
      """.stripMargin, Seq(row("1969-12-31 23:59:42.71"))
    )
  }

  @Test
  def testCompareNull(): Unit = {
    checkResult(
      """
        |SELECT x FROM t5 INTERSECT SELECT x FROM t5
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT x FROM t5 EXCEPT SELECT x FROM t5
      """.stripMargin, Seq()
    )
  }

  @Test
  def testDecimalToBoolean(): Unit = {
    checkResult(
      """
        |SELECT CAST(CAST(17.29 as decimal(10,2)) as boolean)
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0.00 as decimal(10,2)) as boolean)
      """.stripMargin, Seq(row(false))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0.01 as decimal(10,2)) as boolean)
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-17.29 as decimal(10,2)) as boolean)
      """.stripMargin, Seq(row(true))
    )
  }

  @Test
  def testDecimalOutOfRange(): Unit = {
    checkResult(
      """
        |SELECT CAST(0.999999999999999999999 as decimal(20,19))
      """.stripMargin, Seq(row(BigDecimal("1.0000000000000000000")))
    )
    checkResult(
      """
        |SELECT CAST(0.999999999999999999859 as decimal(20,19))
      """.stripMargin, Seq(row(BigDecimal("0.9999999999999999999")))
    )
    checkResult(
      """
        |SELECT CAST(0.999999999999999999849 as decimal(20,19))
      """.stripMargin, Seq(row(BigDecimal("0.9999999999999999998")))
    )
  }

  @Test
  def testNullableIntersect(): Unit = {
    checkResult(
      """
        |SELECT key, CAST(COUNT(v) AS BIGINT) FROM t6 GROUP BY key
        |INTERSECT SELECT key, CAST(MAX(v) AS BIGINT) FROM t6 GROUP BY key
      """.stripMargin, Seq(row(1, 2), row(3, 1))
    )
    checkResult(
      """
        |SELECT CAST(COUNT(v) AS BIGINT) FROM t6 GROUP BY key
        |INTERSECT SELECT CAST(MAX(v) AS BIGINT) FROM t6 GROUP BY key
      """.stripMargin, Seq(row(1), row(2))
    )
  }

  @Test
  def testBooleanToDecimal(): Unit = {
    checkResult(
      """
        |SELECT CAST(true AS DECIMAL(10, 0))
      """.stripMargin, Seq(row(BigDecimal("1")))
    )
    checkResult(
      """
        |SELECT CAST(false AS DECIMAL(10, 0))
      """.stripMargin, Seq(row(BigDecimal("0")))
    )
    checkResult(
      """
        |SELECT CAST(true AS DECIMAL(10, 2))
      """.stripMargin, Seq(row(BigDecimal("1.00")))
    )
    checkResult(
      """
        |SELECT CAST(false AS DECIMAL(10, 2))
      """.stripMargin, Seq(row(BigDecimal("0.00")))
    )
    checkResult(
      """
        |SELECT CAST(true AS DECIMAL(10, 5))
      """.stripMargin, Seq(row(BigDecimal("1.00000")))
    )
    checkResult(
      """
        |SELECT CAST(false AS DECIMAL(10, 5))
      """.stripMargin, Seq(row(BigDecimal("0.00000")))
    )
  }

  @Test
  def testBooleanAndNumeric(): Unit = {
    val listInt = List("tinyint", "smallint", "int", "bigint")
    val listFloat = List("float", "double")
    listInt foreach {
      //tinyint/smallint/int/bigint
      i =>
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) + CAST(true AS $i)
          """.stripMargin, Seq(row(2))
        )
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) + CAST(false AS $i)
          """.stripMargin, Seq(row(1))
        )
        checkResult(
          s"""
             |SELECT CAST(true AS $i) + CAST(1 AS $i)
          """.stripMargin, Seq(row(2))
        )
        checkResult(
          s"""
             |SELECT CAST(false AS $i) + CAST(1 AS $i)
          """.stripMargin, Seq(row(1))
        )
    }
    listFloat foreach {
      i =>
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) + CAST(true AS $i)
        """.stripMargin, Seq(row(2.0))
        )
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) + CAST(false AS $i)
        """.stripMargin, Seq(row(1.0))
        )
        checkResult(
          s"""
             |SELECT CAST(true AS $i) + CAST(1 AS $i)
        """.stripMargin, Seq(row(2.0))
        )
        checkResult(
          s"""
             |SELECT CAST(false AS $i) + CAST(1 AS $i)
        """.stripMargin, Seq(row(1.0))
        )
    }
    //decimal
    val i = "decimal(10, 2)"
    checkResult(
      s"""
         |SELECT CAST(1 AS $i) + CAST(true AS $i)
        """.stripMargin, Seq(row("2.00"))
    )
    checkResult(
      s"""
         |SELECT CAST(1 AS $i) + CAST(false AS $i)
        """.stripMargin, Seq(row("1.00"))
    )
    checkResult(
      s"""
         |SELECT CAST(true AS $i) + CAST(1 AS $i)
        """.stripMargin, Seq(row("2.00"))
    )
    checkResult(
      s"""
         |SELECT CAST(false AS $i) + CAST(1 AS $i)
        """.stripMargin, Seq(row("1.00"))
    )
  }

  @Test
  def testNumericToTimestamp(): Unit = {
    checkResult(
      """
        |SELECT CAST(CAST(1 AS TINYINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0 AS TINYINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1 AS TINYINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:59.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(1 AS SMALLINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0 AS SMALLINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1 AS SMALLINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:59.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(1 AS INT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0 AS INT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1 AS INT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:59.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(1 AS BIGINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0 AS BIGINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1 AS BIGINT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:59.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(1.234 AS FLOAT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0.00 AS FLOAT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1.234 AS FLOAT) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:58.766"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(1.234 AS DOUBLE) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0.00 AS DOUBLE) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1.234 AS DOUBLE) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:58.766"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1.234' AS DECIMAL(10, 3)) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:01.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(0.00 AS DECIMAL(10, 3)) AS TIMESTAMP)
      """.stripMargin, Seq(row("1970-01-01 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT CAST(CAST(-1.234 AS DECIMAL(10, 3)) AS TIMESTAMP)
      """.stripMargin, Seq(row("1969-12-31 23:59:58.766"))
    )
  }

  @Test
  def testNumericOutOfRangeUpExplicit(): Unit = {
    //Overflow
    val list = List(
      ("tinyint", "128"),
      ("smallint", "32768"),
      ("int", "2147483648"),
      ("bigint", "9223372036854775808"))
    list foreach{
      case (i, j) =>
        checkResult(
          s"""
             |SELECT CAST($j AS $i)
        """.stripMargin, Seq(row(s"-$j"))
        )
        checkResult(
          s"""
             |SELECT CAST(CAST($j AS bigint) AS $i)
      """.stripMargin, Seq(row(s"-$j"))
        )
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) < $j
      """.stripMargin, Seq(row(true))
        )
    }
  }

  @Test
  def testNumericOutOfRangeDownExplicit(): Unit = {
    //Overflow
    val list = List(
      ("tinyint", "-129", "127"),
      ("smallint", "-32769", "32767"),
      ("int", "-2147483649", "2147483647"),
      ("bigint", "-9223372036854775809", "9223372036854775807"))
    list foreach{
      case (i, j, k) =>
        checkResult(
          s"""
             |SELECT CAST($j AS $i)
        """.stripMargin, Seq(row(k))
        )
        checkResult(
          s"""
             |SELECT CAST(CAST($j AS bigint) AS $i)
      """.stripMargin, Seq(row(k))
        )
        checkResult(
          s"""
             |SELECT CAST(1 AS $i) > $j
      """.stripMargin, Seq(row(true))
        )
    }
  }

  @Test
  def testNumericOutOfRangeUpImplicit(): Unit = {
    //Overflow
    val list = List(
      ("tinyint", "127", "-128"),
      ("smallint", "32767", "-32768"),
      ("int", "2147483647", "-2147483648"),
      ("bigint", "9223372036854775807", "-9223372036854775808"))
    list foreach{
      case (i, j, k) =>
        checkResult(
          s"""
             |SELECT CAST($j AS $i) + CAST(1 AS $i)
        """.stripMargin, Seq(row(k))
        )
    }
  }

  @Test
  def testNumericOutOfRangeDownImplicit(): Unit = {
    //Overflow
    val list = List(
      ("tinyint", "-128", "127"),
      ("smallint", "-32768", "32767"),
      ("int", "-2147483648", "2147483647"),
      ("bigint", "-9223372036854775808", "9223372036854775807"))
    list foreach{
      case (i, j, k) =>
        checkResult(
          s"""
             |SELECT CAST($j AS $i) - CAST(1 AS $i)
          """.stripMargin, Seq(row(k))
        )
    }
  }

  @Test
  def testFloatingPointToInteger(): Unit = {
    //Overflow
    val list = List(
      "tinyint", "smallint",
      "int", "bigint")
    list foreach {
      i => checkResult(
        s"""
          |SELECT CAST(1.1 AS $i)
        """.stripMargin, Seq(row(1))
      )
    }
  }

  @Test
  def testColumnOverflow(): Unit = {
    val list = List(
      ("tinyint", "-128", "127"),
      ("smallint", "-32768", "32767"),
      ("int", "-2147483648", "2147483647"),
      ("bigint", "-9223372036854775808", "9223372036854775807"))
    list foreach {
      case (i, j, k) =>
        checkResult(
          s"""
             |SELECT CAST(${i}Max AS $i) from t5
           """.stripMargin, Seq(row(j))
        )
        checkResult(
          s"""
             |SELECT CAST(${i}Min AS $i) from t5
           """.stripMargin, Seq(row(k))
        )
        checkResult(
          s"""
             |SELECT CAST(floatField AS $i) from t5
           """.stripMargin, Seq(row(1))
        )
    }
  }

  @Test
  def testTimestampToNumeric(): Unit = {
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS TINYINT)
      """.stripMargin, Seq(row("1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS SMALLINT)
      """.stripMargin, Seq(row("1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS INT)
      """.stripMargin, Seq(row("1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS BIGINT)
      """.stripMargin, Seq(row("1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS FLOAT)
      """.stripMargin, Seq(row("1.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS DOUBLE)
      """.stripMargin, Seq(row("1.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1970-01-01 00:00:01.234' AS TIMESTAMP) AS DECIMAL(10, 2))
      """.stripMargin, Seq(row("1.23"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS TINYINT)
      """.stripMargin, Seq(row("-1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS SMALLINT)
      """.stripMargin, Seq(row("-1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS INT)
      """.stripMargin, Seq(row("-1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS BIGINT)
      """.stripMargin, Seq(row("-1"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS FLOAT)
      """.stripMargin, Seq(row("-1.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS DOUBLE)
      """.stripMargin, Seq(row("-1.234"))
    )
    checkResult(
      """
        |SELECT CAST(CAST('1969-12-31 23:59:58.766' AS TIMESTAMP) AS DECIMAL(10, 2))
      """.stripMargin, Seq(row("-1.23"))
    )
  }

  @Test
  def testTimestampWithNullable(): Unit = {
    checkResult(
      """
        |SELECT a from t5
      """.stripMargin, Seq(row("2018-06-08 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT a from t5 where a = timestamp '2018-06-08 00:00:00.0'
      """.stripMargin, Seq(row("2018-06-08 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' second from t5
      """.stripMargin, Seq(row("2018-06-08 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' minute from t5
      """.stripMargin, Seq(row("2018-06-08 00:01:00.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' hour from t5
      """.stripMargin, Seq(row("2018-06-08 01:00:00.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' day from t5
      """.stripMargin, Seq(row("2018-06-09 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' month from t5
      """.stripMargin, Seq(row("2018-07-08 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT a + interval '1' year from t5
      """.stripMargin, Seq(row("2019-06-08 00:00:00.0"))
    )
  }

  @Test
  def testTimestampLiteralAndInterval(): Unit = {
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0'
      """.stripMargin, Seq(row("2018-06-08 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' = timestamp '2018-06-08 00:00:00.0'
      """.stripMargin, Seq(row(true))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' second
      """.stripMargin, Seq(row("2018-06-08 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' minute
      """.stripMargin, Seq(row("2018-06-08 00:01:00.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' hour
      """.stripMargin, Seq(row("2018-06-08 01:00:00.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' day
      """.stripMargin, Seq(row("2018-06-09 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' month
      """.stripMargin, Seq(row("2018-07-08 00:00:00.0"))
    )
    checkResult(
      """
        |SELECT timestamp '2018-06-08 00:00:00.0' + interval '1' year
      """.stripMargin, Seq(row("2019-06-08 00:00:00.0"))
    )
  }

  @Test
  def testStringCastToTimestampAndInterval(): Unit = {
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' second
      """.stripMargin, Seq(row("1970-01-01 00:00:02.0"))
    )
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' minute
      """.stripMargin, Seq(row("1970-01-01 00:01:01.0"))
    )
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' hour
      """.stripMargin, Seq(row("1970-01-01 01:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' day
      """.stripMargin, Seq(row("1970-01-02 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' month
      """.stripMargin, Seq(row("1970-02-01 00:00:01.0"))
    )
    checkResult(
      """
        |SELECT CAST('1970-01-01 00:00:01' AS TIMESTAMP) + interval '1' year
      """.stripMargin, Seq(row("1971-01-01 00:00:01.0"))
    )
  }

  @Test
  def testNumericCastToTimestampAndInterval(): Unit = {
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' second
      """.stripMargin, Seq(row("1970-01-01 00:00:02.123"))
    )
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' minute
      """.stripMargin, Seq(row("1970-01-01 00:01:01.123"))
    )
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' hour
      """.stripMargin, Seq(row("1970-01-01 01:00:01.123"))
    )
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' day
      """.stripMargin, Seq(row("1970-01-02 00:00:01.123"))
    )
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' month
      """.stripMargin, Seq(row("1970-02-01 00:00:01.123"))
    )
    checkResult(
      """
        |SELECT CAST(1.123 AS TIMESTAMP) + interval '1' year
      """.stripMargin, Seq(row("1971-01-01 00:00:01.123"))
    )
  }

  @Test
  def testIllegalStrToDate(): Unit = {
    checkResult(
      """
        |SELECT CAST('a' AS timestamp)
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT CAST('1' AS timestamp)
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT CAST('a' AS date)
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT CAST('1' AS date)
      """.stripMargin, Seq(row("0001-01-01"))
    )
    checkResult(
      """
        |SELECT CAST('a' AS time)
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT CAST('1' AS time)
      """.stripMargin, Seq(row("01:01:01"))
    )
    checkResult(
      """
        |SELECT CAST('2018-4-31' AS date)
      """.stripMargin, Seq(row(null))
    )
    checkResult(
      """
        |SELECT CAST('2016-2-29' AS date)
      """.stripMargin, Seq(row("2016-02-29"))
    )
    checkResult(
      """
        |SELECT CAST('1900-2-29' AS date)
      """.stripMargin, Seq(row(null))
    )
  }

  @Test
  def testDoubleToDecimalOverflow(): Unit = {
    checkResult(
      """
        |SELECT CAST(v AS DECIMAL(10,5)) from t7
      """.stripMargin,
      Seq(row("-1.00000"),
          row("1.00000"),
          row(null),
          row(null),
          row(null))
    )

    //for decimal(1,1)
    //0.99 should be NULL
    //1.0 should be NULL
    //1 should be NULL
    //22 shuold be NULL
    //-0.99 should be NULL
    //-1.0 should be NULL
    //-1 should be NULL
    //-22 should be NULL
    val list = List(0.99, 1.09, 1.0,
      1, 22, -0.99, -1.0, -1, -22)
    list foreach(i=>
      checkResult(
        s"""
          |SELECT CAST($i AS DECIMAL(1,1))
        """.stripMargin, Seq(row(null))
      )
    )

    checkResult(
      """
        |SELECT CAST(v AS DECIMAL(1,1)) from t8
      """.stripMargin,
      Seq(row(null),
          row(null),
          row(null),
          row(null),
          row(null),
          row(null),
          row(null),
          row(null),
          row(null))
    )
  }

  @Test
  def testOverflowDecimalToOtherType(): Unit = {
    checkResult(
      """
        |SELECT CAST(x AS DECIMAL(1,1)) from t4
      """.stripMargin, Seq(row(null))
    )
    val list = List("tinyint", "smallint", "int", "bigint", "double", "float", "boolean")
    list foreach(i => {
      checkResult(
        s"""
           |SELECT CAST(CAST(x AS DECIMAL(1,1)) AS $i) from t4
          """.stripMargin, Seq(row(null))
      )
      checkResult(
        """
          |SELECT CAST(v AS INT) FROM t9
        """.stripMargin, Seq(row(null))
      )
    })
  }

  @Test
  def testExceptWithDifferentType(): Unit = {
    checkResult(
      """
        |SELECT x FROM t4 EXCEPT
        |SELECT m FROM t4
      """.stripMargin,
      Seq(row("1.000000000000000000"))
    )
    checkResult(
      """
        |SELECT x FROM t5 EXCEPT
        |SELECT m FROM t4
      """.stripMargin,
      Seq(row(null))
    )
    checkResult(
      """
        |SELECT x FROM t4 EXCEPT
        |SELECT x FROM t5
      """.stripMargin,
      Seq(row("1"))
    )
  }

  @Test
  def testStringToFloat(): Unit = {
    val floatList = List("floatField", "doubleField")
    val typeList = List("float", "double")
    val strList = List("\'1.abc\'", "\'abc\'", "\'\\\\n\'", "\'\'")
    val opList = List("<>", "<", ">", "=")

    typeList.foreach { i =>
      strList.foreach { str =>
        val sqlQuery =
          s"""
             |SELECT CAST($str AS $i) FROM t5
           """.stripMargin
        checkResult(sqlQuery, Seq(row(null)))
      }
    }

    floatList.foreach { i =>
      strList.foreach { str =>
        opList.foreach { op =>
          val sqlQuery =
            s"""
               |SELECT $i FROM t5 WHERE $i $op $str
           """.stripMargin
          checkResult(sqlQuery, Seq())
        }
      }
    }
  }

  @Test
  def testConcatImplicitCast(): Unit = {
    checkResult(
      """
        |SELECT CONCAT('', 'hello', 123, 123.45, true, TO_DATE('2018-08-08'),
        |TO_TIMESTAMP('2018-08-08 00:00:00'), CAST(NULL as VARCHAR))
      """.stripMargin, Seq(row("hello123123.45true2018-08-082018-08-08 00:00:00.000"))
    )
  }

  @Test
  def testConcatWsImplicitCast(): Unit = {
    checkResult(
      """
        |SELECT CONCAT_WS('#', '', 'hello', 123, 123.45, true, TO_DATE('2018-08-08'),
        |TO_TIMESTAMP('2018-08-08 00:00:00'), CAST(NULL as VARCHAR))
      """.stripMargin,
      Seq(row("#hello#123#123.45#true#2018-08-08#2018-08-08 00:00:00.000"))
    )
  }

  @Test
  def testFloatToNumeric(): Unit = {
    val list = List("tinyint", "smallint", "int", "bigint")
    list.foreach { i =>
      checkResult(
        s"""
          |SELECT CAST(CAST('1' AS FLOAT) AS $i) FROM t5
        """.stripMargin, Seq(row(1))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('1' AS DOUBLE) AS $i) FROM t5
        """.stripMargin, Seq(row(1))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('aa' AS FLOAT) AS $i) FROM t5
         """.stripMargin, Seq(row(null))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('aa' AS DOUBLE) AS $i) FROM t5
         """.stripMargin, Seq(row(null))
      )
    }
  }

  @Test
  def testNumericToFloat(): Unit = {
    val list = List("tinyint", "smallint", "int", "bigint")
    list.foreach { i =>
      checkResult(
        s"""
           |SELECT CAST(CAST('1' AS $i) AS FLOAT) FROM t5
        """.stripMargin, Seq(row(1.0))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('1' AS $i) AS DOUBLE) FROM t5
        """.stripMargin, Seq(row(1.0))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('aa' AS $i) AS FLOAT) FROM t5
         """.stripMargin, Seq(row(null))
      )
      checkResult(
        s"""
           |SELECT CAST(CAST('aa' AS $i) AS DOUBLE) FROM t5
         """.stripMargin, Seq(row(null))
      )
    }
  }

  @Test
  def testNumericToNumeric(): Unit = {
    val list = List("tinyint", "smallint", "int", "bigint")
    list.foreach { i =>
      list.foreach { j =>
        checkResult(
          s"""
             |SELECT CAST(CAST('1' AS $i) AS $j) FROM t5
        """.stripMargin, Seq(row(1))
        )
        checkResult(
          s"""
             |SELECT CAST(CAST('aa' AS $i) AS $j) FROM t5
         """.stripMargin, Seq(row(null))
        )
      }
    }
  }

  @Test
  def testCallReuse(): Unit = {
    checkResult(
      s"""
         |SELECT
         |  IF(TRUE, 1, 2),
         |  IF(FALSE, 1, 2),
         |  IF(FALSE, 1.0, 2.0),
         |  IF(FALSE, CAST(1 AS BIGINT), CAST(2 AS BIGINT)),
         |  IF(FALSE, CAST(1 AS INT), CAST(2 AS INT)),
         |  IF(FALSE, CAST(1 AS DOUBLE), CAST(2 AS DOUBLE)),
         |  IF(FALSE, CAST(1 AS FLOAT), CAST(2 AS FLOAT))
       """.stripMargin,
      Seq(row(1, 2, 2.0, 2, 2, 2.0, 2.0))
    )
  }

  @Test
  def testTimeIndicatorToTimestamp(): Unit = {
    checkResult(
      s"""
         |SELECT procIndicator <> to_timestamp(0), rowIndicator <> to_timestamp(0) from t5
       """.stripMargin,
      Seq(row(true, true))
    )
  }
}
