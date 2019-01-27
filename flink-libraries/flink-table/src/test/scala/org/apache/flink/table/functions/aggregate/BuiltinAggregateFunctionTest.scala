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
package org.apache.flink.table.functions.aggregate

import java.math.{BigDecimal => JBigDecimal}

import org.apache.flink.api.common.typeinfo.{BigDecimalTypeInfo, Types}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.{ConcatAgg, SingleValue}
import org.apache.flink.table.dataformat.BinaryRow
import org.apache.flink.table.typeutils.BaseRowTypeInfo
import org.junit.{Ignore, Test}

class BuiltinAggregateFunctionTest extends AggregateFunctionTestBase {

  val testDataType = new BaseRowTypeInfo(
                                         Types.BOOLEAN,
                                         Types.BYTE,
                                         Types.SHORT,
                                         Types.INT,
                                         Types.LONG,
                                         Types.DOUBLE,
                                         BigDecimalTypeInfo.of(20, 0),
                                         Types.STRING,
                                         Types.SQL_DATE,
                                         Types.SQL_TIME,
                                         Types.SQL_TIMESTAMP)

  val cornerData = Seq(
    getNullRowForResultType(testDataType),
    row(testDataType, null, 0.toByte, 0.toShort, 0, 0L, 0.0, new JBigDecimal(0), "", 0, 0, 0L),
    row(
      testDataType,
      false,
      Byte.MinValue,
      Short.MinValue,
      Int.MinValue,
      Long.MinValue,
      Double.MinValue,
      new JBigDecimal(Long.MinValue),
      "",
      -100,
      -100,
      -100L
    ),
    row(
      testDataType,
      true,
      Byte.MaxValue,
      Short.MaxValue,
      Int.MaxValue,
      Long.MaxValue,
      Double.MaxValue,
      new JBigDecimal(Long.MaxValue),
      "",
      100,
      100,
      100L)
  )

  val normalData = Seq(
    getNullRowForResultType(testDataType),
    row(testDataType, false, 0.toByte, 0.toShort, 0, 0L, 0.0, new JBigDecimal(0), "111", 0, 0, 0L),
    row(testDataType, true, 1.toByte, 2.toShort, 3, 4L, 5.0, new JBigDecimal(8), "aaa", 9, 10, 11L),
    row(testDataType, false, -10.toByte,  -9.toShort, -8, -7L, -6.0, new JBigDecimal(-5), "zzz",
      -4, -3, -2L)
  )

  val nullData = Seq(
    getNullRowForResultType(testDataType)
  )

  val emptyData: Seq[BinaryRow] = Nil

  private def getTestResultType: BaseRowTypeInfo = {
    new BaseRowTypeInfo(
                        Types.BOOLEAN,
                        Types.BYTE,
                        Types.SHORT,
                        Types.INT,
                        Types.LONG,
                        Types.DOUBLE,
                        BigDecimalTypeInfo.of(20, 0),
                        Types.STRING,
                        Types.SQL_DATE,
                        Types.SQL_TIME,
                        Types.SQL_TIMESTAMP)
  }

  private def getNullRowForResultType(resultType: BaseRowTypeInfo): BinaryRow = {
    row(resultType, null, null, null, null, null, null, null, null, null, null, null)
  }

  @Test
  def testMax(): Unit = {
    val getMaxAggColumnExprSeq = {
      Seq('f0.max, 'f1.max, 'f2.max, 'f3.max, 'f4.max, 'f5.max, 'f6.max, 'f7.max, 'f8.max, 'f9.max,
        'f10.max)
    }

    val resultType = getTestResultType

    testAggregateFunctions(
      cornerData,
      testDataType,
      getMaxAggColumnExprSeq,
      resultType,
      row(
        resultType,
        true,
        Byte.MaxValue,
        Short.MaxValue,
        Int.MaxValue,
        Long.MaxValue,
        Double.MaxValue,
        new JBigDecimal(Long.MaxValue),
        "",
        100,
        100,
        100L),
      resultType
    )

    testAggregateFunctions(
      normalData,
      testDataType,
      getMaxAggColumnExprSeq,
      resultType,
      row(resultType, true, 1.toByte, 2.toShort, 3, 4L, 5.0, new JBigDecimal(8), "zzz",
        9, 10, 11L),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      getMaxAggColumnExprSeq,
      resultType,
      getNullRowForResultType(resultType),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      getMaxAggColumnExprSeq,
      resultType,
      getNullRowForResultType(resultType),
      resultType
    )
  }

  @Test
  def testMin(): Unit = {
    val getMinAggColumnExprSeq = {
      Seq('f0.min, 'f1.min, 'f2.min, 'f3.min, 'f4.min, 'f5.min, 'f6.min, 'f7.min, 'f8.min, 'f9.min,
        'f10.min)
    }

    val resultType = getTestResultType

    testAggregateFunctions(
      cornerData,
      testDataType,
      getMinAggColumnExprSeq,
      resultType,
      row(
        resultType,
        false,
        Byte.MinValue,
        Short.MinValue,
        Int.MinValue,
        Long.MinValue,
        Double.MinValue,
        new JBigDecimal(Long.MinValue),
        "",
        -100,
        -100,
        -100L),
      resultType
    )

    testAggregateFunctions(
      normalData,
      testDataType,
      getMinAggColumnExprSeq,
      resultType,
      row(resultType, false, -10.toByte, -9.toShort, -8, -7L, -6.0, new JBigDecimal(-5), "111", -4,
        -3, -2L),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      getMinAggColumnExprSeq,
      resultType,
      getNullRowForResultType(resultType),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      getMinAggColumnExprSeq,
      resultType,
      getNullRowForResultType(resultType),
      resultType
    )
  }

  @Test
  def testSum(): Unit = {
    val getSumAggColumnExprSeq = {
      Seq('f1.sum, 'f2.sum, 'f3.sum, 'f4.sum, 'f5.sum, 'f6.sum)
    }

    val resultType = new BaseRowTypeInfo(
                                         Types.BYTE,
                                         Types.SHORT,
                                         Types.INT,
                                         Types.LONG,
                                         Types.DOUBLE,
                                         BigDecimalTypeInfo.of(38, 0))

    testAggregateFunctions(
      normalData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, -9.toByte, -7.toShort, -5, -3L, -1.0, new JBigDecimal(3)),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )
  }

  @Test
  def testSum0(): Unit = {
    val getSum0AggColumnExprSeq = {
      Seq('f1.sum0, 'f2.sum0, 'f3.sum0, 'f4.sum0, 'f5.sum0, 'f6.sum0)
    }

    val resultType = new BaseRowTypeInfo(
                                         Types.BYTE,
                                         Types.SHORT,
                                         Types.INT,
                                         Types.LONG,
                                         Types.DOUBLE,
                                         BigDecimalTypeInfo.of(38, 0))

    testAggregateFunctions(
      normalData,
      testDataType,
      getSum0AggColumnExprSeq,
      resultType,
      row(resultType, -9.toByte, -7.toShort, -5, -3L, -1.0, new JBigDecimal(3)),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      getSum0AggColumnExprSeq,
      resultType,
      row(resultType, 0.toByte, 0.toShort, 0, 0L, 0D, new JBigDecimal(0)),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      getSum0AggColumnExprSeq,
      resultType,
      row(resultType, 0.toByte, 0.toShort, 0, 0L, 0D, new JBigDecimal(0)),
      resultType
    )
  }

  @Test
  def testIncrSum(): Unit = {
    val getSumAggColumnExprSeq = {
      Seq('f1.incr_sum, 'f2.incr_sum, 'f3.incr_sum, 'f4.incr_sum, 'f5.incr_sum, 'f6.incr_sum)
    }

    val resultType = new BaseRowTypeInfo(
                                         Types.BYTE,
                                         Types.SHORT,
                                         Types.INT,
                                         Types.LONG,
                                         Types.DOUBLE,
                                         BigDecimalTypeInfo.of(38, 0))

    testAggregateFunctions(
      normalData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, 1.toByte, 2.toShort, 3, 4L, 5.0, new JBigDecimal(8)),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      getSumAggColumnExprSeq,
      resultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )
  }

  @Test
  def testAvg(): Unit = {
    val localResultType = new BaseRowTypeInfo(
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.LONG,
                                              Types.DOUBLE,
                                              Types.LONG,
                                              BigDecimalTypeInfo.of(38, 0),
                                              Types.LONG)

    val resultType = new BaseRowTypeInfo(
                                         Types.DOUBLE,
                                         Types.DOUBLE,
                                         Types.DOUBLE,
                                         Types.DOUBLE,
                                         Types.DOUBLE,
                                         BigDecimalTypeInfo.of(38, 6))

    testAggregateFunctions(
      normalData,
      testDataType,
      Seq('f1.avg, 'f2.avg, 'f3.avg, 'f4.avg, 'f5.avg, 'f6.avg),
      localResultType,
      row(resultType, -9.0 / 3, -7.0 / 3, -5.0 / 3, -3.0 / 3, -1.0 / 3,
        new JBigDecimal(1).setScale(6)),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      Seq('f1.avg, 'f2.avg, 'f3.avg, 'f4.avg, 'f5.avg, 'f6.avg),
      localResultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      Seq('f1.avg, 'f2.avg, 'f3.avg, 'f4.avg, 'f5.avg, 'f6.avg),
      localResultType,
      row(resultType, null, null, null, null, null, null),
      resultType
    )
  }

  @Test @Ignore
  def testCount(): Unit = {
    val resultType = new BaseRowTypeInfo(
      Types.LONG,
      Types.LONG,
      Types.LONG,
      Types.LONG,
      Types.LONG,
      Types.LONG)

    testAggregateFunctions(
      cornerData,
      testDataType,
      Seq('f0.count, 'f1.count, 'f2.count, 'f3.count, 'f4.count, 'f5.count),
      resultType,
      row(resultType, 2L, 3L, 3L, 3L, 3L, 3L),
      resultType
    )

    testAggregateFunctions(
      normalData,
      testDataType,
      Seq('f0.count, 'f1.count, 'f2.count, 'f3.count, 'f4.count, 'f5.count),
      resultType,
      row(resultType, 3L, 3L, 3L, 3L, 3L, 3L),
      resultType
    )

    testAggregateFunctions(
      nullData,
      testDataType,
      Seq('f0.count, 'f1.count, 'f2.count, 'f3.count, 'f4.count, 'f5.count),
      resultType,
      row(resultType, 0L, 0L, 0L, 0L, 0L, 0L),
      resultType
    )

    testAggregateFunctions(
      emptyData,
      testDataType,
      Seq('f0.count, 'f1.count, 'f2.count, 'f3.count, 'f4.count, 'f5.count),
      resultType,
      row(resultType, 0L, 0L, 0L, 0L, 0L, 0L),
      resultType
    )
  }

  @Test
  def testSingleValue(): Unit = {
    testSingleValueInner(Seq(
      getNullRowForResultType(testDataType)))

    testSingleValueInner(Seq(
      row(testDataType, false, 0.toByte, 0.toShort, 0, 0L, 0.0, new JBigDecimal(0), "", 0, 0, 0L)))

    testSingleValueInner(Seq())
  }

  @Test(expected = classOf[Exception])
  def testSingleValueException(): Unit = {
    testSingleValueInner(Seq(
      getNullRowForResultType(testDataType),
      row(testDataType, false, 0.toByte, 0.toShort, 0, 0L, 0.0, new JBigDecimal(0), "", 0, 0, 0L)))
  }

  def testSingleValueInner(inputData: Seq[BinaryRow]): Unit = {
    val localResultType = new BaseRowTypeInfo(
                                              Types.BOOLEAN,
                                              Types.INT,

                                              Types.BYTE,
                                              Types.INT,

                                              Types.SHORT,
                                              Types.INT,

                                              Types.INT,
                                              Types.INT,

                                              Types.LONG,
                                              Types.INT,

                                              Types.DOUBLE,
                                              Types.INT,

                                              BigDecimalTypeInfo.of(20, 0),
                                              Types.INT,

                                              Types.STRING,
                                              Types.INT,

                                              Types.SQL_DATE,
                                              Types.INT,

                                              Types.SQL_TIME,
                                              Types.INT,

                                              Types.SQL_TIMESTAMP,
                                              Types.INT)

    testAggregateFunctions(
      inputData,
      testDataType,
      Seq(SingleValue('f0), SingleValue('f1), SingleValue('f2),
        SingleValue('f3), SingleValue('f4), SingleValue('f5), SingleValue('f6), SingleValue('f7),
        SingleValue('f8), SingleValue('f9), SingleValue('f10)),
      localResultType,
      if (inputData.isEmpty) nullData.head else inputData.head,
      testDataType
    )
  }

  @Test
  def testMinMaxWithFixLengthString(): Unit = {
    val inputDataType = new BaseRowTypeInfo(
      Types.STRING
    )
    val inputData = Seq(
      row(inputDataType, "TEST"),
      row(inputDataType, "HELLO"),
      row(inputDataType, "WORLD"),
      row(inputDataType, "")
    )
    val resultType = new BaseRowTypeInfo(
      Types.STRING,
      Types.STRING
    )

    val aggExprs = Seq('f0.min, 'f0.max)

    testWithFixLengthString(
      inputData,
      inputDataType,
      aggExprs,
      resultType,
      row(resultType, "", "WORLD"),
      resultType
    )
  }

  @Test
  def testConcatAgg(): Unit = {
    val inputDataType = new BaseRowTypeInfo(
      Types.STRING,
      Types.STRING
    )
    val inputData = Seq(
      row(inputDataType, "-", "T"),
      row(inputDataType, "-", "T"),
      row(inputDataType, "-", "T"),
      row(inputDataType, "-", "T")
    )
    val localResultType = new BaseRowTypeInfo(
      Types.STRING,
      Types.STRING
    )
    val resultType = new BaseRowTypeInfo(
      Types.STRING
    )

    val aggExprs = Seq(ConcatAgg('f1, 'f0))

    testAggregateFunctions(
      inputData,
      inputDataType,
      aggExprs,
      localResultType,
      row(resultType, "T-T-T-T"),
      resultType
    )

    testAggregateFunctions(
      Seq(
        row(inputDataType, "-", null),
        row(inputDataType, "-", "T"),
        row(inputDataType, "-", "T"),
        row(inputDataType, "-", null)
      ),
      inputDataType,
      aggExprs,
      localResultType,
      row(resultType, "T-T"),
      resultType
    )

    testAggregateFunctions(
      Seq(
        row(inputDataType, "-", null),
        row(inputDataType, "-", null),
        row(inputDataType, "-", null),
        row(inputDataType, "-", null)
      ),
      inputDataType,
      aggExprs,
      localResultType,
      row(resultType, null),
      resultType
    )

    testAggregateFunctions(
      Seq(
        row(inputDataType, "-", ""),
        row(inputDataType, "-", ""),
        row(inputDataType, "-", ""),
        row(inputDataType, "-", "")
      ),
      inputDataType,
      aggExprs,
      localResultType,
      row(resultType, "---"),
      resultType
    )

  }
}
