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

package org.apache.flink.table.planner.runtime.batch.table

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.{BatchTableEnvUtil, BatchTestBase}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.logical.{DecimalType, LogicalType}
import org.apache.flink.types.Row

import org.junit.{Assert, Test}

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.Seq

/**
  * Conformance test of TableApi type Decimal(p,s).
  * Served also as documentation of our Decimal behaviors.
  */
class DecimalITCase extends BatchTestBase {

  private def checkQuery(
      sourceColTypes: Seq[LogicalType],
      sourceRows: Seq[Row],
      tableTransfer: Table => Table,
      expectedColTypes: Seq[LogicalType],
      expectedRows: Seq[Row],
      isSorted: Boolean = false): Unit = {
    val rowTypeInfo = new RowTypeInfo(sourceColTypes.toArray.map(fromLogicalTypeToTypeInfo): _*)
    val fieldNames = rowTypeInfo.getFieldNames.mkString(",")
    val t = BatchTableEnvUtil.fromCollection(tEnv, sourceRows, rowTypeInfo, fieldNames)

    // check result schema
    val resultTable = tableTransfer(t)
    val ts2 = resultTable.getSchema.getFieldDataTypes.map(fromDataTypeToLogicalType)
    Assert.assertEquals(expectedColTypes.length, ts2.length)

    Assert.assertTrue(expectedColTypes.zip(ts2).forall {
      case (t1, t2) => isInteroperable(t1, t2)
    })

    def prepareResult(isSorted: Boolean, seq: Seq[Row]) = {
      if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
    }

    val resultRows = executeQuery(resultTable)
    Assert.assertEquals(
      prepareResult(isSorted, expectedRows),
      prepareResult(isSorted, resultRows))
  }

  // a Seq of one Row
  private def s1r(args: Any*): Seq[Row] = Seq(row(args: _*))

  private def expectOverflow(action: () => Unit): Unit = {
    val ex: Throwable =
      try {
        action()
        throw new Exception("query succeeds and matches expected results")
      } catch {
        case e: Throwable => e
      }

    def isOverflow(ex: Throwable): Boolean = ex match {
      case null => false
      case _ => ex.getMessage.contains("out of range") || isOverflow(ex.getCause)
    }

    if (!isOverflow(ex)) {
      throw new AssertionError("Overflow exception expected, but not thrown.", ex)
    }
  }

  private def DECIMAL = (p: Int, s: Int) => new DecimalType(p, s)

  private def BOOL = DataTypes.BOOLEAN.getLogicalType

  private def INT = DataTypes.INT.getLogicalType

  private def LONG = DataTypes.BIGINT.getLogicalType

  private def DOUBLE = DataTypes.DOUBLE.getLogicalType

  private def STRING = DataTypes.STRING.getLogicalType

  // d"xxx" => new BigDecimal("xxx")
  // d"xxx$yy" => new BigDecimal("xxx").setScale(yy)
  private implicit class DecimalConvertor(val sc: StringContext) {
    def d(args: Any*): java.math.BigDecimal = args.length match {
      case 0 => new JBigDecimal(sc.parts.head)
      case 1 => new JBigDecimal(sc.parts.head).setScale(args.head.asInstanceOf[Int])
      case _ => throw new RuntimeException("invalid interpolation")
    }
  }

  //
  // tests
  //

  @Test
  def testDataSource(): Unit = {

    // the most basic case

    checkQuery(
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"),
      table => table.select('*),
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"))

    // data from source are rounded to their declared scale before entering next step
    checkQuery(
      Seq(DECIMAL(7, 2)),
      s1r(d"100.004"),
      table => table.select('f0, 'f0 + 'f0), // 100.00+100.00
      Seq(DECIMAL(7, 2), DECIMAL(8, 2)),
      s1r(d"100.00", d"200.00")) // not 200.008=>200.01

    // trailing zeros are padded to the scale
    checkQuery(
      Seq(DECIMAL(7, 2)),
      s1r(d"100.1"),
      table => table.select('f0, 'f0 + 'f0), // 100.00+100.00
      Seq(DECIMAL(7, 2), DECIMAL(8, 2)),
      s1r(d"100.10", d"200.20"))

    // source data is within precision after rounding
    checkQuery(
      Seq(DECIMAL(5, 2)),
      s1r(d"100.0040"), // p=7 => rounding => p=5
      table => table.select('f0, 'f0 + 'f0), // 100.00+100.00
      Seq(DECIMAL(5, 2), DECIMAL(6, 2)),
      s1r(d"100.00", d"200.00"))

    // source data overflows over precision (after rounding)
    checkQuery(
      Seq(DECIMAL(2, 0)),
      s1r(d"123"),
      table => table.select('*),
      Seq(DECIMAL(2, 0)),
      s1r(null))

    checkQuery(
      Seq(DECIMAL(4, 2)),
      s1r(d"123.0000"),
      table => table.select('*),
      Seq(DECIMAL(4, 2)),
      s1r(null))
  }

  @Test
  def testUnaryPlusMinus(): Unit = {

    checkQuery(
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"),
      table => table.select( + 'f0, - 'f1, - (( + 'f0) - ( - 'f1))),
      Seq(DECIMAL(10, 0), DECIMAL(7, 2), DECIMAL(13, 2)),
      s1r(d"123", d"-123.45", d"-246.45"))
  }

  @Test
  def testPlusMinus(): Unit = {

    // see calcite ReturnTypes.DECIMAL_SUM
    // s = max(s1,s2), p-s = max(p1-s1, p2-s2) + 1
    // p then is capped at 38
    checkQuery(
      Seq(DECIMAL(10, 2), DECIMAL(10, 4)),
      s1r(d"100.12", d"200.1234"),
      table => table.select('f0 + 'f1, 'f0 - 'f1),
      Seq(DECIMAL(13, 4), DECIMAL(13, 4)),
      s1r(d"300.2434", d"-100.0034"))

    // INT => DECIMAL(10,0)
    // approximate + exact => approximate
    checkQuery(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"100.00", 200, 3.14),
      table => table.select('f0 + 'f1, 'f1 + 'f0, 'f0 + 'f2, 'f2 + 'f0),
      Seq(DECIMAL(13, 2), DECIMAL(13, 2), DOUBLE, DOUBLE),
      s1r(d"300.00", d"300.00", d"103.14", d"103.14"))

    // our result type precision is capped at 38
    // SQL2003 $6.26 -- result scale is dictated as max(s1,s2). no approximation allowed.
    // calcite -- scale is not reduced; integral part may be reduced. overflow may occur
    //   (38,10)+(38,28)=>(57,28)=>(38,28)
    // T-SQL -- scale may be reduced to keep the integral part. approximation may occur
    //   (38,10)+(38,28)=>(57,28)=>(38,9)
    checkQuery(
      Seq(DECIMAL(38, 10), DECIMAL(38, 28)),
      s1r(d"100.0123456789", d"200.0123456789012345678901234567"),
      table => table.select('f0 + 'f1, 'f0 - 'f1),
      Seq(DECIMAL(38, 28), DECIMAL(38, 28)),
      s1r(d"300.0246913578012345678901234567", d"-100.0000000000012345678901234567"))

    checkQuery(
      Seq(DECIMAL(38, 10), DECIMAL(38, 28)),
      s1r(d"1e10", d"0"),
      table => table.select('f1 + 'f0, 'f1 - 'f0 ),
      Seq(DECIMAL(38, 28), DECIMAL(38, 28)), // 10 digits integral part
      s1r(null, null))

    checkQuery(
      Seq(DECIMAL(38, 0)),
      s1r(d"5e37"),
      table => table.select('f0 + 'f0 ),
      Seq(DECIMAL(38, 0)),
      s1r(null)) // requires 39 digits

    checkQuery(
      Seq(DECIMAL(38, 0), DECIMAL(38, 0)),
      s1r(d"5e37", d"5e37"),
      table => table.select('f0 + 'f0 -'f1 ), // overflows in subexpression
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test
  def testMultiply(): Unit = {

    // see calcite ReturnTypes.DECIMAL_PRODUCT
    // s = s1+s2, p = p1+p2
    // both p&s are capped at 38
    // if s>38, result is rounded to s=38, and the integral part can only be zero
    checkQuery(
      Seq(DECIMAL(5, 2), DECIMAL(10, 4)),
      s1r(d"1.00", d"2.0000"),
      table => table.select('f0*'f0, 'f0*'f1 ),
      Seq(DECIMAL(10, 4), DECIMAL(15, 6)),
      s1r(d"1.0000", d"2.000000"))

    // INT => DECIMAL(10,0)
    // approximate * exact => approximate
    checkQuery(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"1.00", 200, 3.14),
      table => table.select('f0*'f1, 'f1*'f0, 'f0*'f2, 'f2*'f0 ),
      Seq(DECIMAL(20, 2), DECIMAL(20, 2), DOUBLE, DOUBLE),
      s1r(d"200.00", d"200.00", 3.14, 3.14))

    // precision is capped at 38; scale will not be reduced (unless over 38)
    // similar to plus&minus, and calcite behavior is different from T-SQL.
    checkQuery(
      Seq(DECIMAL(30, 6), DECIMAL(30, 10)),
      s1r(d"1", d"2"),
      table => table.select('f0*'f0, 'f0*'f1 ),
      Seq(DECIMAL(38, 12), DECIMAL(38, 16)),
      s1r(d"1${12}", d"2${16}"))

    checkQuery(
      Seq(DECIMAL(30, 20)),
      s1r(d"0.1"),
      table => table.select('f0*'f0 ),
      Seq(DECIMAL(38, 38)), // (60,40)=>(38,38)
      s1r(d"0.01${38}"))

    // scalastyle:off
    // we don't have this ridiculous behavior:
    //   https://blogs.msdn.microsoft
    //   .com/sqlprogrammability/2006/03/29/multiplication-and-division-with-numerics/
    // scalastyle:on
    checkQuery(
      Seq(DECIMAL(38, 10), DECIMAL(38, 10)),
      s1r(d"0.0000006", d"1.0"),
      table => table.select('f0*'f1 ),
      Seq(DECIMAL(38, 20)),
      s1r(d"0.0000006${20}"))

    // result overflow
    checkQuery(
      Seq(DECIMAL(38, 0)),
      s1r(d"1e19"),
      table => table.select('f0*'f0 ),
      Seq(DECIMAL(38, 0)),
      s1r(null))

    checkQuery(
      Seq(DECIMAL(30, 20)),
      s1r(d"1.0"),
      table => table.select('f0*'f0 ),
      Seq(DECIMAL(38, 38)), // (60,40)=>(38,38), no space for integral part
      s1r(null))
  }

  @Test
  def testDivide(): Unit = {

//    // the default impl of Calcite apparently borrows from T-SQL, but differs in details.
//    // Flink overrides it to follow T-SQL exactly. See FlinkTypeFactory.createDecimalQuotient()
    checkQuery( // test 1/3 in different scales
      Seq(DECIMAL(20, 2), DECIMAL(2, 1), DECIMAL(4, 3), DECIMAL(20, 10), DECIMAL(20, 16)),
      s1r(d"1.00", d"3", d"3", d"3", d"3"),
      table => table.select('f0/'f1, 'f0/'f2, 'f0/'f3, 'f0/'f4 ),
      Seq(DECIMAL(25, 6), DECIMAL(28, 7), DECIMAL(38, 10), DECIMAL(38, 6)),
      s1r(d"0.333333", d"0.3333333", d"0.3333333333", d"0.333333"))

    // INT => DECIMAL(10,0)
    // approximate / exact => approximate
    checkQuery(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"1.00", 2, 3.0),
      table => table.select('f0/'f1, 'f1/'f0, 'f0/'f2, 'f2/'f0 ),
      Seq(DECIMAL(21, 13), DECIMAL(23, 11), DOUBLE, DOUBLE),
      s1r(d"0.5${13}", d"2${11}", 1.0 / 3.0, 3.0 / 1.0))

    // result overflow, because result type integral part is reduced
    checkQuery(
      Seq(DECIMAL(30, 0), DECIMAL(30, 20)),
      s1r(d"1e20", d"1e-15"),
      table => table.select('f0/'f1 ),
      Seq(DECIMAL(38, 6)),
      s1r(null))
  }

  @Test
  def testMod(): Unit = {
    // signs. consistent with Java's % operator.
    checkQuery(
      Seq(DECIMAL(1, 0), DECIMAL(1, 0)),
      s1r(d"3", d"5"),
      table => table.select('f0 % 'f1, (-'f0) % 'f1,'f0 % (-'f1), (-'f0) % (-'f1)),
      Seq(DECIMAL(1, 0), DECIMAL(1, 0), DECIMAL(1, 0), DECIMAL(1, 0)),
      s1r(3 % 5, (-3) % 5, 3 % (-5), (-3) % (-5)))

    // rounding in case s1>s2. note that SQL2003 requires s1=s2=0.
    // (In T-SQL, s2 is expanded to s1, so that there's no rounding.)
    checkQuery(
      Seq(DECIMAL(10, 4), DECIMAL(10, 2)),
      s1r(d"3.1234", d"5"),
      table => table.select('f0 % 'f1),
      Seq(DECIMAL(10, 2)),
      s1r(d"3.12"))
  }

  @Test // functions that treat Decimal as exact value
  def testExactFunctions(): Unit = {
    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      table => table.select('f0.floor, 'f0.ceil),
      Seq(DECIMAL(10, 0), DECIMAL(10, 0)),
      s1r(d"3", d"4"))

    // calcite: SIGN(Decimal(p,s))=>Decimal(p,s)
    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      table => table.select('f0.sign, (-'f0).sign, ('f0 - 'f0).sign ),
      Seq(DECIMAL(10, 2), DECIMAL(10, 2), DECIMAL(11, 2)),
      s1r(d"1.00", d"-1.00", d"0.00"))

    // ROUND(Decimal(p,s)[,INT])
    checkQuery(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      table => table.select('f0.round(0), 'f0.round(0)),
      Seq(DECIMAL(8, 0), DECIMAL(8, 0)),
      s1r(d"647", d"647"))

    checkQuery(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      table => table.select('f0.round(1), 'f0.round(2), 'f0.round(3), 'f0.round(4) ),
      Seq(DECIMAL(9, 1), DECIMAL(10, 2), DECIMAL(10, 3), DECIMAL(10, 3)),
      s1r(d"646.6", d"646.65", d"646.646", d"646.646"))

    checkQuery(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      table => table.select('f0.round(-1), 'f0.round(-2), 'f0.round(-3), 'f0.round(-4) ),
      Seq(DECIMAL(8, 0), DECIMAL(8, 0), DECIMAL(8, 0), DECIMAL(8, 0)),
      s1r(d"650", d"600", d"1000", d"0"))

    checkQuery(
      Seq(DECIMAL(4, 2)),
      s1r(d"99.99"),
      table => table.select('f0.round(1), (-'f0).round(1), 'f0.round(-1), (-'f0).round(-1) ),
      Seq(DECIMAL(4, 1), DECIMAL(4, 1), DECIMAL(3, 0), DECIMAL(3, 0)),
      s1r(d"100.0", d"-100.0", d"100", d"-100"))

    checkQuery(
      Seq(DECIMAL(38, 0)),
      s1r(d"1E38".subtract(d"1")),
      table => table.select('f0.round(-1) ),
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test // functions e.g. sin() that treat Decimal as double
  def testApproximateFunctions(): Unit = {

    import java.lang.Math._

    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      table => table.select('f0.sin, 'f0.cos, 'f0.tan, 'f0.cot ),
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(sin(0.12), cos(0.12), tan(0.12), 1.0 / tan(0.12)))

    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      table => table.select('f0.asin, 'f0.acos, 'f0.atan ),
      Seq(DOUBLE, DOUBLE, DOUBLE),
      s1r(asin(0.12), acos(0.12), atan(0.12)))

    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      table => table.select('f0.degrees, 'f0.radians),
      Seq(DOUBLE, DOUBLE),
      s1r(toDegrees(0.12), toRadians(0.12)))
  }

  @Test
  def testAggSum(): Unit = {

    // SUM(Decimal(p,s))=>Decimal(38,s)
    checkQuery(
      Seq(DECIMAL(6, 3)),
      (0 until 100).map(_ => row(d"1.000")),
      table => table.select('f0.sum ),
      Seq(DECIMAL(38, 3)),
      s1r(d"100.000"))

    checkQuery(
      Seq(DECIMAL(37, 0)),
      (0 until 100).map(_ => row(d"1e36")),
      table => table.select('f0.sum ),
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test
  def testAggAvg(): Unit = {

    // AVG(Decimal(p,s)) => Decimal(38,s)/Decimal(20,0) => Decimal(38, max(s,6))
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"1${10}")),
      table => table.select('f0.avg, 'f1.avg ),
      Seq(DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000000", d"1${10}"))

    checkQuery(
      Seq(DECIMAL(37, 0)),
      (0 until 100).map(_ => row(d"1e36")),
      table => table.select('f0.avg),
      Seq(DECIMAL(38, 6)),
      s1r(null))
  }

  @Test
  def testAggMinMaxCount(): Unit = {

    // MIN/MAX(T) => T
    checkQuery(
      Seq(DECIMAL(6, 3)),
      (10 to 90).map(i => row(java.math.BigDecimal.valueOf(i))),
      table => table.select('f0.min, 'f0.max, 'f0.count ),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), LONG),
      s1r(d"10.000", d"90.000", 81L))
  }

  @Test
  def testCast(): Unit = {

    // String, numeric/Decimal => Decimal
    checkQuery(
      Seq(DECIMAL(8, 2), INT, DOUBLE, STRING),
      s1r(d"3.14", 3, 3.14, "3.14"),
      table => table.select('f0.cast(DataTypes.DECIMAL(8,4)),
        'f1.cast(DataTypes.DECIMAL(8,4)),
        'f2.cast(DataTypes.DECIMAL(8,4)),
        'f3.cast(DataTypes.DECIMAL(8,4)) ),
      Seq(DECIMAL(8, 4), DECIMAL(8, 4), DECIMAL(8, 4), DECIMAL(8, 4)),
      s1r(d"3.1400", d"3.0000", d"3.1400", d"3.1400"))

    // round up
    checkQuery(
      Seq(DECIMAL(8, 2), DOUBLE, STRING),
      s1r(d"3.15", 3.15, "3.15"),
      table => table.select(
        'f0.cast(DataTypes.DECIMAL(8,1)),
        'f1.cast(DataTypes.DECIMAL(8,1)),
        'f2.cast(DataTypes.DECIMAL(8,1))),
      Seq(DECIMAL(8, 1), DECIMAL(8, 1), DECIMAL(8, 1)),
      s1r(d"3.2", d"3.2", d"3.2"))

    checkQuery(
      Seq(DECIMAL(4, 2)),
      s1r(d"13.14"),
      table => table.select('f0.cast(DataTypes.DECIMAL(3,2)) ),
      Seq(DECIMAL(3, 2)),
      s1r(null))

    checkQuery(
      Seq(STRING),
      s1r("13.14"),
      table => table.select('f0.cast(DataTypes.DECIMAL(3,2)) ),
      Seq(DECIMAL(3, 2)),
      s1r(null))

    // Decimal => String, numeric
    checkQuery(
      Seq(DECIMAL(4, 2)),
      s1r(d"1.99"),
      table => table.select(
        'f0.cast(DataTypes.VARCHAR(64)),
        'f0.cast(DataTypes.DOUBLE),
        'f0.cast(DataTypes.INT)),
      Seq(STRING, DOUBLE, INT),
      s1r("1.99", 1.99, 1))

    checkQuery(
      Seq(DECIMAL(10, 0), DECIMAL(10, 0)),
      s1r(d"-2147483648", d"2147483647"),
      table => table.select('f0.cast(DataTypes.INT), 'f1.cast(DataTypes.INT)),
      Seq(INT, INT),
      s1r(-2147483648, 2147483647))
  }

  @Test
  def testEquality(): Unit = {

    // expressions that test equality.
    //   =, CASE, NULLIF, IN, IS DISTINCT FROM

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select('f0==='f1, 'f0==='f2, 'f0==='f3, 'f1==='f0, 'f2==='f0, 'f3==='f0 ),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select('f0.in('f1), 'f0.in('f2), 'f0.in('f3),
        'f1.in('f0), 'f2.in('f0), 'f3.in('f0) ),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select(
        ('f0 === 'f1) ? (1, 0),
        ('f1 === 'f0) ?(1, 0),
        ('f0 === 'f2) ? (1, 0),
        ('f2 === 'f0) ? (1, 0),
        ('f0 === 'f3) ? (1, 0),
        ('f3 === 'f0) ? (1, 0)),
      Seq(INT, INT, INT, INT, INT, INT),
      s1r(1, 1, 1, 1, 1, 1))
  }

  @Test
  def testComparison(): Unit = {

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select('f0<'f1, 'f0<'f2, 'f0<'f3, 'f1<'f0, 'f2<'f0, 'f3<'f0 ),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(false, false, false, false, false, false))

    // no overflow during type conversion.
    // conceptually both operands are promoted to infinite precision before comparison.
    checkQuery(
      Seq(DECIMAL(1, 0), DECIMAL(2, 0), INT, DOUBLE),
      s1r(d"1", d"99", 99, 99.0),
      table => table.select('f0<'f1, 'f0<'f2, 'f0<'f3, 'f1<'f0, 'f2<'f0, 'f3<'f0 ),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, false, false, false))

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select(
        'f0.between('f1, 1),
        'f1.between('f0, 1),
        'f0.between('f2, 1),
        'f2.between('f0, 1),
        'f0.between('f3, 1),
        'f3.between('f0, 1)),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.select(
        'f0.between(0, 'f1),
        'f1.between(0, 'f0),
        'f0.between(0, 'f2),
        'f2.between(0, 'f0),
        'f0.between(0, 'f3),
        'f3.between(0, 'f0)),
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))
  }

  @Test
  def testJoin1(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('a === 'f0).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin2(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('a === 'f1).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin3(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('b === 'f0).select(1.count),
      Seq(LONG),
      s1r(1L))

  }

  @Test
  def testJoin4(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('a === 'f2).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin5(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('c === 'f0).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin6(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('a === 'f3).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin7(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as('a, 'b, 'c, 'd).join(table).where('a === 'f3).select(1.count),
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testGroupBy(): Unit = {
    checkQuery(
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1"), row(d"3"), row(d"1.0"), row(d"2")),
      table => table.groupBy('f0).select(1.count),
      Seq(LONG),
      Seq(row(2L), row(1L), row(1L)))
  }

  @Test
  def testOrderBy(): Unit = {
    env.setParallelism(1) // set sink parallelism to 1
    checkQuery(
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1"), row(d"3"), row(d"1.0"), row(d"2")),
      table => table.select('f0).orderBy('f0),
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1.00"), row(d"1.00"), row(d"2.00"), row(d"3.00")),
      isSorted = true)
  }

  @Test
  def testSimpleNull(): Unit = {
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      Seq(row(d"100.000", null, null)),
      table => table.union(table).select('f0, 'f1, 'f2).orderBy('f0),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null))
  }

  @Test
  def testAggAvgGroupBy(): Unit = {

    // null
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      table => table.groupBy('f0).select('f0, 'f1.avg, 'f2.avg),
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", null, null))

    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"100.000", d"1${10}")),
      table => table.groupBy('f0).select('f0, 'f1.avg, 'f2.avg),
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", d"100.000000", d"1${10}"))
  }

  @Test
  def testAggMinGroupBy(): Unit = {

    // null
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      table => table.groupBy('f0).select('f0, 'f1.min, 'f2.min),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null))

    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(i => row(d"100.000", new JBigDecimal(100 - i), d"1${10}")),
      table => table.groupBy('f0).select('f0, 'f1.min, 'f2.min),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", d"1.000", d"1${10}"))
  }

}
