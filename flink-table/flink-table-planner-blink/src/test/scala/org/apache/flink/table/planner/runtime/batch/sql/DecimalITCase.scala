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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.types.PlannerTypeUtils.isInteroperable
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo
import org.apache.flink.table.types.logical.{DecimalType, LogicalType}
import org.apache.flink.types.Row

import org.junit.{Assert, Test}

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.Seq

/**
  * Conformance test of SQL type Decimal(p,s).
  * Served also as documentation of our Decimal behaviors.
  */
class DecimalITCase extends BatchTestBase {

  private case class Coll(colTypes: Seq[LogicalType], rows: Seq[Row])

  private var globalTableId = 0
  private def checkQueryX(
      tables: Seq[Coll],
      query: String,
      expected: Coll,
      isSorted: Boolean = false)
    : Unit = {

    var tableId = 0
    var queryX = query
    tables.foreach{ table =>
      tableId += 1
      globalTableId += 1
      val tableName = "Table" + tableId
      val newTableName = tableName + "_" + globalTableId
      val rowTypeInfo = new RowTypeInfo(table.colTypes.toArray.map(fromLogicalTypeToTypeInfo): _*)
      val fieldNames = rowTypeInfo.getFieldNames.mkString(",")
      registerCollection(newTableName, table.rows, rowTypeInfo, fieldNames)
      queryX = queryX.replace(tableName, newTableName)
    }

    // check result schema
    val resultTable = parseQuery(queryX)
    val ts1 = expected.colTypes
    val ts2 = resultTable.getSchema.getFieldDataTypes.map(fromDataTypeToLogicalType)
    Assert.assertEquals(ts1.length, ts2.length)

    Assert.assertTrue(ts1.zip(ts2).forall {
      case (t1, t2) => isInteroperable(t1, t2)
    })

    def prepareResult(isSorted: Boolean, seq: Seq[Row]) = {
      if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
    }
    val resultRows = executeQuery(resultTable)
    Assert.assertEquals(
      prepareResult(isSorted, expected.rows),
      prepareResult(isSorted, resultRows))
  }

  private def checkQuery1(
      sourceColTypes: Seq[LogicalType],
      sourceRows: Seq[Row],
      query: String,
      expectedColTypes: Seq[LogicalType],
      expectedRows: Seq[Row],
      isSorted: Boolean = false)
    : Unit = {
    checkQueryX(
      Seq(Coll(sourceColTypes, sourceRows)),
      query,
      Coll(expectedColTypes, expectedRows),
      isSorted)
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
    checkQuery1(
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"),
      "select * from Table1",
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"))

    // data from source are rounded to their declared scale before entering next step
    checkQuery1(
      Seq(DECIMAL(7, 2)),
      s1r(d"100.004"),
      "select f0, f0+f0 from Table1",  // 100.00+100.00
      Seq(DECIMAL(7, 2), DECIMAL(8, 2)),
      s1r(d"100.00", d"200.00"))  // not 200.008=>200.01

    // trailing zeros are padded to the scale
    checkQuery1(
      Seq(DECIMAL(7, 2)),
      s1r(d"100.1"),
      "select f0, f0+f0 from Table1",  // 100.00+100.00
      Seq(DECIMAL(7, 2), DECIMAL(8, 2)),
      s1r(d"100.10", d"200.20"))

    // source data is within precision after rounding
    checkQuery1(
      Seq(DECIMAL(5, 2)),
      s1r(d"100.0040"), // p=7 => rounding => p=5
      "select f0, f0+f0 from Table1",
      Seq(DECIMAL(5, 2), DECIMAL(6, 2)),
      s1r(d"100.00", d"200.00"))

    // source data overflows over precision (after rounding)
    checkQuery1(
      Seq(DECIMAL(2, 0)),
      s1r(d"123"),
      "select * from Table1",
      Seq(DECIMAL(2, 0)),
      s1r(null))

    checkQuery1(
      Seq(DECIMAL(4, 2)),
      s1r(d"123.0000"),
      "select * from Table1",
      Seq(DECIMAL(4, 2)),
      s1r(null))
  }

  @Test
  def testLiterals(): Unit = {

    checkQuery1(
      Seq(DECIMAL(1,0)),
      s1r(d"0"),
      "select 12, 12.3, 12.34 from Table1",
      Seq(INT, DECIMAL(3, 1), DECIMAL(4, 2)),
      s1r(12, d"12.3", d"12.34"))

    checkQuery1(
      Seq(DECIMAL(1,0)),
      s1r(d"0"),
      "select 123456789012345678901234567890.12345678 from Table1",
      Seq(DECIMAL(38, 8)),
      s1r(d"123456789012345678901234567890.12345678"))

    expectOverflow(()=>
      checkQuery1(
        Seq(DECIMAL(1,0)),
        s1r(d"0"),
        "select 123456789012345678901234567890.123456789 from Table1",
        Seq(DECIMAL(38, 9)),
        s1r(d"123456789012345678901234567890.123456789")))
  }

  @Test
  def testUnaryPlusMinus(): Unit = {

    checkQuery1(
      Seq(DECIMAL(10, 0), DECIMAL(7, 2)),
      s1r(d"123", d"123.45"),
      "select +f0, -f1, -((+f0)-(-f1)) from Table1",
      Seq(DECIMAL(10, 0), DECIMAL(7, 2), DECIMAL(13,2)),
      s1r(d"123", d"-123.45", d"-246.45"))
  }

  @Test
  def testPlusMinus(): Unit = {

    // see calcite ReturnTypes.DECIMAL_SUM
    // s = max(s1,s2), p-s = max(p1-s1, p2-s2) + 1
    // p then is capped at 38
    checkQuery1(
      Seq(DECIMAL(10, 2), DECIMAL(10, 4)),
      s1r(d"100.12", d"200.1234"),
      "select f0+f1, f0-f1 from Table1",
      Seq(DECIMAL(13, 4), DECIMAL(13, 4)),
      s1r(d"300.2434", d"-100.0034"))

    // INT => DECIMAL(10,0)
    // approximate + exact => approximate
    checkQuery1(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"100.00", 200, 3.14),
      "select f0+f1, f1+f0, f0+f2, f2+f0 from Table1",
      Seq(DECIMAL(13, 2), DECIMAL(13, 2), DOUBLE, DOUBLE),
      s1r(d"300.00", d"300.00", d"103.14", d"103.14"))

    // our result type precision is capped at 38
    // SQL2003 $6.26 -- result scale is dictated as max(s1,s2). no approximation allowed.
    // calcite -- scale is not reduced; integral part may be reduced. overflow may occur
    //   (38,10)+(38,28)=>(57,28)=>(38,28)
    // T-SQL -- scale may be reduced to keep the integral part. approximation may occur
    //   (38,10)+(38,28)=>(57,28)=>(38,9)
    checkQuery1(
      Seq(DECIMAL(38, 10), DECIMAL(38, 28)),
      s1r(d"100.0123456789", d"200.0123456789012345678901234567"),
      "select f0+f1, f0-f1 from Table1",
      Seq(DECIMAL(38, 28), DECIMAL(38, 28)),
      s1r(d"300.0246913578012345678901234567", d"-100.0000000000012345678901234567"))

    checkQuery1(
      Seq(DECIMAL(38, 10), DECIMAL(38, 28)),
      s1r(d"1e10", d"0"),
      "select f1+f0, f1-f0 from Table1",
      Seq(DECIMAL(38, 28), DECIMAL(38, 28)), // 10 digits integral part
      s1r(null, null))

    checkQuery1(
      Seq(DECIMAL(38, 0)),
      s1r(d"5e37"),
      "select f0+f0 from Table1",
      Seq(DECIMAL(38, 0)),
      s1r(null)) // requires 39 digits

    checkQuery1(
      Seq(DECIMAL(38, 0), DECIMAL(38, 0)),
      s1r(d"5e37", d"5e37"),
      "select f0+f0-f1 from Table1",  // overflows in subexpression
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test
  def testMultiply(): Unit = {

    // see calcite ReturnTypes.DECIMAL_PRODUCT
    // s = s1+s2, p = p1+p2
    // both p&s are capped at 38
    // if s>38, result is rounded to s=38, and the integral part can only be zero
    checkQuery1(
      Seq(DECIMAL(5, 2), DECIMAL(10, 4)),
      s1r(d"1.00", d"2.0000"),
      "select f0*f0, f0*f1 from Table1",
      Seq(DECIMAL(10, 4), DECIMAL(15, 6)),
      s1r(d"1.0000", d"2.000000"))

    // INT => DECIMAL(10,0)
    // approximate * exact => approximate
    checkQuery1(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"1.00", 200, 3.14),
      "select f0*f1, f1*f0, f0*f2, f2*f0 from Table1",
      Seq(DECIMAL(20, 2), DECIMAL(20, 2), DOUBLE, DOUBLE),
      s1r(d"200.00", d"200.00", 3.14, 3.14))

    // precision is capped at 38; scale will not be reduced (unless over 38)
    // similar to plus&minus, and calcite behavior is different from T-SQL.
    checkQuery1(
      Seq(DECIMAL(30, 6), DECIMAL(30, 10)),
      s1r(d"1", d"2"),
      "select f0*f0, f0*f1 from Table1",
      Seq(DECIMAL(38, 12), DECIMAL(38, 16)),
      s1r(d"1${12}", d"2${16}"))

    checkQuery1(
      Seq(DECIMAL(30, 20)),
      s1r(d"0.1"),
      "select f0*f0 from Table1",
      Seq(DECIMAL(38, 38)),  // (60,40)=>(38,38)
      s1r(d"0.01${38}"))

    // scalastyle:off
    // we don't have this ridiculous behavior:
    //   https://blogs.msdn.microsoft.com/sqlprogrammability/2006/03/29/multiplication-and-division-with-numerics/
    // scalastyle:on
    checkQuery1(
      Seq(DECIMAL(38, 10), DECIMAL(38, 10)),
      s1r(d"0.0000006", d"1.0"),
      "select f0*f1 from Table1",
      Seq(DECIMAL(38, 20)),
      s1r(d"0.0000006${20}"))

    // result overflow
    checkQuery1(
      Seq(DECIMAL(38, 0)),
      s1r(d"1e19"),
      "select f0*f0 from Table1",
      Seq(DECIMAL(38, 0)),
      s1r(null))

    checkQuery1(
      Seq(DECIMAL(30, 20)),
      s1r(d"1.0"),
      "select f0*f0 from Table1",
      Seq(DECIMAL(38, 38)),  // (60,40)=>(38,38), no space for integral part
      s1r(null))
  }

  @Test
  def testDivide(): Unit = {

    // the default impl of Calcite apparently borrows from T-SQL, but differs in details.
    // Flink overrides it to follow T-SQL exactly. See FlinkTypeFactory.createDecimalQuotient()
    checkQuery1(  // test 1/3 in different scales
      Seq(DECIMAL(20, 2), DECIMAL(2, 1), DECIMAL(4, 3), DECIMAL(20, 10), DECIMAL(20, 16)),
      s1r(d"1.00", d"3", d"3", d"3", d"3"),
      "select f0/f1, f0/f2, f0/f3, f0/f4 from Table1",
      Seq(DECIMAL(25, 6), DECIMAL(28, 7), DECIMAL(38, 10), DECIMAL(38, 6)),
      s1r(d"0.333333", d"0.3333333", d"0.3333333333", d"0.333333"))

    // INT => DECIMAL(10,0)
    // approximate / exact => approximate
    checkQuery1(
      Seq(DECIMAL(10, 2), INT, DOUBLE),
      s1r(d"1.00", 2, 3.0),
      "select f0/f1, f1/f0, f0/f2, f2/f0 from Table1",
      Seq(DECIMAL(21, 13), DECIMAL(23, 11), DOUBLE, DOUBLE),
      s1r(d"0.5${13}", d"2${11}", 1.0/3.0, 3.0/1.0))

    // result overflow, because result type integral part is reduced
    checkQuery1(
      Seq(DECIMAL(30, 0), DECIMAL(30, 20)),
      s1r(d"1e20", d"1e-15"),
      "select f0/f1 from Table1",
      Seq(DECIMAL(38, 6)),
      s1r(null))
  }

  @Test
  def testMod(): Unit = {

    // MOD(Exact1, Exact2) => Exact2
    checkQuery1(
      Seq(DECIMAL(10, 2), DECIMAL(10, 4), INT),
      s1r(d"3.00", d"5.0000", 7),
      "select mod(f0,f1), mod(f1,f0), mod(f0,f2), mod(f2,f0) from Table1",
      Seq(DECIMAL(10, 4), DECIMAL(10, 2), INT, DECIMAL(10, 2)),
      s1r(d"3.0000", d"2.00", 3, d"1.00"))

    // signs. consistent with Java's % operator.
    checkQuery1(
      Seq(DECIMAL(1, 0), DECIMAL(1, 0)),
      s1r(d"3", d"5"),
      "select mod(f0,f1), mod(-f0,f1), mod(f0,-f1), mod(-f0,-f1) from Table1",
      Seq(DECIMAL(1, 0), DECIMAL(1, 0), DECIMAL(1, 0), DECIMAL(1, 0)),
      s1r(3%5, (-3)%5, 3%(-5), (-3)%(-5)))

    // rounding in case s1>s2. note that SQL2003 requires s1=s2=0.
    // (In T-SQL, s2 is expanded to s1, so that there's no rounding.)
    checkQuery1(
      Seq(DECIMAL(10, 4), DECIMAL(10, 2)),
      s1r(d"3.1234", d"5"),
      "select mod(f0,f1) from Table1",
      Seq(DECIMAL(10, 2)),
      s1r(d"3.12"))
  }

  @Test // functions that treat Decimal as exact value
  def testExactFunctions(): Unit = {
    checkQuery1(
      Seq(DECIMAL(10, 2), DECIMAL(10, 2)),
      s1r(d"3.14", d"2.17"),
      "select if(f0>f1, f0, f1) from Table1",
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"))

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      "select abs(f0), abs(-f0) from Table1",
      Seq(DECIMAL(10, 2), DECIMAL(10, 2)),
      s1r(d"3.14", d"3.14"))

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      "select floor(f0), ceil(f0) from Table1",
      Seq(DECIMAL(10, 0), DECIMAL(10, 0)),
      s1r(d"3", d"4"))

    // calcite: SIGN(Decimal(p,s))=>Decimal(p,s)
    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      "select sign(f0), sign(-f0), sign(f0-f0) from Table1",
      Seq(DECIMAL(10, 2), DECIMAL(10, 2), DECIMAL(11, 2)),
      s1r(d"1.00", d"-1.00", d"0.00"))

    // ROUND(Decimal(p,s)[,INT])
    checkQuery1(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      "select round(f0), round(f0, 0) from Table1",
      Seq(DECIMAL(8, 0), DECIMAL(8, 0)),
      s1r(d"647", d"647"))

    checkQuery1(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      "select round(f0,1), round(f0,2), round(f0,3), round(f0,4) from Table1",
      Seq(DECIMAL(9, 1), DECIMAL(10, 2), DECIMAL(10, 3), DECIMAL(10, 3)),
      s1r(d"646.6", d"646.65", d"646.646", d"646.646"))

    checkQuery1(
      Seq(DECIMAL(10, 3)),
      s1r(d"646.646"),
      "select round(f0,-1), round(f0,-2), round(f0,-3), round(f0,-4) from Table1",
      Seq(DECIMAL(8, 0), DECIMAL(8, 0), DECIMAL(8, 0), DECIMAL(8, 0)),
      s1r(d"650", d"600", d"1000", d"0"))

    checkQuery1(
      Seq(DECIMAL(4, 2)),
      s1r(d"99.99"),
      "select round(f0,1), round(-f0,1), round(f0,-1), round(-f0,-1) from Table1",
      Seq(DECIMAL(4, 1), DECIMAL(4, 1), DECIMAL(3, 0), DECIMAL(3, 0)),
      s1r(d"100.0", d"-100.0", d"100", d"-100"))

    checkQuery1(
      Seq(DECIMAL(38, 0)),
      s1r(d"1E38".subtract(d"1")),
      "select round(f0,-1) from Table1",
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test // functions e.g. sin() that treat Decimal as double
  def testApproximateFunctions(): Unit = {

    import java.lang.Math._

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      "select log10(f0), ln(f0), log(f0), log2(f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(log10(3.14), Math.log(3.14), Math.log(3.14), Math.log(3.14)/Math.log(2.0)))

    checkQuery1(
      Seq(DECIMAL(10, 2), DOUBLE),
      s1r(d"3.14", 3.14),
      "select log(f0,f0), log(f0,f1), log(f1,f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE),
      s1r(1.0, 1.0, 1.0))

    checkQuery1(
      Seq(DECIMAL(10, 2), DOUBLE),
      s1r(d"3.14", 0.3),
      "select power(f0,f0), power(f0,f1), power(f1,f0), sqrt(f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(pow(3.14, 3.14), pow(3.14, 0.3), pow(0.3, 3.14), pow(3.14, 0.5)))

    checkQuery1(
      Seq(DECIMAL(10, 2), DOUBLE),
      s1r(d"3.14", 0.3),
      "select exp(f0), exp(f1) from Table1",
      Seq(DOUBLE, DOUBLE),
      s1r(exp(3.14), exp(0.3)))

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      "select sin(f0), cos(f0), tan(f0), cot(f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(sin(0.12), cos(0.12), tan(0.12), 1.0/tan(0.12)))

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      "select asin(f0), acos(f0), atan(f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE),
      s1r(asin(0.12), acos(0.12), atan(0.12)))

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      "select degrees(f0), radians(f0) from Table1",
      Seq(DOUBLE, DOUBLE),
      s1r(toDegrees(0.12), toRadians(0.12)))
  }

  @Test
  def testAggSum(): Unit = {

    // SUM(Decimal(p,s))=>Decimal(38,s)
    checkQuery1(
      Seq(DECIMAL(6, 3)),
      (0 until 100).map(_ => row(d"1.000")),
      "select sum(f0) from Table1",
      Seq(DECIMAL(38, 3)),
      s1r(d"100.000"))

    checkQuery1(
      Seq(DECIMAL(37, 0)),
      (0 until 100).map(_ => row(d"1e36")),
      "select sum(f0) from Table1",
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test
  def testAggAvg(): Unit = {

    // AVG(Decimal(p,s)) => Decimal(38,s)/Decimal(20,0) => Decimal(38, max(s,6))
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"1${10}")),
      "select avg(f0), avg(f1) from Table1",
      Seq(DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000000", d"1${10}"))

    checkQuery1(
      Seq(DECIMAL(37, 0)),
      (0 until 100).map(_ => row(d"1e36")),
      "select avg(f0) from Table1",
      Seq(DECIMAL(38, 6)),
      s1r(null))
  }

  @Test
  def testAggMinMaxCount(): Unit = {

    // MIN/MAX(T) => T
    checkQuery1(
      Seq(DECIMAL(6, 3)),
      (10 to 90).map(i => row(java.math.BigDecimal.valueOf(i))),
      "select min(f0), max(f0), count(f0) from Table1",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), LONG),
      s1r(d"10.000", d"90.000", 81L))
  }

  @Test
  def testCaseWhen(): Unit = {

    // result type: SQL2003 $9.23, calcite RelDataTypeFactory.leastRestrictive()
    checkQuery1(
      Seq(DECIMAL(8, 4), DECIMAL(10, 2)),
      s1r(d"0.0001", d"0.01"),
      "select case f0 when 0 then f0 else f1 end from Table1",
      Seq(DECIMAL(12, 4)),
      s1r(d"0.0100"))

    checkQuery1(
      Seq(DECIMAL(8, 4), INT),
      s1r(d"0.0001", 1),
      "select case f0 when 0 then f0 else f1 end from Table1",
      Seq(DECIMAL(14, 4)),
      s1r(d"1.0000"))

    checkQuery1(
      Seq(DECIMAL(8, 4), DOUBLE),
      s1r(d"0.0001", 3.14),
      "select case f0 when 0 then f1 else f0 end from Table1",
      Seq(DOUBLE),
      s1r(d"0.0001".doubleValue()))
  }

  @Test
  def testCast(): Unit = {

    // String, numeric/Decimal => Decimal
    checkQuery1(
      Seq(DECIMAL(8, 2), INT, DOUBLE, STRING),
      s1r(d"3.14", 3, 3.14, "3.14"),
      "select cast(f0 as Decimal(8,4)), cast(f1 as Decimal(8,4)), " +
        "cast(f2 as Decimal(8,4)), cast(f3 as Decimal(8,4)) from Table1",
      Seq(DECIMAL(8, 4), DECIMAL(8, 4), DECIMAL(8, 4), DECIMAL(8, 4)),
      s1r(d"3.1400", d"3.0000", d"3.1400", d"3.1400"))

    // round up
    checkQuery1(
      Seq(DECIMAL(8, 2), DOUBLE, STRING),
      s1r(d"3.15", 3.15, "3.15"),
      "select cast(f0 as Decimal(8,1)), cast(f1 as Decimal(8,1)), " +
        "cast(f2 as Decimal(8,1)) from Table1",
      Seq(DECIMAL(8, 1), DECIMAL(8, 1), DECIMAL(8, 1)),
      s1r(d"3.2", d"3.2", d"3.2"))

    checkQuery1(
      Seq(DECIMAL(4, 2)),
      s1r(d"13.14"),
      "select cast(f0 as Decimal(3,2)) from Table1",
      Seq(DECIMAL(3, 2)),
      s1r(null))

    checkQuery1(
      Seq(STRING),
      s1r("13.14"),
      "select cast(f0 as Decimal(3,2)) from Table1",
      Seq(DECIMAL(3, 2)),
      s1r(null))

    // Decimal => String, numeric
    checkQuery1(
      Seq(DECIMAL(4, 2)),
      s1r(d"1.99"),
      "select cast(f0 as VARCHAR(64)), cast(f0 as DOUBLE), cast(f0 as INT) from Table1",
      Seq(STRING, DOUBLE, INT),
      s1r("1.99", 1.99, 1))

    checkQuery1(
      Seq(DECIMAL(10, 0), DECIMAL(10, 0)),
      s1r(d"-2147483648", d"2147483647"),
      "select cast(f0 as INT), cast(f1 as INT) from Table1",
      Seq(INT, INT),
      s1r(-2147483648, 2147483647))
  }

  @Test
  def testEquality(): Unit = {

    // expressions that test equality.
    //   =, CASE, NULLIF, IN, IS DISTINCT FROM

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select f0=f1, f0=f2, f0=f3, f1=f0, f2=f0, f3=f0 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select f0 IN(f1), f0 IN(f2), f0 IN(f3), " +
        "f1 IN(f0), f2 IN(f0), f3 IN(f0) from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select " +
        "f0 IS DISTINCT FROM f1, f1 IS DISTINCT FROM f0, " +
        "f0 IS DISTINCT FROM f2, f2 IS DISTINCT FROM f0, " +
        "f0 IS DISTINCT FROM f3, f3 IS DISTINCT FROM f0 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(false, false, false, false, false, false))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select NULLIF(f0,f1), NULLIF(f0,f2), NULLIF(f0,f3)," +
        "NULLIF(f1,f0), NULLIF(f2,f0), NULLIF(f3,f0) from Table1",
      Seq(DECIMAL(8, 2), DECIMAL(8, 2), DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(null, null, null, null, null, null))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select " +
        "case f0 when f1 then 1 else 0 end, case f1 when f0 then 1 else 0 end, " +
        "case f0 when f2 then 1 else 0 end, case f2 when f0 then 1 else 0 end, " +
        "case f0 when f3 then 1 else 0 end, case f3 when f0 then 1 else 0 end from Table1",
      Seq(INT, INT, INT, INT, INT, INT),
      s1r(1, 1, 1, 1, 1, 1))
  }

  @Test
  def testComparison(): Unit = {

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select f0<f1, f0<f2, f0<f3, f1<f0, f2<f0, f3<f0 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(false, false, false, false, false, false))

    // no overflow during type conversion.
    // conceptually both operands are promoted to infinite precision before comparison.
    checkQuery1(
      Seq(DECIMAL(1, 0), DECIMAL(2, 0), INT, DOUBLE),
      s1r(d"1", d"99", 99, 99.0),
      "select f0<f1, f0<f2, f0<f3, f1<f0, f2<f0, f3<f0 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, false, false, false))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select " +
        "f0 between f1 and 1, f1 between f0 and 1, " +
        "f0 between f2 and 1, f2 between f0 and 1, " +
        "f0 between f3 and 1, f3 between f0 and 1 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select " +
        "f0 between 0 and f1, f1 between 0 and f0, " +
        "f0 between 0 and f2, f2 between 0 and f0, " +
        "f0 between 0 and f3, f3 between 0 and f0 from Table1",
      Seq(BOOL, BOOL, BOOL, BOOL, BOOL, BOOL),
      s1r(true, true, true, true, true, true))
  }

  @Test
  def testJoin1(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f0",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin2(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f1",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin3(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f1=B.f0",
      Seq(LONG),
      s1r(1L))

  }

  @Test
  def testJoin4(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f2",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin5(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f2=B.f0",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin6(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f3",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin7(): Unit = {
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f3=B.f0",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testGroupBy(): Unit = {
    checkQuery1(
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1"), row(d"3"), row(d"1.0"), row(d"2")),
      "select count(*) from Table1 A group by f0",
      Seq(LONG),
      Seq(row(2L), row(1L), row(1L)))
  }

  @Test
  def testOrderBy(): Unit = {
    env.setParallelism(1) // set sink parallelism to 1
    checkQuery1(
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1"), row(d"3"), row(d"1.0"), row(d"2")),
      "select f0 from Table1 A order by f0",
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1.00"), row(d"1.00"), row(d"2.00"), row(d"3.00")),
      isSorted = true)
  }

  @Test
  def testSimpleNull(): Unit = {
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      Seq(row(d"100.000", null, null)),
      "select distinct(f0), f1, f2 from (select t1.f0, t1.f1, t1.f2 from Table1 t1 " +
          "union all (SELECT * FROM Table1)) order by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null))
  }

  @Test
  def testAggAvgGroupBy(): Unit = {

    // null
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      "select f0, avg(f1), avg(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", null, null))

    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"100.000", d"1${10}")),
      "select f0, avg(f1), avg(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", d"100.000000", d"1${10}"))
  }

  @Test
  def testAggMinGroupBy(): Unit = {

    // null
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      "select f0, min(f1), min(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null))

    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(i => row(d"100.000", new JBigDecimal(100 - i), d"1${10}")),
      "select f0, min(f1), min(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", d"1.000", d"1${10}"))
  }

}
