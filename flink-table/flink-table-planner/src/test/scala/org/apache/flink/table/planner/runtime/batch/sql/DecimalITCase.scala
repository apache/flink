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

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.Seq

/**
 * Conformance test of SQL type Decimal(p,s). Served also as documentation of our Decimal behaviors.
 */
class DecimalITCase extends BatchTestBase {

  private case class Coll(colTypes: Seq[LogicalType], rows: Seq[Row])

  private var globalTableId = 0
  private def checkQueryX(
      tables: Seq[Coll],
      query: String,
      expected: Coll,
      isSorted: Boolean = false): Unit = {

    var tableId = 0
    var queryX = query
    tables.foreach {
      table =>
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
    assertThat(ts1.length).isEqualTo(ts2.length)

    assertThat(ts1.zip(ts2).forall { case (t1, t2) => isInteroperable(t1, t2) }).isTrue

    def prepareResult(isSorted: Boolean, seq: Seq[Row]) = {
      if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
    }
    val resultRows = executeQuery(resultTable)
    assertThat(prepareResult(isSorted, expected.rows))
      .isEqualTo(prepareResult(isSorted, resultRows))
  }

  private def checkQuery1(
      sourceColTypes: Seq[LogicalType],
      sourceRows: Seq[Row],
      query: String,
      expectedColTypes: Seq[LogicalType],
      expectedRows: Seq[Row],
      isSorted: Boolean = false): Unit = {
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
  implicit private class DecimalConvertor(val sc: StringContext) {
    def d(args: Any*): java.math.BigDecimal = args.length match {
      case 0 => new JBigDecimal(sc.parts.head)
      case 1 => new JBigDecimal(sc.parts.head).setScale(args.head.asInstanceOf[Int])
      case _ => throw new RuntimeException("invalid interpolation")
    }
  }

  //
  // tests
  //
  @Test // functions e.g. sin() that treat Decimal as double
  def testApproximateFunctions(): Unit = {

    import java.lang.Math._

    checkQuery1(
      Seq(DECIMAL(10, 2)),
      s1r(d"3.14"),
      "select log10(f0), ln(f0), log(f0), log2(f0) from Table1",
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(log10(3.14), Math.log(3.14), Math.log(3.14), Math.log(3.14) / Math.log(2.0))
    )

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
      s1r(pow(3.14, 3.14), pow(3.14, 0.3), pow(0.3, 3.14), pow(3.14, 0.5))
    )

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
      s1r(sin(0.12), cos(0.12), tan(0.12), 1.0 / tan(0.12))
    )

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
      s1r(d"100.000000", d"1${10}")
    )

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
      s1r(d"10.000", d"90.000", 81L)
    )
  }

  @Test
  def testJoin1(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f0",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin2(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f1",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin3(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f1=B.f0",
      Seq(LONG),
      s1r(1L))

  }

  @Test
  def testJoin4(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f2",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin5(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f2=B.f0",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin6(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery1(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      "select count(*) from Table1 A, Table1 B where A.f0=B.f3",
      Seq(LONG),
      s1r(1L))
  }

  @Test
  def testJoin7(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
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
      isSorted = true
    )
  }

  @Test
  def testSimpleNull(): Unit = {
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      Seq(row(d"100.000", null, null)),
      "select distinct(f0), f1, f2 from (select t1.f0, t1.f1, t1.f2 from Table1 t1 " +
        "union all (SELECT * FROM Table1)) order by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null)
    )
  }

  @Test
  def testAggAvgGroupBy(): Unit = {

    // null
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      "select f0, avg(f1), avg(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", null, null)
    )

    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"100.000", d"1${10}")),
      "select f0, avg(f1), avg(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", d"100.000000", d"1${10}")
    )
  }

  @Test
  def testAggMinGroupBy(): Unit = {

    // null
    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      "select f0, min(f1), min(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null)
    )

    checkQuery1(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(i => row(d"100.000", new JBigDecimal(100 - i), d"1${10}")),
      "select f0, min(f1), min(f2) from Table1 group by f0",
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", d"1.000", d"1${10}")
    )
  }

}
