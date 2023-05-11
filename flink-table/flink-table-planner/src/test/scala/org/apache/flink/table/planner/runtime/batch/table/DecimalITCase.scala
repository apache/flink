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

import org.apache.flink.table.api._
import org.apache.flink.table.api.DataTypes.{BIGINT, DECIMAL, DOUBLE, INT}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.types.DataType
import org.apache.flink.types.Row

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

import java.math.{BigDecimal => JBigDecimal}

import scala.collection.JavaConverters._
import scala.collection.Seq

/**
 * Conformance test of TableApi type Decimal(p,s). Served also as documentation of our Decimal
 * behaviors.
 */
class DecimalITCase extends BatchTestBase {

  private def checkQuery(
      sourceColTypes: Seq[DataType],
      sourceRows: Seq[Row],
      tableTransfer: Table => Table,
      expectedColTypes: Seq[DataType],
      expectedRows: Seq[Row],
      isSorted: Boolean = false): Unit = {
    val t = tEnv.fromValues(DataTypes.ROW(sourceColTypes: _*), sourceRows: _*)

    // check result schema
    val resultTable = tableTransfer(t)
    val ts2 = resultTable.getResolvedSchema.getColumnDataTypes.asScala
    assertThat(expectedColTypes.length).isEqualTo(ts2.length)

    expectedColTypes.zip(ts2).foreach { case (t1, t2) => assertThat(t1).isEqualTo(t2) }

    def prepareResult(isSorted: Boolean, seq: Seq[Row]) = {
      if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
    }

    val resultRows = executeQuery(resultTable)
    assertThat(prepareResult(isSorted, expectedRows)).isEqualTo(prepareResult(isSorted, resultRows))
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

    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      table => table.select('f0.sin, 'f0.cos, 'f0.tan, 'f0.cot),
      Seq(DOUBLE, DOUBLE, DOUBLE, DOUBLE),
      s1r(sin(0.12), cos(0.12), tan(0.12), 1.0 / tan(0.12))
    )

    checkQuery(
      Seq(DECIMAL(10, 2)),
      s1r(d"0.12"),
      table => table.select('f0.asin, 'f0.acos, 'f0.atan),
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
      table => table.select('f0.sum),
      Seq(DECIMAL(38, 3)),
      s1r(d"100.000"))

    checkQuery(
      Seq(DECIMAL(37, 0)),
      (0 until 100).map(_ => row(d"1e36")),
      table => table.select('f0.sum),
      Seq(DECIMAL(38, 0)),
      s1r(null))
  }

  @Test
  def testAggAvg(): Unit = {

    // AVG(Decimal(p,s)) => Decimal(38,s)/Decimal(20,0) => Decimal(38, max(s,6))
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"1${10}")),
      table => table.select('f0.avg, 'f1.avg),
      Seq(DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000000", d"1${10}")
    )

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
      table => table.select('f0.min, 'f0.max, 'f0.count),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), BIGINT.notNull()),
      s1r(d"10.000", d"90.000", 81L)
    )
  }

  @Test
  def testJoin1(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('a === 'f0).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testJoin2(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('a === 'f1).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testJoin3(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('b === 'f0).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )

  }

  @Test
  def testJoin4(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('a === 'f2).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testJoin5(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('c === 'f0).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testJoin6(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")

    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('a === 'f3).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testJoin7(): Unit = {
    tEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin, NestedLoopJoin")
    checkQuery(
      Seq(DECIMAL(8, 2), DECIMAL(8, 4), INT, DOUBLE),
      s1r(d"1", d"1", 1, 1.0),
      table => table.as("a", "b", "c", "d").join(table).where('a === 'f3).select(1.count),
      Seq(BIGINT.notNull()),
      s1r(1L)
    )
  }

  @Test
  def testGroupBy(): Unit = {
    checkQuery(
      Seq(DECIMAL(8, 2)),
      Seq(row(d"1"), row(d"3"), row(d"1.0"), row(d"2")),
      table => table.groupBy('f0).select(1.count),
      Seq(BIGINT.notNull()),
      Seq(row(2L), row(1L), row(1L))
    )
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
      isSorted = true
    )
  }

  @Test
  def testSimpleNull(): Unit = {
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      Seq(row(d"100.000", null, null)),
      table => table.union(table).select('f0, 'f1, 'f2).orderBy('f0),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null)
    )
  }

  @Test
  def testAggAvgGroupBy(): Unit = {

    // null
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      table => table.groupBy('f0).select('f0, 'f1.avg, 'f2.avg),
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", null, null)
    )

    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", d"100.000", d"1${10}")),
      table => table.groupBy('f0).select('f0, 'f1.avg, 'f2.avg),
      Seq(DECIMAL(6, 3), DECIMAL(38, 6), DECIMAL(38, 10)),
      s1r(d"100.000", d"100.000000", d"1${10}")
    )
  }

  @Test
  def testAggMinGroupBy(): Unit = {

    // null
    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(_ => row(d"100.000", null, null)),
      table => table.groupBy('f0).select('f0, 'f1.min, 'f2.min),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", null, null)
    )

    checkQuery(
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      (0 until 100).map(i => row(d"100.000", new JBigDecimal(100 - i), d"1${10}")),
      table => table.groupBy('f0).select('f0, 'f1.min, 'f2.min),
      Seq(DECIMAL(6, 3), DECIMAL(6, 3), DECIMAL(20, 10)),
      s1r(d"100.000", d"1.000", d"1${10}")
    )
  }

}
