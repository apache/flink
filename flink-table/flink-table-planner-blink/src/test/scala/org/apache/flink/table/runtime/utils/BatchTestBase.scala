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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment => ScalaExecEnv}
import org.apache.flink.table.api.java.{BatchTableEnvironment => JavaBatchTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv}
import org.apache.flink.table.api.{SqlParserException, Table, TableConfig, TableConfigOptions, TableEnvironment, TableImpl}
import org.apache.flink.table.dataformat.{BinaryRow, BinaryRowWriter}
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.plan.stats.FlinkStatistic
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.utils.BatchAbstractTestBase.DEFAULT_PARALLELISM
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.util.{BaseRowTestUtil, DiffRepository}
import org.apache.flink.types.Row

import org.apache.calcite.rel.RelNode
import org.apache.calcite.runtime.CalciteContextException
import org.apache.calcite.sql.SqlExplainLevel
import org.apache.calcite.sql.parser.SqlParseException
import org.apache.commons.lang3.SystemUtils
import org.junit.Assert._
import org.junit.rules.TestName
import org.junit.{Assert, Rule}

import java.lang.{Iterable => JIterable}
import java.util.TimeZone
import java.util.regex.Pattern

import scala.collection.JavaConverters._
import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

class BatchTestBase extends BatchAbstractTestBase {

  TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
  val conf: TableConfig = BatchTestBase.initConfigForTest(new TableConfig)
  val jobConfig = new Configuration()
  val env: ScalaExecEnv = generatorScalaTestEnv
  val javaEnv: StreamExecutionEnvironment = generatorTestEnv
  env.getConfig.enableObjectReuse()
  val tEnv: ScalaBatchTableEnv = TableEnvironment.getBatchTableEnvironment(env, conf)
  val javaTableEnv: JavaBatchTableEnv = TableEnvironment.getBatchTableEnvironment(javaEnv, conf)
  val LINE_COL_PATTERN: Pattern = Pattern.compile("At line ([0-9]+), column ([0-9]+)")
  val LINE_COL_TWICE_PATTERN: Pattern = Pattern.compile("(?s)From line ([0-9]+),"
      + " column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)")

  private lazy val diffRepository = DiffRepository.lookup(this.getClass)
  val testName: TestName = new TestName

  @Rule
  def name: TestName = testName

  /**
    * Explain ast tree nodes of table and the logical plan after optimization.
    * @param table table to explain for
    * @return string presentation of of explaining
    */
  def explainLogical(table: Table): String = {
    val ast = table.asInstanceOf[TableImpl].getRelNode
    val logicalPlan = getPlan(ast)

    s"== Abstract Syntax Tree ==" +
        System.lineSeparator +
        s"${FlinkRelOptUtil.toString(ast)}" +
        System.lineSeparator +
        s"== Optimized Logical Plan ==" +
        System.lineSeparator +
        s"$logicalPlan"
  }

  protected def generatorTestEnv: StreamExecutionEnvironment = {
    StreamExecutionEnvironment.getExecutionEnvironment
  }

  protected def generatorScalaTestEnv: ScalaExecEnv = {
    ScalaExecEnv.getExecutionEnvironment
  }

  def checkResult(sqlQuery: String, expectedResult: Seq[Row], isSorted: Boolean = false): Unit = {
    check(sqlQuery, (result: Seq[Row]) => checkSame(expectedResult, result, isSorted))
  }

  def checkSize(sqlQuery: String, expectedSize: Int): Unit = {
    check(sqlQuery, (result: Seq[Row]) => {
      if (result.size != expectedSize) {
        val errorMessage =
          s"""
             |Results
             |${
            sideBySide(
              s"== Correct Result - $expectedSize ==" +:
                  prepareResult(Seq(), isSorted = false),
              s"== Actual Result - ${result.size} ==" +:
                  prepareResult(result, isSorted = false)).mkString("\n")
          }
        """.stripMargin
        Some(errorMessage)
      } else None
    })
  }

  def verifyPlanAndCheckResult(
      sqlQuery: String,
      expectedResult: Seq[Row],
      isSorted: Boolean = false): Unit = {
    verifyPlan(sqlQuery)
    checkResult(sqlQuery, expectedResult, isSorted)
  }

  def verifyPlan(sqlQuery: String): Unit = verifyPlan(parseQuery(sqlQuery))

  def verifyPlan(table: Table): Unit = {
    val relNode = table.asInstanceOf[TableImpl].getRelNode
    val actual = SystemUtils.LINE_SEPARATOR + getPlan(relNode)
    assertEqualsOrExpand("planAfter", actual.toString, expand = false)
  }

  private def assertEqualsOrExpand(tag: String, actual: String, expand: Boolean = true): Unit = {
    val expected = s"$${$tag}"
    if (!expand) {
      diffRepository.assertEquals(this.name.getMethodName, tag, expected, actual)
      return
    }
    val expanded = diffRepository.expand(this.name.getMethodName, tag, expected)
    if (expanded != null && !expanded.equals(expected)) {
      // expected does exist, check result
      diffRepository.assertEquals(this.name.getMethodName, tag, expected, actual)
    } else {
      // expected does not exist, update
      diffRepository.expand(this.name.getMethodName, tag, actual)
    }
  }

  private def getPlan(relNode: RelNode): String = {
    val optimized = tEnv.optimize(relNode)
    FlinkRelOptUtil.toString(optimized, SqlExplainLevel.EXPPLAN_ATTRIBUTES)
  }

  def check(sqlQuery: String, checkFunc: (Seq[Row]) => Option[String]): Unit = {
    val table = parseQuery(sqlQuery)
    val result = executeQuery(table)

    checkFunc(result).foreach { results =>
      val plan = explainLogical(table)
      Assert.fail(
        s"""
           |Results do not match for query:
           |  $sqlQuery
           |$results
           |Plan:
           |  $plan
       """.stripMargin)
    }
  }

  def checkFailed(sqlQuery: String, expectedMsgPattern: String): Unit = {
    try{
      val table = parseQuery(sqlQuery)
      val _ = executeQuery(table)
      // If got here, no exception is thrown.
      if (expectedMsgPattern != null) {
        throw new AssertionError(s"Expected query to throw exception, but it did not;"
            + s" query [$sqlQuery ];"
            + s" expected [$expectedMsgPattern]")
      }
    } catch {
      case spe: SqlParserException =>
        val errMsg = spe.getMessage
        if (null == expectedMsgPattern) {
          throw new RuntimeException(s"Error while parsing query: $sqlQuery", spe)
        } else if (null == errMsg || !errMsg.matches(expectedMsgPattern)) {
          throw new RuntimeException(s"Error did not match expected [$expectedMsgPattern] while "
              + s"parsing query [$sqlQuery]", spe)
        }
      case thrown: Throwable =>
        var actualExp = thrown
        var actualMsg = actualExp.getMessage
        var actualLine = -1
        var actualColumn = -1
        var actualEndLine = 100
        var actualEndColumn = 99

        var ece: CalciteContextException = null
        var spe: SqlParseException = null
        var ex = actualExp
        var found = false
        while (null != ex && !found) {
          ex match {
            case ex1: CalciteContextException =>
              ece = ex1
              found = true
            case ex2: Throwable if ex2.getCause == ex =>
              found = true
            case _ =>
              ex = ex.getCause
          }
        }
        ex = actualExp
        found = false
        while (null != ex && !found) {
          ex match {
            case ex1: SqlParseException if ex1.getPos != null =>
              spe = ex1
              found = true
            case ex2: Throwable if ex2.getCause == ex =>
              found = true
            case _ =>
              ex = ex.getCause
          }
        }

        if (ece != null) {
          actualLine = ece.getPosLine
          actualColumn = ece.getPosColumn
          actualEndLine = ece.getEndPosLine
          actualEndColumn = ece.getEndPosColumn
          if (ece.getCause != null) {
            actualExp = ece.getCause
            actualMsg = actualExp.getMessage
          }
        } else if (spe != null) {
          actualLine = spe.getPos.getLineNum
          actualColumn = spe.getPos.getColumnNum
          actualEndLine = spe.getPos.getEndLineNum
          actualEndColumn = spe.getPos.getEndColumnNum
          if (spe.getCause != null) {
            actualExp = spe.getCause
            actualMsg = actualExp.getMessage
          }
        } else {
          val message = actualMsg
          if (message != null) {
            var matcher = LINE_COL_TWICE_PATTERN.matcher(message)
            if (matcher.matches) {
              actualLine = matcher.group(1).toInt
              actualColumn = matcher.group(2).toInt
              actualEndLine = matcher.group(3).toInt
              actualEndColumn = matcher.group(4).toInt
              actualMsg = matcher.group(5)
            }
            else {
              matcher = LINE_COL_PATTERN.matcher(message)
              if (matcher.matches) {
                actualLine = matcher.group(1).toInt
                actualColumn = matcher.group(2).toInt
              }
            }
          }
        }
        if (null == expectedMsgPattern) {
          actualExp.printStackTrace()
          fail(s"Validator threw unexpected exception; query [$sqlQuery];" +
              s" exception [$actualMsg]; class [$actualExp.getClass];" +
              s" pos [line $actualLine col $actualColumn thru line $actualLine col $actualColumn]")
        }
        if (actualMsg == null || !actualMsg.matches(expectedMsgPattern)) {
          actualExp.printStackTrace()
          fail(s"Validator threw different "
              + s"exception than expected; query [$sqlQuery];\n"
              + s" expected pattern [$expectedMsgPattern];\n"
              + s" actual [$actualMsg];\n"
              + s" pos [$actualLine"
              + s" col $actualColumn"
              + s" thru line $actualEndLine"
              + s" col $actualEndColumn].")
        }
    }
  }

  def checkEmptyResult(sqlQuery: String): Unit = {
    val table = parseQuery(sqlQuery)
    val result = executeQuery(table)

    checkEmpty(result).foreach { results =>
      Assert.fail(
        s"""
           |Results do not match for query:
           |$results
       """.stripMargin)
    }
  }

  def parseQuery(sqlQuery: String): Table = tEnv.sqlQuery(sqlQuery)

  def executeQuery(table: Table): Seq[Row] = TableUtil.collect(table.asInstanceOf[TableImpl])

  def executeQuery(sqlQuery: String): Seq[Row] = {
    val table = parseQuery(sqlQuery)
    executeQuery(table)
  }

  def explainQuery(sqlQuery: String): Unit = {
    val table = parseQuery(sqlQuery)
    executeQuery(table)
    tEnv.explain(table)
  }

  private def prepareResult(seq: Seq[Row], isSorted: Boolean) = {
    if (!isSorted) seq.map(_.toString).sortBy(s => s) else seq.map(_.toString)
  }

  def checkSame(
      expectedResult: Seq[Row],
      result: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (expectedResult.size != result.size
        || !prepareResult(expectedResult, isSorted).equals(prepareResult(result, isSorted))) {
      val errorMessage =
        s"""
           |Results
           |${
          sideBySide(
            s"== Correct Result - ${expectedResult.size} ==" +:
                prepareResult(expectedResult, isSorted),
            s"== Actual Result - ${result.size} ==" +:
                prepareResult(result, isSorted)).mkString("\n")
        }
        """.stripMargin
      Some(errorMessage)
    } else None
  }

  private def checkEmpty(result: Seq[Row]) = {
    val expectedResult = Nil
    checkSame(expectedResult, result, isSorted = true)
  }

  private def sideBySide(left: Seq[String], right: Seq[String]) = {
    val maxLeftSize = left.map(_.length).max
    val leftPadded = left ++ Seq.fill(math.max(right.size - left.size, 0))("")
    val rightPadded = right ++ Seq.fill(math.max(left.size - right.size, 0))("")

    leftPadded.zip(rightPadded).map {
      case (l, r) =>
        (if (l == r || l.startsWith("== Correct")) " " else "!") +
            l + (" " * ((maxLeftSize - l.length) + 3)) + r
    }
  }

  implicit def registerCollection(
      tableName: String,
      data: Iterable[Row],
      typeInfo: TypeInformation[Row],
      fields: String): Unit = {

    BatchTableEnvUtil.registerCollection(tEnv, tableName, data, typeInfo, fields)
  }

  implicit def registerCollectionOfJavaTableEnv(
      tableName: String,
      data: Iterable[Row],
      typeInfo: TypeInformation[Row],
      fields: String): Unit = {
    BatchTableEnvUtil.registerCollection(javaTableEnv, tableName, data, typeInfo, fields)
  }

  def registerJavaCollection[T](
      tableName: String,
      data: JIterable[T],
      typeInfo: TypeInformation[T],
      fields: String): Unit = {
    BatchTableEnvUtil.registerCollection(tEnv, tableName, data.asScala, typeInfo, fields)
  }

  def registerCollection[T](
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String,
      fieldNullables: Array[Boolean]): Unit = {
    BatchTableEnvUtil.registerCollection(
      tEnv, tableName, data, typeInfo, fields, fieldNullables, None)
  }

  def registerCollection[T](
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String,
      fieldNullables: Array[Boolean],
      statistic: FlinkStatistic): Unit = {
    BatchTableEnvUtil.registerCollection(
      tEnv, tableName, data, typeInfo, fields, fieldNullables, Some(statistic))
  }

  def registerFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      f: AggregateFunction[T, ACC]): Unit = {
    tEnv.registerFunction(name, f)
  }
}

object BatchTestBase {

  def row(args: Any*): Row = {
    val values = args.toArray
    val row = new Row(values.length)
    var i = 0
    while (i < values.length) {
      row.setField(i, values(i))
      i += 1
    }
    row
  }

  def binaryRow(types: Array[LogicalType], fields: Any*): BinaryRow = {
    assertEquals(
      "Filed count inconsistent with type information",
      fields.length,
      types.length)
    val row = new BinaryRow(fields.length)
    val writer = new BinaryRowWriter(row)
    writer.reset()
    fields.zipWithIndex.foreach { case (field, index) =>
      if (field == null) writer.setNullAt(index)
      else BaseRowTestUtil.write(writer, index, field, types(index))
    }
    writer.complete()
    row
  }

  def initConfigForTest(conf: TableConfig): TableConfig = {
    // TODO prepare for some resource config
    conf.getConf.setInteger(
      TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, DEFAULT_PARALLELISM)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM, 1)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_EXTERNAL_BUFFER_MEM, 1)
    conf.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_HASH_AGG_TABLE_MEM, 2)
    conf
  }

  def compareResult[T](expectedStrings: Array[String],
      result: Array[T],
      sort: Boolean,
      asTuples: Boolean = false): Unit = {
    val resultStringsBuffer: ArrayBuffer[String] = new ArrayBuffer[String](result.size)
    result.foreach { v =>
      v match {
        case t0: Tuple if asTuples =>
          val first: Any = t0.getField(0)
          val bld: StringBuilder = new StringBuilder(if (first == null) "null" else first.toString)
          (1 until t0.getArity).foreach {
            idx => val next = t0.getField(idx)
              bld.append(',').append(if (next == null) "null" else next.toString)
              resultStringsBuffer += bld.toString()
          }
        case _ =>
      }
      if (asTuples && !v.isInstanceOf[Tuple]) {
        throw new IllegalArgumentException(v + " is no tuple")
      }
      if (!asTuples) {
        resultStringsBuffer += (if (v == null) "null" else v.toString)
      }
    }
    val resultStrings = resultStringsBuffer.toArray
    if (sort) {
      Sorting.quickSort(expectedStrings)
      Sorting.quickSort(resultStrings)
    }
    val msg = s"Different elements in arrays: expected ${expectedStrings.length} elements " +
        s"and received ${resultStrings.length}\n " +
        s"expected: ${expectedStrings.mkString}\n " +
        s"received: ${resultStrings.mkString}"
    assertEquals(msg, expectedStrings.length, resultStrings.length)
    expectedStrings.zip(resultStrings).foreach{
      case (e, r) =>
        assertEquals(msg, e, r)
    }
  }
}
