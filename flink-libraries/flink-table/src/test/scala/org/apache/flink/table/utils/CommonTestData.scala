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

package org.apache.flink.table.utils

import java.io.{File, FileOutputStream, OutputStreamWriter}
import java.util

import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.table.sources.{BatchTableSource, CsvTableSource}
import org.apache.calcite.sql.SqlKind
import org.apache.calcite.tools.RuleSet
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment}
import org.apache.flink.table.expressions._
import org.apache.flink.table.sinks.TableSink
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

object CommonTestData {

  def getCsvTableSource: CsvTableSource = {
    val csvRecords = Seq(
      "First#Id#Score#Last",
      "Mike#1#12.3#Smith",
      "Bob#2#45.6#Taylor",
      "Sam#3#7.89#Miller",
      "Peter#4#0.12#Smith",
      "% Just a comment",
      "Liz#5#34.5#Williams",
      "Sally#6#6.78#Miller",
      "Alice#7#90.1#Smith",
      "Kelly#8#2.34#Williams"
    )

    val tempFile = File.createTempFile("csv-test", "tmp")
    tempFile.deleteOnExit()
    val tmpWriter = new OutputStreamWriter(new FileOutputStream(tempFile), "UTF-8")
    tmpWriter.write(csvRecords.mkString("$"))
    tmpWriter.close()

    new CsvTableSource(
      tempFile.getAbsolutePath,
      Array("first", "id", "score", "last"),
      Array(
        BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.DOUBLE_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO
      ),
      fieldDelim = "#",
      rowDelim = "$",
      ignoreFirstLine = true,
      ignoreComments = "%"
    )
  }

  def getNestedTableSource: BatchTableSource[Person] = {
    new BatchTableSource[Person] {
      override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Person] = {
        val executionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        executionEnvironment.fromCollection(
          util.Arrays.asList(
            new Person("Mike", "Smith", new Address("5th Ave", "New-York")),
            new Person("Sally", "Miller", new Address("Potsdamer Platz", "Berlin")),
            new Person("Bob", "Taylor", new Address("Pearse Street", "Dublin"))),
          getReturnType
        )
      }

      override def getReturnType: TypeInformation[Person] = {
        TypeExtractor.getForClass(classOf[Person])
      }
    }
  }

  class Person(var firstName: String, var lastName: String, var address: Address) {
    def this() {
      this(null, null, null)
    }
  }

  class Address(var street: String, var city: String) {
    def this() {
      this(null, null)
    }
  }

  def getMockTableEnvironment: TableEnvironment = new MockTableEnvironment

  def getFilterableTableSource(
    fieldNames: Array[String] = Array[String](
      "name", "id", "amount", "price"),
    fieldTypes: Array[TypeInformation[_]] = Array(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO)) = new TestFilterableTableSource(fieldNames, fieldTypes)
}

class MockTableEnvironment extends TableEnvironment(new TableConfig) {

  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = ???

  override protected def checkValidTableName(name: String): Unit = ???

  override def sql(query: String): Table = ???

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = ???

  override protected def getBuiltInNormRuleSet: RuleSet = ???

  override protected def getBuiltInOptRuleSet: RuleSet = ???
}

class TestFilterableTableSource(
    fieldNames: Array[String],
    fieldTypes: Array[TypeInformation[_]])
  extends BatchTableSource[Row]
    with StreamTableSource[Row]
    with FilterableTableSource
    with DefinedFieldNames {

  private var filterPredicate: Option[Expression] = None

  /** Returns the data of the table as a [[DataSet]]. */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.fromElements[Row](
      generateDynamicCollection(33, fieldNames, filterPredicate): _*)
  }

  /** Returns the data of the table as a [[DataStream]]. */
  def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromElements[Row](
      generateDynamicCollection(33, fieldNames, filterPredicate): _*)
  }

  private def generateDynamicCollection(
    num: Int,
    fieldNames: Array[String],
    predicate: Option[Expression]): Seq[Row] = {

    if (predicate.isEmpty) {
      throw new RuntimeException("filter expression was not set")
    }

    val literal = predicate.get.children.last
      .asInstanceOf[Literal]
      .value.asInstanceOf[Int]

    def shouldCreateRow(value: Int): Boolean = {
      value > literal
    }

    def createRow(row: Row, name: String, pos: Int, value: Int): Unit = {
      name match {
        case "name" =>
          row.setField(pos, s"Record_$value")
        case "id" =>
          row.setField(pos, value.toLong)
        case "amount" =>
          row.setField(pos, value.toInt)
        case "price" =>
          row.setField(pos, value.toDouble)
        case _ =>
          throw new IllegalArgumentException(s"unknown fieldName name $name")
      }
    }

    for {cnt <- 0 until num}
      yield {
        val row = new Row(fieldNames.length)
        fieldNames.zipWithIndex.foreach {
          case (name, index) =>
            if (shouldCreateRow(cnt)) {
              createRow(row, name, index, cnt)
            }
        }
        row
      }
  }

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes: _*)

  /** Returns the names of the table fields. */
  override def getFieldNames: Array[String] = fieldNames

  /** Returns the indices of the table fields. */
  override def getFieldIndices: Array[Int] = fieldNames.indices.toArray

  def getPredicate: Option[Expression] = filterPredicate

  /** Return an unsupported predicate expression. */
  override def setPredicate(predicate: Expression): Expression = {
    val (filterPredicate, unused) = extractLeftPredicateWithGraterThan(predicate)
    if (filterPredicate.isDefined) {
      this.filterPredicate = filterPredicate
    }
    unused
  }

  private def extractLeftPredicateWithGraterThan(
    predicate: Expression): (Option[Expression], Expression) = {

    predicate match {
      case e: Expression =>
        e.children.head match {
          case bc: BinaryComparison =>
            bc.sqlOperator.kind match {
              case SqlKind.GREATER_THAN =>
                (Option(e.children.head), e.children.last)
              case _ => (None, e)
            }
          case _ => (None, e)
        }
      case _ => (None, null)
    }
  }
}
