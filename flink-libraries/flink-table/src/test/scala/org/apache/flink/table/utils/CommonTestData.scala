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

import scala.collection.JavaConverters._

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

  def getFilterableTableSource = new TestFilterableTableSource
}

class MockTableEnvironment extends TableEnvironment(new TableConfig) {

  override private[flink] def writeToSink[T](table: Table, sink: TableSink[T]): Unit = ???

  override protected def checkValidTableName(name: String): Unit = ???

  override def sql(query: String): Table = ???

  override def registerTableSource(name: String, tableSource: TableSource[_]): Unit = ???

  override protected def getBuiltInNormRuleSet: RuleSet = ???

  override protected def getBuiltInOptRuleSet: RuleSet = ???
}

class TestFilterableTableSource
    extends BatchTableSource[Row]
    with StreamTableSource[Row]
    with FilterableTableSource
    with DefinedFieldNames {

  import org.apache.flink.table.api.Types._

  val fieldNames = Array("name", "id", "amount", "price")
  val fieldTypes = Array[TypeInformation[_]](STRING, LONG, INT, DOUBLE)

  private var filterLiteral: Literal = _
  private var filterPredicates: Array[Expression] = Array.empty

  /** Returns the data of the table as a [[DataSet]]. */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.fromCollection[Row](generateDynamicCollection(33).asJava, getReturnType)
  }

  /** Returns the data of the table as a [[DataStream]]. */
  def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    execEnv.fromCollection[Row](generateDynamicCollection(33).asJava, getReturnType)
  }

  private def generateDynamicCollection(num: Int): Seq[Row] = {

    if (filterLiteral == null) {
      throw new RuntimeException("filter expression was not set")
    }

    val filterValue = filterLiteral.value.asInstanceOf[Number].intValue()

    def shouldCreateRow(value: Int): Boolean = {
      value > filterValue
    }

    for {
      cnt <- 0 until num
      if shouldCreateRow(cnt)
    } yield {
        val row = new Row(fieldNames.length)
        fieldNames.zipWithIndex.foreach { case (name, index) =>
          name match {
            case "name" =>
              row.setField(index, s"Record_$cnt")
            case "id" =>
              row.setField(index, cnt.toLong)
            case "amount" =>
              row.setField(index, cnt.toInt)
            case "price" =>
              row.setField(index, cnt.toDouble)
          }
        }
      row
      }
  }

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes, fieldNames)

  /** Returns the names of the table fields. */
  override def getFieldNames: Array[String] = fieldNames

  /** Returns the indices of the table fields. */
  override def getFieldIndices: Array[Int] = fieldNames.indices.toArray

  override def getPredicate: Array[Expression] = filterPredicates

  /** Return an unsupported predicates expression. */
  override def setPredicate(predicates: Array[Expression]): Array[Expression] = {
    predicates(0) match {
      case gt: GreaterThan =>
        gt.left match {
          case f: ResolvedFieldReference =>
            gt.right match {
              case l: Literal =>
                if (f.name.equals("amount")) {
                  filterLiteral = l
                  filterPredicates = Array(predicates(0))
                  Array(predicates(1))
                } else predicates
              case _ => predicates
            }
          case _ => predicates
        }
      case _ => predicates
    }
  }
}
