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

package org.apache.flink.table.api.scala.batch

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.{DataSet => JavaSet, ExecutionEnvironment => JavaExecEnv}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.table.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.{BatchTableSource, ProjectableTableSource}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.TestBaseUtils
import org.apache.flink.types.Row
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ProjectableTableSourceITCase(mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  private val tableName = "MyTable"
  private var tableEnv: BatchTableEnvironment = null

  @Before
  def initTableEnv(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getTableEnvironment(env, config)
    tableEnv.registerTableSource(tableName, new TestProjectableTableSource)
  }

  @Test
  def testTableAPI(): Unit = {
    val results = tableEnv
                  .scan(tableName)
                  .where("amount < 4")
                  .select("id, name")
                  .collect()

    val expected = Seq(
      "0,Record_0", "1,Record_1", "2,Record_2", "3,Record_3", "16,Record_16",
      "17,Record_17", "18,Record_18", "19,Record_19", "32,Record_32").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }


  @Test
  def testSQL(): Unit = {
    val results = tableEnv
                  .sql(s"select id, name from $tableName where amount < 4 ")
                  .collect()

    val expected = Seq(
      "0,Record_0", "1,Record_1", "2,Record_2", "3,Record_3", "16,Record_16",
      "17,Record_17", "18,Record_18", "19,Record_19", "32,Record_32").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }
}

class TestProjectableTableSource(
  fieldTypes: Array[TypeInformation[_]],
  fieldNames: Array[String])
  extends BatchTableSource[Row] with ProjectableTableSource[Row] {

  def this() = this(
    fieldTypes = Array(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO),
    fieldNames = Array[String]("name", "id", "amount", "price")
  )

  /** Returns the data of the table as a [[org.apache.flink.api.java.DataSet]]. */
  override def getDataSet(execEnv: JavaExecEnv): JavaSet[Row] = {
    execEnv.fromCollection(generateDynamicCollection(33, fieldNames).asJava, getReturnType)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = fieldNames

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes: _*)

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = fieldNames.length

  override def projectFields(fields: Array[Int]): TestProjectableTableSource = {
    val projectedFieldTypes = new Array[TypeInformation[_]](fields.length)
    val projectedFieldNames = new Array[String](fields.length)

    fields.zipWithIndex.foreach(f => {
      projectedFieldTypes(f._2) = fieldTypes(f._1)
      projectedFieldNames(f._2) = fieldNames(f._1)
    })
    new TestProjectableTableSource(projectedFieldTypes, projectedFieldNames)
  }

  private def generateDynamicCollection(num: Int, fieldNames: Array[String]): Seq[Row] = {
    for {cnt <- 0 until num}
      yield {
        val row = new Row(fieldNames.length)
        fieldNames.zipWithIndex.foreach(
          f =>
            f._1 match {
              case "name" =>
                row.setField(f._2, "Record_" + cnt)
              case "id" =>
                row.setField(f._2, cnt.toLong)
              case "amount" =>
                row.setField(f._2, cnt.toInt % 16)
              case "price" =>
                row.setField(f._2, cnt.toDouble / 3)
              case _ =>
                throw new IllegalArgumentException(s"unknown field name $f._1")
            }
        )
        row
      }
  }
}
