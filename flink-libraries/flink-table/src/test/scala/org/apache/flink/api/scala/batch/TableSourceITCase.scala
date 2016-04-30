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

package org.apache.flink.api.scala.batch

import java.io.{File, FileOutputStream, OutputStreamWriter}

import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.{DataSet => JavaSet, ExecutionEnvironment => JavaExecEnv}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.sources.{BatchTableSource, CsvTableSource}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class TableSourceITCase(mode: TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

  @Test
  def testBatchTableSourceTableAPI(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("MyTestTable", new TestBatchTableSource())
    val results = tEnv
      .scan("MyTestTable")
      .where('amount < 4)
      .select('amount * 'id, 'name)
      .collect()

    val expected = Seq(
      "0,Record_0", "0,Record_16", "0,Record_32", "1,Record_1", "17,Record_17",
      "36,Record_18", "4,Record_2", "57,Record_19", "9,Record_3").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testBatchTableSourceSQL(): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    tEnv.registerTableSource("MyTestTable", new TestBatchTableSource())
    val results = tEnv.sql(
      "SELECT amount * id, name FROM MyTestTable WHERE amount < 4").collect()

    val expected = Seq(
      "0,Record_0", "0,Record_16", "0,Record_32", "1,Record_1", "17,Record_17",
      "36,Record_18", "4,Record_2", "57,Record_19", "9,Record_3").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCsvTableSource(): Unit = {

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

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val csvTable = new CsvTableSource(
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

    tEnv.registerTableSource("csvTable", csvTable)
    val results = tEnv.sql(
      "SELECT last, sum(score), max(id) FROM csvTable GROUP BY last").collect()

    val expected = Seq(
      "Smith,102.52,7",
      "Taylor,45.6,2",
      "Miller,14.67,6",
      "Williams,36.84,8").mkString("\n")
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

}

class TestBatchTableSource extends BatchTableSource[Row] {

  val fieldTypes: Array[TypeInformation[_]] = Array(
    BasicTypeInfo.STRING_TYPE_INFO,
    BasicTypeInfo.LONG_TYPE_INFO,
    BasicTypeInfo.INT_TYPE_INFO
  )

  /** Returns the data of the table as a [[org.apache.flink.api.java.DataSet]]. */
  override def getDataSet(execEnv: JavaExecEnv): JavaSet[Row] = {
    execEnv.createInput(new GeneratingInputFormat(33), getReturnType).setParallelism(1)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = Array("name", "id", "amount")

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes)

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = 3
}

class GeneratingInputFormat(val num: Int) extends GenericInputFormat[Row] {

  var cnt = 0L

  override def reachedEnd(): Boolean = cnt >= num

  override def nextRecord(reuse: Row): Row = {
    reuse.setField(0, s"Record_$cnt")
    reuse.setField(1, cnt)
    reuse.setField(2, (cnt % 16).toInt)
    cnt += 1
    reuse
  }
}
