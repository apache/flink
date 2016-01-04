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
package org.apache.flink.api.scala.table.test

import java.lang.reflect.{Type, ParameterizedType}

import com.google.common.base.{Charsets, Preconditions}
import com.google.common.io.Files
import org.apache.flink.api.java.table.TableEnvironment.CsvOptions
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.java.table.TableEnvironment
import org.apache.flink.api.java.tuple.Tuple3
import org.apache.flink.api.table.{Row, Table}
import org.apache.flink.core.fs.{FileSystem, Path}
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

/*TODO: This test is using java api, should change to scala api in the future (or move
 * [[TableEnvironment]] to shared api?)
 * */
@RunWith(classOf[Parameterized])
class FromCsvITCase (mode: TestExecutionMode) extends MultipleProgramsTestBase(mode){
  private var resultPath: String = null
  private var expected: String = ""
  private val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder = _tempFolder

  @Before
  def before(): Unit = {
    resultPath = tempFolder.newFile().toURI.toString
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  def createInputData(data: String): String = {
    val dataFile = tempFolder.newFile("data")
    Files.write(data, dataFile, Charsets.UTF_8)
    dataFile.toURI.toString
  }

  @Test
  def testTuple(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: TableEnvironment = new TableEnvironment
    val inputData: String = "ABC,2.20,3\nDEF,5.1,5\nDEF,3.30,1\nGHI,3.30,10"
    val dataPath: String = createInputData(inputData)
    val path: Path = new Path(Preconditions.checkNotNull(dataPath))
    val table: Table = tableEnv.fromCsvFile(path, env,
      Array[Class[_]](classOf[String], classOf[java.lang.Float],
      classOf[java.lang.Integer]))

    type JavaDataSet[T] = org.apache.flink.api.java.DataSet[T]
    val ds: JavaDataSet[Row] = tableEnv.toDataSet(table, classOf[Row])
    ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)

    env.execute

    expected = "ABC,2.2,3\nDEF,5.1,5\nDEF,3.3,1\nGHI,3.3,10"
  }

  @Test
  def testTupleWithOptions(): Unit = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv: TableEnvironment = new TableEnvironment
    val inputData: String = "ABC*2.20*3\nDEF*5.1*5\nDEF*3.30*1\n" +
      "#this is comment\nbadline\nGHI*3.30*10"
    val dataPath: String = createInputData(inputData)
    val path: Path = new Path(Preconditions.checkNotNull(dataPath))
    val options: CsvOptions = new CsvOptions
    options.commentPrefix = "#"
    options.fieldDelimiter = "*"
    options.includedMask = Array(true, true, false)
    options.ignoreInvalidLines = true
    val table: Table = tableEnv.fromCsvFile(path, env,
      Array[Class[_]](classOf[String], classOf[java.lang.Float], classOf[java.lang.Integer]),
      fields = "a,b,c",
      options
    )

    type JavaDataSet[T] = org.apache.flink.api.java.DataSet[T]
    val ds: JavaDataSet[Row] = tableEnv.toDataSet(table, classOf[Row])
    ds.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)

    env.execute

    expected = "ABC,2.2,0\nDEF,5.1,0\nDEF,3.3,0\nGHI,3.3,0"
  }

}
