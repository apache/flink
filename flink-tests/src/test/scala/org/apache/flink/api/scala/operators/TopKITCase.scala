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
package org.apache.flink.api.scala.operators

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class TopKITCase(mode: TestExecutionMode) extends MultipleProgramsTestBase(mode) {
  private var resultPath: String = null
  private var expected: String = null
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

  @Test
  def topKOrderVerification1: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).map(x => x._3)
    val topK = ds.topK(3, true);
    topK.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Luke Skywalker\n" + "I am fine.\n" + "Hi\n"
  }

  @Test
  def topKOrderVerification2: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).map(x => x._3)
    val topK = ds.topK(3, false);
    topK.writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Comment#1\n" + "Comment#10\n" + "Comment#11\n"
  }

  @Test
  def topKExceedSourceSize: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env).map(x => x._3)
    val topK = ds.topK(22);
    assertEquals(21, topK.collect().size)
    topK.first(3).writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Luke Skywalker\n" + "I am fine.\n" + "Hi\n"
  }

  @Test
  def topKComplicatedType: Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = CollectionDataSets.get3TupleDataSet(env)
    val topK = ds.topK(3).map(x => x._3);
    assertEquals(3, topK.collect().size)
    topK.first(3).writeAsText(resultPath, WriteMode.OVERWRITE)
    env.execute()
    expected = "Comment#13\n" + "Comment#14\n" + "Comment#15\n"
  }
}
