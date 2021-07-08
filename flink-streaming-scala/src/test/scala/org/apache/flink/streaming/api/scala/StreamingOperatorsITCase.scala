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

package org.apache.flink.streaming.api.scala

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}

import org.junit.rules.TemporaryFolder
import org.junit.{After, Before, Rule, Test}

class StreamingOperatorsITCase extends AbstractTestBase {

  var resultPath1: String = _
  var resultPath2: String = _
  var resultPath3: String = _
  var expected1: String = _
  var expected2: String = _
  var expected3: String = _

  val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  def before(): Unit = {
    val temp = tempFolder
    resultPath1 = temp.newFile.toURI.toString
    resultPath2 = temp.newFile.toURI.toString
    resultPath3 = temp.newFile.toURI.toString
    expected1 = ""
    expected2 = ""
    expected3 = ""
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected1, resultPath1)
    TestBaseUtils.compareResultsByLinesInMemory(expected2, resultPath2)
    TestBaseUtils.compareResultsByLinesInMemory(expected3, resultPath3)
  }

  @Test
  def testKeyedAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.getConfig.setMaxParallelism(1)

    val inp = env.fromElements(
      StreamingOperatorsITCase.Outer(1, StreamingOperatorsITCase.Inner(3, "alma"), true),
      StreamingOperatorsITCase.Outer(1, StreamingOperatorsITCase.Inner(6, "alma"), true),
      StreamingOperatorsITCase.Outer(2, StreamingOperatorsITCase.Inner(7, "alma"), true),
      StreamingOperatorsITCase.Outer(2, StreamingOperatorsITCase.Inner(8, "alma"), true)
    )

    inp
      .keyBy("a")
      .sum("i.c")
        .writeAsText(resultPath3, FileSystem.WriteMode.OVERWRITE)

    expected3 =
      "Outer(1,Inner(3,alma),true)\n" +
      "Outer(1,Inner(9,alma),true)\n" +
      "Outer(2,Inner(15,alma),true)\n" +
      "Outer(2,Inner(7,alma),true)"

    env.execute()
  }
}

object StreamingOperatorsITCase {
  case class Inner(c: Short, d: String)
  case class Outer(a: Int, i: StreamingOperatorsITCase.Inner, b: Boolean)
}

