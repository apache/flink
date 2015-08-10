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

package org.apache.flink.graph.scala.test.operations

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.graph.scala.utils.VertexToTuple2Map
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class JoinWithVerticesITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode) {

  private var resultPath: String = null
  private var expectedResult: String = null

  var tempFolder: TemporaryFolder = new TemporaryFolder()

  @Rule
  def getFolder(): TemporaryFolder = {
    tempFolder;
  }

  @Before
  @throws(classOf[Exception])
  def before {
    resultPath = tempFolder.newFile.toURI.toString
  }

  @After
  @throws(classOf[Exception])
  def after {
    TestBaseUtils.compareResultsByLinesInMemory(expectedResult, resultPath)
  }

  @Test
  @throws(classOf[Exception])
  def testJoinWithVertexSet {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithVertices(graph.getVertices.map(new
        VertexToTuple2Map[Long, Long]), new AddValuesMapper)
    result.getVerticesAsTuple2().writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2\n" + "2,4\n" + "3,6\n" + "4,8\n" + "5,10\n"
  }

  @Test
  @throws(classOf[Exception])
  def testJoinWithVertexSetSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val tupleSet = graph.getVertices.map(new VertexToTuple2Map[Long, Long])
    val result: Graph[Long, Long, Long] = graph.joinWithVertices[Long](tupleSet,
      (originalvalue: Long, tuplevalue: Long) => originalvalue + tuplevalue)
    result.getVerticesAsTuple2().writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2\n" + "2,4\n" + "3,6\n" + "4,8\n" + "5,10\n"
  }


  final class AddValuesMapper extends MapFunction[(Long, Long), Long] {
    @throws(classOf[Exception])
    def map(tuple: (Long, Long)): Long = {
      tuple._1 + tuple._2
    }
  }

}
