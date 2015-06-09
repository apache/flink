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

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class GraphMutationsITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
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
  def testAddVertex {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)

    val newgraph = graph.addVertex(new Vertex[Long, Long](6L, 6L))
    newgraph.getVerticesAsTuple2.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n"
  }

  @Test
  @throws(classOf[Exception])
  def testAddVertexExisting {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addVertex(new Vertex[Long, Long](1L, 1L))
    newgraph.getVerticesAsTuple2.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n"
  }

  @Test
  @throws(classOf[Exception])
  def testAddVertexNoEdges {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addVertex(new Vertex[Long, Long](6L, 6L))
    newgraph.getVerticesAsTuple2.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n"
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveVertex {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertex(new Vertex[Long, Long](5L, 5L))
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n"
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveInvalidVertex {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertex(new Vertex[Long, Long](6L, 6L))
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testAddEdge {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdge(new Vertex[Long, Long](6L, 6L), new Vertex[Long, Long](1L,
      1L), 61L)
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n" + "6,1,61\n"
  }

  @Test
  @throws(classOf[Exception])
  def testAddExistingEdge {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdge(new Vertex[Long, Long](1L, 1L), new Vertex[Long, Long](2L,
      2L), 12L)
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5," +
      "35\n" + "4,5,45\n" + "5,1,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveEdge {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeEdge(new Edge[Long, Long](5L, 1L, 51L))
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveInvalidEdge {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeEdge(new Edge[Long, Long](6L, 1L, 61L))
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n"
  }
}
