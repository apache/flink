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

import org.apache.flink.api.common.functions.FilterFunction
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
class GraphOperationsITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
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
  def testUndirected {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.getUndirected().getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "2,1,12\n" + "1,3,13\n" + "3,1,13\n" + "2,3,23\n" + "3,2," +
      "23\n" + "3,4,34\n" + "4,3,34\n" + "3,5,35\n" + "5,3,35\n" + "4,5,45\n" + "5,4,45\n" +
      "5,1,51\n" + "1,5,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testReverse {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.reverse().getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "2,1,12\n" + "3,1,13\n" + "3,2,23\n" + "4,3,34\n" + "5,3,35\n" + "5,4," +
      "45\n" + "1,5,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testSubGraph {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.subgraph(new FilterFunction[Vertex[Long, Long]] {
      @throws(classOf[Exception])
      def filter(vertex: Vertex[Long, Long]): Boolean = {
        return (vertex.getValue > 2)
      }
    }, new FilterFunction[Edge[Long, Long]] {

      @throws(classOf[Exception])
      override def filter(edge: Edge[Long, Long]): Boolean = {
        return (edge.getValue > 34)
      }
    }).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,5,35\n" + "4,5,45\n"
  }

  @Test
  @throws(classOf[Exception])
  def testSubGraphSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.subgraph(
      vertex => vertex.getValue > 2,
      edge => edge.getValue > 34
    ).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,5,35\n" + "4,5,45\n"
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnVertices {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.filterOnVertices(new FilterFunction[Vertex[Long, Long]] {
      @throws(classOf[Exception])
      def filter(vertex: Vertex[Long, Long]): Boolean = {
        vertex.getValue > 2
      }
    }).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnVerticesSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.filterOnVertices(
      vertex => vertex.getValue > 2
    ).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnEdges {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.filterOnEdges(new FilterFunction[Edge[Long, Long]] {
      @throws(classOf[Exception])
      def filter(edge: Edge[Long, Long]): Boolean = {
        edge.getValue > 34
      }
    }).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnEdgesSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.filterOnEdges(
      edge => edge.getValue > 34
    ).getEdgesAsTuple3().writeAsCsv(resultPath)
    env.execute
    expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testNumberOfVertices {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    env.fromElements(graph.numberOfVertices).writeAsText(resultPath)
    env.execute
    expectedResult = "5"
  }

  @Test
  @throws(classOf[Exception])
  def testNumberOfEdges {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    env.fromElements(graph.numberOfEdges).writeAsText(resultPath)
    env.execute
    expectedResult = "7"
  }

  @Test
  @throws(classOf[Exception])
  def testVertexIds {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.getVertexIds.writeAsText(resultPath)
    env.execute
    expectedResult = "1\n2\n3\n4\n5\n"
  }

  @Test
  @throws(classOf[Exception])
  def testEdgesIds {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph.getEdgeIds.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2\n" + "1,3\n" + "2,3\n" + "3,4\n" + "3,5\n" + "4,5\n" + "5,1\n"
  }

  @Test
  @throws(classOf[Exception])
  def testUnion {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val vertices: List[Vertex[Long, Long]] = List[Vertex[Long, Long]](
      new Vertex[Long, Long](6L, 6L)
    )
    val edges: List[Edge[Long, Long]] = List[Edge[Long, Long]](
      new Edge[Long, Long](6L, 1L, 61L)
    )

    val newgraph = graph.union(Graph.fromCollection(vertices, edges, env))
    newgraph.getEdgesAsTuple3.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n" + "6,1,61\n"
  }
}
