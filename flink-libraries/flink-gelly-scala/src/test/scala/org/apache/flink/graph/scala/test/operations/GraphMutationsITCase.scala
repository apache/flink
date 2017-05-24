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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import _root_.scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class GraphMutationsITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null

  @Test
  @throws(classOf[Exception])
  def testAddVertex() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)

    val newgraph = graph.addVertex(new Vertex[Long, Long](6L, 6L))
    val res = newgraph.getVertices.collect().toList
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddVertexExisting() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addVertex(new Vertex[Long, Long](1L, 1L))
    val res = newgraph.getVertices.collect().toList
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddVertexNoEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addVertex(new Vertex[Long, Long](6L, 6L))
    val res = newgraph.getVertices.collect().toList
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)

    val newgraph = graph.addVertices(List[Vertex[Long, Long]](new Vertex[Long, Long](5L, 0L),
      new Vertex[Long, Long](6L, 6L), new Vertex[Long, Long](7L, 7L)))
    val res = newgraph.getVertices.collect().toList
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n" + "7,7\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddVerticesExisting() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)

    val newgraph = graph.addVertices(List[Vertex[Long, Long]](new Vertex[Long, Long](5L, 5L),
        new Vertex[Long, Long](6L, 6L)))
    val res = newgraph.getVertices.collect().toList
    expectedResult = "1,1\n" + "2,2\n" + "3,3\n" + "4,4\n" + "5,5\n" + "6,6\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveVertex() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertex(new Vertex[Long, Long](5L, 5L))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveInvalidVertex() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertex(new Vertex[Long, Long](6L, 6L))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertices(List[Vertex[Long, Long]](new Vertex[Long, Long](1L, 1L),
        new Vertex[Long, Long](2L, 2L)))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveValidAndInvalidVertex() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeVertices(List[Vertex[Long, Long]](new Vertex[Long, Long](1L, 1L),
        new Vertex[Long, Long](6L, 6L)))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddEdge() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdge(new Vertex[Long, Long](6L, 6L), new Vertex[Long, Long](1L,
      1L), 61L)
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n" + "6,1,61\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdges(List[Edge[Long, Long]](new Edge(2L, 4L, 24L),
       new Edge(4L, 1L, 41L), new Edge(4L, 3L, 43L)))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "2,4,24\n" + "3,4,34\n" + "3,5," +
    "35\n" + "4,1,41\n" + "4,3,43\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddEdgesInvalidVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdges(List[Edge[Long, Long]](new Edge(6L, 1L, 61L),
       new Edge(7L, 8L, 78L)))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5," +
    "35\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAddExistingEdge() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.addEdge(new Vertex[Long, Long](1L, 1L), new Vertex[Long, Long](2L,
      2L), 12L)
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5," +
      "35\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveEdge() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    var graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph = graph.addEdge(new Vertex[Long, Long](1L, 1L), new Vertex[Long, Long](2L, 2L), 12L)
    graph = graph.removeEdge(new Edge[Long, Long](5L, 1L, 51L))
    val res = graph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" +
      "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveInvalidEdge() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeEdge(new Edge[Long, Long](6L, 1L, 61L))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    var graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    graph = graph.addEdge(new Vertex[Long, Long](3L, 3L), new Vertex[Long, Long](4L, 4L), 34L)
    graph = graph.removeEdges(List[Edge[Long, Long]](new Edge(1L, 2L, 12L), new Edge(4L, 5L, 45L)))
    val res = graph.getEdges.collect().toList
    expectedResult = "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,4,34\n" + "3,5,35\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testRemoveSameEdgeTwiceEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val newgraph = graph.removeEdges(List[Edge[Long, Long]](new Edge(1L, 2L, 12L),
       new Edge(1L, 2L, 12L)))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }
}
