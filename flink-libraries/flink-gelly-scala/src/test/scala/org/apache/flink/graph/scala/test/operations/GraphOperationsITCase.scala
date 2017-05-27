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
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import _root_.scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class GraphOperationsITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null
    
  @Test
  @throws(classOf[Exception])
  def testUndirected() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.getUndirected().getEdges.collect().toList

    expectedResult = "1,2,12\n" + "2,1,12\n" + "1,3,13\n" + "3,1,13\n" + "2,3,23\n" + "3,2," +
      "23\n" + "3,4,34\n" + "4,3,34\n" + "3,5,35\n" + "5,3,35\n" + "4,5,45\n" + "5,4,45\n" +
      "5,1,51\n" + "1,5,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testReverse() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.reverse().getEdges.collect().toList

    expectedResult = "2,1,12\n" + "3,1,13\n" + "3,2,23\n" + "4,3,34\n" + "5,3,35\n" + "5,4," +
      "45\n" + "1,5,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testSubGraph() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.subgraph(new FilterFunction[Vertex[Long, Long]] {
      @throws(classOf[Exception])
      def filter(vertex: Vertex[Long, Long]): Boolean = {
        vertex.getValue > 2
      }
    }, new FilterFunction[Edge[Long, Long]] {

      @throws(classOf[Exception])
      override def filter(edge: Edge[Long, Long]): Boolean = {
        edge.getValue > 34
      }
    }).getEdges.collect().toList

    expectedResult = "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testSubGraphSugar() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.subgraph(
      vertex => vertex.getValue > 2,
      edge => edge.getValue > 34
    ).getEdges.collect().toList

    expectedResult = "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.filterOnVertices(new FilterFunction[Vertex[Long, Long]] {
      @throws(classOf[Exception])
      def filter(vertex: Vertex[Long, Long]): Boolean = {
        vertex.getValue > 2
      }
    }).getEdges.collect().toList

    expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnVerticesSugar() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.filterOnVertices(
      vertex => vertex.getValue > 2
    ).getEdges.collect().toList

    expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.filterOnEdges(new FilterFunction[Edge[Long, Long]] {
      @throws(classOf[Exception])
      def filter(edge: Edge[Long, Long]): Boolean = {
        edge.getValue > 34
      }
    }).getEdges.collect().toList

    expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testFilterOnEdgesSugar() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.filterOnEdges(
      edge => edge.getValue > 34
    ).getEdges.collect().toList

    expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testNumberOfVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = env.fromElements(graph.numberOfVertices()).collect().toList
    expectedResult = "5"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testNumberOfEdges() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = env.fromElements(graph.numberOfEdges()).collect().toList
    expectedResult = "7"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testVertexIds() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.getVertexIds().collect().toList
    expectedResult = "1\n2\n3\n4\n5\n"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testEdgesIds() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.getEdgeIds().collect().toList
    expectedResult = "(1,2)\n" + "(1,3)\n" + "(2,3)\n" + "(3,4)\n" + "(3,5)\n" + "(4,5)\n" +
      "(5,1)\n"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testUnion() {
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
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n" + "6,1,61\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testDifference() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val vertices: List[Vertex[Long, Long]] = List[Vertex[Long, Long]](
      new Vertex[Long, Long](1L, 1L), new Vertex[Long, Long](3L, 3L),
      new Vertex[Long, Long](6L, 6L) 
    )
    val edges: List[Edge[Long, Long]] = List[Edge[Long, Long]](
      new Edge[Long, Long](1L, 3L, 13L), new Edge[Long, Long](1L, 6L, 16L),
      new Edge[Long, Long](6L, 3L, 63L)
    )

    val newgraph = graph.difference(Graph.fromCollection(vertices, edges, env))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "4,5,45\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testDifferenceNoCommonVertices() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val vertices: List[Vertex[Long, Long]] = List[Vertex[Long, Long]](
      new Vertex[Long, Long](6L, 6L) 
    )
    val edges: List[Edge[Long, Long]] = List[Edge[Long, Long]](
      new Edge[Long, Long](6L, 6L, 66L)
    )

    val newgraph = graph.difference(Graph.fromCollection(vertices, edges, env))
    val res = newgraph.getEdges.collect().toList
    expectedResult = "1,2,12\n" + "1,3,13\n" + "2,3,23\n" + "3,4,34\n" + "3,5,35\n" + "4,5," +
      "45\n" + "5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testTriplets() {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.getTriplets().collect().toList
    expectedResult = "1,2,1,2,12\n" + "1,3,1,3,13\n" + "2,3,2,3,23\n" + "3,4,3,4,34\n" +
      "3,5,3,5,35\n" + "4,5,4,5,45\n" + "5,1,5,1,51\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }
}
