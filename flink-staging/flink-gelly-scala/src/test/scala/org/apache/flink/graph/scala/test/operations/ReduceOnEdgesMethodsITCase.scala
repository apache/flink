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
import org.apache.flink.graph._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.graph.scala.{EdgesFunction, EdgesFunctionWithVertexValue, Graph}
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.util.Collector
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class ReduceOnEdgesMethodsITCase(mode: MultipleProgramsTestBase.TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

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
  def testAllNeighborsWithValueGreaterThanFour {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result = graph.groupReduceOnEdges(new SelectNeighborsValueGreaterThanFour,
      EdgeDirection.ALL)
    result.writeAsCsv(resultPath)
    env.execute


    expectedResult = "5,1\n" + "5,3\n" + "5,4"
  }


  @Test
  @throws(classOf[Exception])
  def testAllNeighbors {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result = graph.groupReduceOnEdges(new SelectNeighbors, EdgeDirection.ALL)
    result.writeAsCsv(resultPath)
    env.execute


    expectedResult = "1,2\n" + "1,3\n" + "1,5\n" + "2,1\n" + "2,3\n" + "3,1\n" + "3,2\n" +
      "3,4\n" + "3,5\n" + "4,3\n" + "4,5\n" + "5,1\n" + "5,3\n" + "5,4"
  }

  @Test
  @throws(classOf[Exception])
  def testLowestWeightOutNeighborNoValue {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val verticesWithLowestOutNeighbor: DataSet[(Long, Long)] = graph.reduceOnEdges(new
        SelectMinWeightNeighborNoValue, EdgeDirection.OUT)
    verticesWithLowestOutNeighbor.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,12\n" + "2,23\n" + "3,34\n" + "4,45\n" + "5,51\n"
  }

  @Test
  @throws(classOf[Exception])
  def testLowestWeightInNeighborNoValue {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val verticesWithLowestOutNeighbor: DataSet[(Long, Long)] = graph.reduceOnEdges(new
        SelectMinWeightNeighborNoValue, EdgeDirection.IN)
    verticesWithLowestOutNeighbor.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,51\n" + "2,12\n" + "3,13\n" + "4,34\n" + "5,35\n"
  }

  @Test
  @throws(classOf[Exception])
  def testMaxWeightAllNeighbors {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val verticesWithMaxEdgeWeight: DataSet[(Long, Long)] = graph.reduceOnEdges(new
        SelectMaxWeightNeighborNoValue, EdgeDirection.ALL)
    verticesWithMaxEdgeWeight.writeAsCsv(resultPath)
    env.execute
    expectedResult = "1,51\n" + "2,23\n" + "3,35\n" + "4,45\n" + "5,51\n"
  }

  final class SelectNeighborsValueGreaterThanFour extends EdgesFunctionWithVertexValue[Long,
    Long, Long, (Long, Long)] {
    @throws(classOf[Exception])
    override def iterateEdges(v: Vertex[Long, Long], edges: Iterable[Edge[Long, Long]], out:
    Collector[(Long, Long)]): Unit = {
      for (edge <- edges) {
        if (v.getValue > 4) {
          if (v.getId == edge.getTarget) {
            out.collect((v.getId, edge.getSource))
          }
          else {
            out.collect((v.getId, edge.getTarget))
          }
        }
      }
    }
  }

  final class SelectNeighbors extends EdgesFunction[Long, Long, (Long, Long)] {
    @throws(classOf[Exception])
    override def iterateEdges(edges: Iterable[(Long, Edge[Long, Long])], out: Collector[
      (Long, Long)]) {
      for (edge <- edges) {
        if (edge._1.equals(edge._2.getTarget)) {
          out.collect(new Tuple2[Long, Long](edge._1, edge._2.getSource))
        }
        else {
          out.collect(new Tuple2[Long, Long](edge._1, edge._2.getTarget))
        }
      }
    }
  }

  final class SelectMinWeightNeighborNoValue extends ReduceEdgesFunction[Long] {
    override def reduceEdges(firstEdgeValue: Long, secondEdgeValue: Long): Long = {
      Math.min(firstEdgeValue, secondEdgeValue)
    }
  }

  final class SelectMaxWeightNeighborNoValue extends ReduceEdgesFunction[Long] {
    override def reduceEdges(firstEdgeValue: Long, secondEdgeValue: Long): Long = {
      Math.max(firstEdgeValue, secondEdgeValue)
    }
  }

}
