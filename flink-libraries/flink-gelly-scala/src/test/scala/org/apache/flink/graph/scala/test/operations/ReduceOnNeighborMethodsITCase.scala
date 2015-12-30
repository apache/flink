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
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.graph.scala.{NeighborsFunctionWithVertexValue, _}
import org.apache.flink.graph.{Edge, EdgeDirection, ReduceNeighborsFunction, Vertex}
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.util.Collector
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import _root_.scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class ReduceOnNeighborMethodsITCase(mode: MultipleProgramsTestBase.TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null

  @Test
  @throws(classOf[Exception])
  def testSumOfAllNeighborsNoValue {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.reduceOnNeighbors(new SumNeighbors, EdgeDirection.ALL)
    .collect().toList
    expectedResult = "(1,10)\n" + "(2,4)\n" + "(3,12)\n" + "(4,8)\n" + "(5,8)\n"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testSumOfOutNeighborsNoValue {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val res = graph.reduceOnNeighbors(new SumNeighbors, EdgeDirection.OUT).collect().toList
    expectedResult = "(1,5)\n" + "(2,3)\n" + "(3,9)\n" + "(4,5)\n" + "(5,1)\n"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testSumOfAllNeighbors {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result = graph.groupReduceOnNeighbors(new SumAllNeighbors, EdgeDirection.ALL)
    val res = result.collect().toList
    expectedResult = "(1,11)\n" + "(2,6)\n" + "(3,15)\n" + "(4,12)\n" + "(5,13)\n"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testSumOfInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo = {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result = graph.groupReduceOnNeighbors(new
        SumInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo, EdgeDirection.IN)
    val res = result.collect().toList
    expectedResult = "(3,59)\n" + "(3,118)\n" + "(4,204)\n" + "(4,102)\n" + "(5,570)\n" + "(5,285)"
    TestBaseUtils.compareResultAsText(res.asJava, expectedResult)
  }

  final class SumNeighbors extends ReduceNeighborsFunction[Long] {
    override def reduceNeighbors(firstNeighbor: Long, secondNeighbor: Long): Long = {
      firstNeighbor + secondNeighbor
    }
  }

  final class SumAllNeighbors extends NeighborsFunctionWithVertexValue[Long, Long, Long, (Long,
    Long)] {
    @throws(classOf[Exception])
    def iterateNeighbors(vertex: Vertex[Long, Long], neighbors: Iterable[(Edge[Long, Long],
      Vertex[Long, Long])], out: Collector[(Long, Long)]) {
      var sum: Long = 0
      for (neighbor <- neighbors) {
        sum += neighbor._2.getValue
      }
      out.collect((vertex.getId, sum + vertex.getValue))
    }
  }

  final class SumInNeighborsNoValueMultipliedByTwoIdGreaterThanTwo extends
  NeighborsFunction[Long, Long, Long, (Long, Long)] {
    @throws(classOf[Exception])
    def iterateNeighbors(neighbors: Iterable[(Long, Edge[Long, Long], Vertex[Long, Long])],
                         out: Collector[(Long, Long)]) {
      var sum: Long = 0
      var next: (Long, Edge[Long, Long], Vertex[Long, Long]) = null
      val neighborsIterator: Iterator[(Long, Edge[Long, Long], Vertex[Long, Long])] =
        neighbors.iterator
      while (neighborsIterator.hasNext) {
        next = neighborsIterator.next
        sum += next._3.getValue * next._2.getValue
      }
      if (next._1 > 2) {
        out.collect(new Tuple2[Long, Long](next._1, sum))
        out.collect(new Tuple2[Long, Long](next._1, sum * 2))
      }
    }
  }

}
