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
import org.apache.flink.graph.scala.utils.EdgeToTuple3Map
import org.apache.flink.graph.{Edge, EdgeJoinFunction}
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import _root_.scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class JoinWithEdgesITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null

  @Test
  @throws(classOf[Exception])
  def testWithEdgesInputDataset {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdges(graph.getEdges.map(new
        EdgeToTuple3Map[Long, Long]), new AddValuesMapper)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,26\n" + "2,3,46\n" + "3,4,68\n" + "3,5,70\n" + "4,5," +
      "90\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testWithEdgesInputDatasetSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdges(graph.getEdges.map(new
        EdgeToTuple3Map[Long, Long]), (originalValue: Long, tupleValue: Long) =>
      originalValue + tupleValue)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,26\n" + "2,3,46\n" + "3,4,68\n" + "3,5,70\n" + "4,5," +
      "90\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testWithEdgesOnSource {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdgesOnSource[Long](graph.getEdges
      .map(new ProjectSourceAndValueMapper), (originalValue: Long, tupleValue: Long) =>
      originalValue + tupleValue)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,25\n" + "2,3,46\n" + "3,4,68\n" + "3,5,69\n" + "4,5," +
      "90\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testWithEdgesOnSourceSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdgesOnSource[Long](graph.getEdges
      .map(new ProjectSourceAndValueMapper), (originalValue: Long, tupleValue: Long) =>
      originalValue + tupleValue)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,25\n" + "2,3,46\n" + "3,4,68\n" + "3,5,69\n" + "4,5," +
      "90\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testWithEdgesOnTarget {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdgesOnTarget[Long](graph.getEdges
      .map(new ProjectTargetAndValueMapper), (originalValue: Long, tupleValue: Long) =>
      originalValue + tupleValue)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,26\n" + "2,3,36\n" + "3,4,68\n" + "3,5,70\n" + "4,5," +
      "80\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testWithEdgesOnTargetSugar {
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils
      .getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
    val result: Graph[Long, Long, Long] = graph.joinWithEdgesOnTarget[Long](graph.getEdges
      .map(new ProjectTargetAndValueMapper), (originalValue: Long, tupleValue: Long) =>
      originalValue + tupleValue)
    val res = result.getEdges.collect().toList
    expectedResult = "1,2,24\n" + "1,3,26\n" + "2,3,36\n" + "3,4,68\n" + "3,5,70\n" + "4,5," +
      "80\n" + "5,1,102\n"
    TestBaseUtils.compareResultAsTuples(res.asJava, expectedResult)
  }


  final class AddValuesMapper extends EdgeJoinFunction[Long, Long] {
    @throws(classOf[Exception])
    def edgeJoin(edgeValue: Long, inputValue: Long): Long = {
      edgeValue + inputValue
    }
  }

  final class ProjectSourceAndValueMapper extends MapFunction[Edge[Long, Long], (Long, Long)] {
    @throws(classOf[Exception])
    def map(edge: Edge[Long, Long]): (Long, Long) = {
      (edge.getSource, edge.getValue)
    }
  }

  final class ProjectTargetAndValueMapper extends MapFunction[Edge[Long, Long], (Long, Long)] {
    @throws(classOf[Exception])
    def map(edge: Edge[Long, Long]): (Long, Long) = {
      (edge.getTarget, edge.getValue)
    }
  }

}
