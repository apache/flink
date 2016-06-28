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

import java.io.{File, FileOutputStream, IOException, OutputStreamWriter}

import com.google.common.base.Charsets
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.{FileInputSplit, Path}
import org.apache.flink.graph.scala._
import org.apache.flink.test.util.{MultipleProgramsTestBase, TestBaseUtils}
import org.apache.flink.types.NullValue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import _root_.scala.collection.JavaConverters._

@RunWith(classOf[Parameterized])
class GraphCreationWithAdjacencyListITCase(mode: MultipleProgramsTestBase.TestExecutionMode)
  extends MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null

  @Test
  @throws(classOf[Exception])
  def testAdjFileWithValues() {
    /*
     * Test when both vertices and edges have values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val adjFileContent = "0-Node0\t1-0.1,4-0.2,5-0.3\n" + "1-Node1 2-0.3\n" +
      "2-Node2 3-0.3\n" + "3-Node3 \n" + "4-Node4\n" + "5-Node5 6-0.7\n" + "6-Node6 3-0.2\n" +
      "7-Node7 4-0.1\n" + "8-Node8 0-0.2\n"

    val adjFileSplit = createTempFile(adjFileContent)

    val graph = Graph.fromAdjacencyListFile[Long, String, Double](
      filePath = adjFileSplit.getPath.toString,
      env = env)

    val result = graph.getTriplets().collect()

    expectedResult = "0,1,Node0,Node1,0.1\n" + "0,4,Node0,Node4,0.2\n" +
      "0,5,Node0,Node5,0.3\n" + "1,2,Node1,Node2,0.3\n" + "2,3,Node2,Node3,0.3\n" +
      "5,6,Node5,Node6,0.7\n" + "6,3,Node6,Node3,0.2\n" + "7,4,Node7,Node4,0.1\n" +
      "8,0,Node8,Node0,0.2\n"

    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testAdjFileNoEdgeValues() {
    /*
     * Test when edges have no values and vertices have values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val adjFileContent = "0-Node0 1,4,5,8\n" + "1-Node1 0,2\n" + "2-Node2 3\n" + "3-Node3\n" +
      "4-Node4\n" + "5-Node5 6\n" + "6-Node6 3\n" + "7-Node7 4\n" + "8-Node8 0\n"

    val adjFileSplit = createTempFile(adjFileContent)

    val graph = Graph.fromAdjacencyListFile[Long, String, NullValue](
      filePath = adjFileSplit.getPath.toString,
      env = env)

    val result = graph.getTriplets().collect()
    expectedResult = "0,1,Node0,Node1,(null)\n" + "0,4,Node0,Node4,(null)\n" + "0,5,Node0,Node5," +
      "(null)\n" + "0,8,Node0,Node8,(null)\n" + "1,0,Node1,Node0,(null)\n" + "1,2,Node1,Node2," +
      "(null)\n" + "2,3,Node2,Node3,(null)\n" + "5,6,Node5,Node6,(null)\n" + "6,3,Node6,Node3," +
      "(null)\n" + "7,4,Node7,Node4,(null)\n" + "8,0,Node8,Node0,(null)\n"

    //    System.out.println("\nres: " + result + "\n")
    //    System.out.println("ex-res: " + expectedResult + "\n")
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
    assert(graph.getVertexIds().count() == 9)
    assert(graph.getEdgeIds().count() == 11)
  }

  @Test
  @throws(classOf[Exception])
  def testAdjFileNoVertexValues() {
    /*
     * Test when vertices have no values and edges have values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val adjFileContent = "0 1-0.1,4-0.2,5-0.3,8-0.1\n" + "1 0-0.8,2-0.3\n" + "2 3-0.3\n" + "3\n" +
      "4\n" + "5 6-0.7\n" + "6 3-0.2\n" + "7 4-0.1\n" + "8 0-0.2\n"

    val adjFileSplit = createTempFile(adjFileContent)
    val graph = Graph.fromAdjacencyListFile[Long, NullValue, Double](
      filePath = adjFileSplit.getPath.toString,
      env = env)

    val result = graph.getTriplets().collect()

    expectedResult = "0,1,(null),(null),0.1\n" + "0,4,(null),(null),0.2\n" + "0,5,(null),(null),0" +
      ".3\n" + "0,8,(null),(null),0.1\n" + "1,0,(null),(null),0.8\n" + "1,2,(null),(null),0.3\n" +
      "2,3,(null),(null),0.3\n" + "5,6,(null),(null),0.7\n" + "6,3,(null),(null),0.2\n" + "7," +
      "4,(null),(null),0.1\n" + "8,0,(null),(null),0.2\n"

    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
    assert(graph.getVertexIds().count() == 9)
  }

  @Test
  @throws(classOf[Exception])
  def testAdjFileNoValues() {
    /*
     * Test with Adjacency List file: neither vertex nor edge values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val adjFileContent = "0 1,4,5,8\n" + "1 0,2\n" + "2 3\n" + "3\n" + "4\n" + "5 6\n" + "6 3," +
      "7\n" + "7 4\n" + "8 0\n"

    val adjFileSplit = createTempFile(adjFileContent)
    val graph = Graph.fromAdjacencyListFile[Long, NullValue, NullValue](
      filePath = adjFileSplit.getPath.toString,
      env = env)

    val result = graph.getTriplets().collect()
    expectedResult = "0,1,(null),(null),(null)\n" + "0,4,(null),(null),(null)\n" + "0,5,(null)," +
      "(null),(null)\n" + "0,8,(null),(null),(null)\n" + "1,0,(null),(null),(null)\n" + "1,2," +
      "(null),(null),(null)\n" + "2,3,(null),(null),(null)\n" + "5,6,(null),(null),(null)\n" +
      "6,3,(null),(null),(null)\n" + "6,7,(null),(null),(null)\n" + "7,4,(null),(null),(null)\n" +
      "8,0,(null),(null),(null)\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
    assert(graph.getVertexIds().count() == 9)
  }


  @Test
  @throws(classOf[Exception])
  def testwriteAsAdjacencyList() {
    /*
     * Test writeAsAdjacencyList method and also user specified delimiters.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val adjFileContent = "0:0.0#1:0.1$4:0.2$5:0.3\n" + "1:1.0#0:0.8$2:0.3\n" + "2:2.0#3:0.3\n" +
      "3:3.0 \n" + "4:4.0\n" + "5:5.0#6:0.7\n" + "6:6.0\n"

    val adjFileSplit = createTempFile(adjFileContent)

    val graph = Graph.fromAdjacencyListFile[Long, Double, Double](
      filePath = adjFileSplit.getPath.toString,
      sourceNeighborsDelimiter = "#",
      verticesDelimiter = "$",
      vertexValueDelimiter = ":",
      env = env)

    val tempFile = System.getProperty("java.io.tmpdir") + "tempGraph.txt"

    graph.writeAsAdjacencyList(tempFile, "#", "$", ":");

    val result = graph.getTriplets().collect()
    expectedResult = "0,1,0.0,1.0,0.1\n" + "0,4,0.0,4.0,0.2\n" + "0,5,0.0,5.0,0.3\n" +
      "1,0,1.0,0.0,0.8\n" + "1,2,1.0,2.0,0.3\n" + "2,3,2.0,3.0,0.3\n" + "5,6,5.0,6.0,0.7\n"

    val myGraph = Graph.fromAdjacencyListFile[Long, Double, Double](
      filePath = tempFile,
      sourceNeighborsDelimiter = "#",
      verticesDelimiter = "$",
      vertexValueDelimiter = ":",
      env = env)

    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
    assert(graph.getVertexIds().count() == 7)
  }


  @throws(classOf[IOException])
  def createTempFile(content: String): FileInputSplit = {
    val tempFile = File.createTempFile("test_contents", "tmp")
    tempFile.deleteOnExit()

    val wrt = new OutputStreamWriter(new FileOutputStream(tempFile), Charsets.UTF_8)
    wrt.write(content)
    wrt.close()

    new FileInputSplit(0, new Path(tempFile.toURI.toString), 0, tempFile.length,
      Array("localhost"))
  }

  final class VertexDoubleIdAssigner extends MapFunction[Long, Double] {
    @throws(classOf[Exception])
    def map(id: Long): Double = {
      id.toDouble
    }
  }

}
