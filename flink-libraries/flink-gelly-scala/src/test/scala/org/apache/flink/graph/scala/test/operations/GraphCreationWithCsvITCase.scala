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
import java.nio.charset.StandardCharsets

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
class GraphCreationWithCsvITCase(mode: MultipleProgramsTestBase.TestExecutionMode) extends
MultipleProgramsTestBase(mode) {

  private var expectedResult: String = null

  @Test
  @throws(classOf[Exception])
  def testCsvWithValues() {
    /*
     * Test with two Csv files, both vertices and edges have values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val verticesContent =  "1,1\n2,2\n3,3\n"
    val verticesSplit = createTempFile(verticesContent)
    val edgesContent =  "1,2,ot\n3,2,tt\n3,1,to\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, Long, String](
        pathVertices = verticesSplit.getPath.toString,
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,1,2,ot\n3,2,3,2,tt\n3,1,3,1,to\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvNoEdgeValues() {
    /*
     * Test with two Csv files; edges have no values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val verticesContent =  "1,one\n2,two\n3,three\n"
    val verticesSplit = createTempFile(verticesContent)
    val edgesContent =  "1,2\n3,2\n3,1\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, String, NullValue](
        pathVertices = verticesSplit.getPath.toString,
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,one,two,(null)\n3,2,three,two,(null)\n3,1,three,one,(null)\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvWithMapperValues() {
    /*
     * Test with edges Csv file and vertex mapper initializer
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val edgesContent =  "1,2,12\n3,2,32\n3,1,31\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, Double, Long](
        pathEdges = edgesSplit.getPath.toString,
        vertexValueInitializer = new VertexDoubleIdAssigner(),
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,1.0,2.0,12\n3,2,3.0,2.0,32\n3,1,3.0,1.0,31\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvNoVertexValues() {
    /*
     * Test with edges Csv file: no vertex values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val edgesContent =  "1,2,12\n3,2,32\n3,1,31\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, NullValue, Long](
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,(null),(null),12\n3,2,(null),(null),32\n" +
      "3,1,(null),(null),31\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvNoValues() {
    /*
     * Test with edges Csv file: neither vertex nor edge values
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val edgesContent =  "1,2\n3,2\n3,1\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, NullValue, NullValue](
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,(null),(null),(null)\n" +
      "3,2,(null),(null),(null)\n3,1,(null),(null),(null)\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvOptionsVertices() {
    /*
     * Test the options for vertices: delimiters, comments, ignore first line.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val verticesContent =  "42#42\t" + "%this-is-a-comment\t" +
      "1#1\t" + "2#2\t" + "3#3\t"
    val verticesSplit = createTempFile(verticesContent)
    val edgesContent =  "1,2,ot\n3,2,tt\n3,1,to\n"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, Long, String](
        pathVertices = verticesSplit.getPath.toString,
        lineDelimiterVertices = "\t",
        fieldDelimiterVertices = "#",
        ignoreFirstLineVertices = true,
        ignoreCommentsVertices = "%",
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,1,2,ot\n3,2,3,2,tt\n3,1,3,1,to\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @Test
  @throws(classOf[Exception])
  def testCsvOptionsEdges() {
    /*
     * Test the options for edges: delimiters, comments, ignore first line.
     */
    val env = ExecutionEnvironment.getExecutionEnvironment
    val verticesContent =  "1,1\n2,2\n3,3\n"
    val verticesSplit = createTempFile(verticesContent)
    val edgesContent =  "42#42#ignore&" + "1#2#ot&" + "3#2#tt&" + "3#1#to&" +
      "//this-is-a-comment"
    val edgesSplit = createTempFile(edgesContent)
    val graph = Graph.fromCsvReader[Long, Long, String](
        pathVertices = verticesSplit.getPath.toString,
        lineDelimiterEdges = "&",
        fieldDelimiterEdges = "#",
        ignoreFirstLineEdges = true,
        ignoreCommentsEdges = "//",
        pathEdges = edgesSplit.getPath.toString,
        env = env)
    
    val result = graph.getTriplets().collect()
    expectedResult = "1,2,1,2,ot\n3,2,3,2,tt\n3,1,3,1,to\n"
    TestBaseUtils.compareResultAsTuples(result.asJava, expectedResult)
  }

  @throws(classOf[IOException])
  def createTempFile(content: String): FileInputSplit = {
    val tempFile = File.createTempFile("test_contents", "tmp")
    tempFile.deleteOnExit()

    val wrt = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8)
    wrt.write(content)
    wrt.close()

    new FileInputSplit(0, new Path(tempFile.toURI.toString), 0, tempFile.length,
        Array("localhost"))
    }

    final class VertexDoubleIdAssigner extends MapFunction[Long, Double] {
      @throws(classOf[Exception])
      def map(id: Long): Double = {id.toDouble}
    }

}
