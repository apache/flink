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

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.test.TestGraphUtils
import org.apache.flink.graph.{Edge, Vertex}
import org.apache.flink.test.util.{AbstractMultipleProgramsTestBase, MultipleProgramsTestBase}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class GraphOperationsITCase(mode: AbstractMultipleProgramsTestBase.TestExecutionMode) extends MultipleProgramsTestBase(mode) {

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
        compareResultsByLinesInMemory(expectedResult, resultPath)
    }

    @Test
    @throws(classOf[Exception])
    def testUndirected {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        graph.getUndirected().getEdgesAsTuple3().writeAsCsv(resultPath)
        env.execute
        expectedResult = "1,2,12\n" + "2,1,12\n" + "1,3,13\n" + "3,1,13\n" + "2,3,23\n" + "3,2,23\n" + "3,4,34\n" + "4,3,34\n" + "3,5,35\n" + "5,3,35\n" + "4,5,45\n" + "5,4,45\n" + "5,1,51\n" + "1,5,51\n"
    }

    @Test
    @throws(classOf[Exception])
    def testReverse {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        graph.reverse().getEdgesAsTuple3().writeAsCsv(resultPath)
        env.execute
        expectedResult = "2,1,12\n" + "3,1,13\n" + "3,2,23\n" + "4,3,34\n" + "5,3,35\n" + "5,4,45\n" + "1,5,51\n"
    }

    @Test
    @throws(classOf[Exception])
    def testSubGraph {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
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
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
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
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
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
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
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
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
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
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        graph.filterOnEdges(
            edge => edge.getValue > 34
        ).getEdgesAsTuple3().writeAsCsv(resultPath)
        env.execute
        expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n"
    }
}
