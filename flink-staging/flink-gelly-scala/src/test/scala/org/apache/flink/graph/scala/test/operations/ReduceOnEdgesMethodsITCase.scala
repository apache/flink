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
import org.apache.flink.graph.{Edge, EdgeDirection, Vertex}
import org.apache.flink.test.util.{AbstractMultipleProgramsTestBase, MultipleProgramsTestBase}
import org.apache.flink.util.Collector
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Rule, Test}

@RunWith(classOf[Parameterized])
class ReduceOnEdgesMethodsITCase(mode: AbstractMultipleProgramsTestBase.TestExecutionMode) extends MultipleProgramsTestBase(mode) {

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
    def testWithSameValue {
        val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
        val graph: Graph[Long, Long, Long] = Graph.fromDataSet(TestGraphUtils.getLongLongVertexData(env), TestGraphUtils.getLongLongEdgeData(env), env)
        val result = graph.groupReduceOnEdges(new SelectNeighborsValueGreaterThanFour, EdgeDirection.ALL)
        result.writeAsCsv(resultPath)
        env.execute

        expectedResult = "5,1\n" + "5,3\n" + "5,4"
    }

    final class SelectNeighborsValueGreaterThanFour extends EdgesFunctionWithVertexValue[Long, Long, Long, (Long, Long)] {
        @throws(classOf[Exception])
        override def iterationFunction(v: Vertex[Long, Long], edges: Iterable[Edge[Long, Long]], out: Collector[(Long, Long)]): Unit = {
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


}