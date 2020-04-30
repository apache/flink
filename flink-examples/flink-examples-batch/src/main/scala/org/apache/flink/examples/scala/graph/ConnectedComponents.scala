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

package org.apache.flink.examples.scala.graph

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData
import org.apache.flink.util.Collector

/**
 * An implementation of the connected components algorithm, using a delta iteration.
 *
 * Initially, the algorithm assigns each vertex an unique ID. In each step, a vertex picks the
 * minimum of its own ID and its neighbors' IDs, as its new ID and tells its neighbors about its
 * new ID. After the algorithm has completed, all vertices in the same component will have the same
 * ID.
 *
 * A vertex whose component ID did not change needs not propagate its information in the next
 * step. Because of that, the algorithm is easily expressible via a delta iteration. We here model
 * the solution set as the vertices with their current component ids, and the workset as the changed
 * vertices. Because we see all vertices initially as changed, the initial workset and the initial
 * solution set are identical. Also, the delta to the solution set is consequently also the next
 * workset.
 * 
 * Input files are plain text files and must be formatted as follows:
 *
 *   - Vertices represented as IDs and separated by new-line characters. For example,
 *     `"1\n2\n12\n42\n63"` gives five vertices (1), (2), (12), (42), and (63).
 *   - Edges are represented as pairs for vertex IDs which are separated by space characters. Edges
 *     are separated by new-line characters. For example `"1 2\n2 12\n1 12\n42 63"`
 *     gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63).
 *
 * Usage:
 * {{{
 *   ConnectedComponents --vertices <path> --edges <path> --result <path> --iterations <n>
 * }}}
 *   
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.graph.util.ConnectedComponentsData]] and 10 iterations.
 * 
 *
 * This example shows how to use:
 *
 *   - Delta Iterations
 *   - Generic-typed Functions 
 *   
 */
object ConnectedComponents {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val maxIterations: Int = params.getInt("iterations", 10)

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // read vertex and edge data
    // assign the initial components (equal to the vertex id)
    val vertices =
      getVertexDataSet(env, params).map { id => (id, id) }.withForwardedFields("*->_1;*->_2")

    // undirected edges by emitting for each input edge the input
    // edges itself and an inverted version
    val edges =
      getEdgeDataSet(env, params).flatMap { edge => Seq(edge, (edge._2, edge._1)) }

    // open a delta iteration
    val verticesWithComponents = vertices.iterateDelta(vertices, maxIterations, Array("_1")) {
      (s, ws) =>

        // apply the step logic: join with the edges
        val allNeighbors = ws.join(edges).where(0).equalTo(0) { (vertex, edge) =>
          (edge._2, vertex._2)
        }.withForwardedFieldsFirst("_2->_2").withForwardedFieldsSecond("_2->_1")

        // select the minimum neighbor
        val minNeighbors = allNeighbors.groupBy(0).min(1)

        // update if the component of the candidate is smaller
        val updatedComponents = minNeighbors.join(s).where(0).equalTo(0) {
          (newVertex, oldVertex, out: Collector[(Long, Long)]) =>
            if (newVertex._2 < oldVertex._2) out.collect(newVertex)
        }.withForwardedFieldsFirst("*")

        // delta and new workset are identical
        (updatedComponents, updatedComponents)
    }

    if (params.has("output")) {
      verticesWithComponents.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala Connected Components Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      verticesWithComponents.print()
    }

  }

  private def getVertexDataSet(env: ExecutionEnvironment, params: ParameterTool): DataSet[Long] = {
    if (params.has("vertices")) {
      env.readCsvFile[Tuple1[Long]](
        params.get("vertices"),
        includedFields = Array(0))
        .map { x => x._1 }
    }
    else {
      println("Executing ConnectedComponents example with default vertices data set.")
      println("Use --vertices to specify file input.")
      env.fromCollection(ConnectedComponentsData.VERTICES)
    }
  }

  private def getEdgeDataSet(env: ExecutionEnvironment, params: ParameterTool):
                     DataSet[(Long, Long)] = {
    if (params.has("edges")) {
      env.readCsvFile[(Long, Long)](
        params.get("edges"),
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
        .map { x => (x._1, x._2)}
    }
    else {
      println("Executing ConnectedComponents example with default edges data set.")
      println("Use --edges to specify file input.")
      val edgeData = ConnectedComponentsData.EDGES map {
        case Array(x, y) => (x.asInstanceOf[Long], y.asInstanceOf[Long])
      }
      env.fromCollection(edgeData)
    }
  }
}
