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
 *     `"1\n2\n12\n42\n63\n"` gives five vertices (1), (2), (12), (42), and (63).
 *   - Edges are represented as pairs for vertex IDs which are separated by space characters. Edges
 *     are separated by new-line characters. For example `"1 2\n2 12\n1 12\n42 63\n"`
 *     gives four (undirected) edges (1)-(2), (2)-(12), (1)-(12), and (42)-(63).
 *
 * Usage:
 * {{{
 *   ConnectedComponents <vertices path> <edges path> <result path> <max number of iterations>
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
    if (!parseParameters(args)) {
      return
    }
    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read vertex and edge data
    // assign the initial components (equal to the vertex id)
    val vertices = getVerticesDataSet(env).map { id => (id, id) }.withForwardedFields("*->_1;*->_2")

    // undirected edges by emitting for each input edge the input edges itself and an inverted
    // version
    val edges = getEdgesDataSet(env).flatMap { edge => Seq(edge, (edge._2, edge._1)) }

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
    if (fileOutput) {
      verticesWithComponents.writeAsCsv(outputPath, "\n", " ")
      env.execute("Scala Connected Components Example")
    } else {
      verticesWithComponents.print()
    }

  }
 
  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 4) {
        verticesPath = args(0)
        edgesPath = args(1)
        outputPath = args(2)
        maxIterations = args(3).toInt

        true
      } else {
        System.err.println("Usage: ConnectedComponents <vertices path> <edges path> <result path>" +
          " <max number of iterations>")

        false
      }
    } else {
      System.out.println("Executing Connected Components example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: ConnectedComponents <vertices path> <edges path> <result path>" +
        " <max number of iterations>")

      true
    }
  }

  private def getVerticesDataSet(env: ExecutionEnvironment): DataSet[Long] = {
    if (fileOutput) {
       env.readCsvFile[Tuple1[Long]](
        verticesPath,
        includedFields = Array(0))
        .map { x => x._1 }
    }
    else {
      env.fromCollection(ConnectedComponentsData.VERTICES)
    }
  }
  
  private def getEdgesDataSet(env: ExecutionEnvironment): DataSet[(Long, Long)] = {
    if (fileOutput) {
      env.readCsvFile[(Long, Long)](
        edgesPath,
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
        .map { x => (x._1, x._2)}
    }
    else {
      val edgeData = ConnectedComponentsData.EDGES map {
        case Array(x, y) => (x.asInstanceOf[Long], y.asInstanceOf[Long])
      }
      env.fromCollection(edgeData)
    }
  }

  private var fileOutput: Boolean = false
  private var verticesPath: String = null
  private var edgesPath: String = null
  private var maxIterations: Int = 10
  private var outputPath: String = null
}
