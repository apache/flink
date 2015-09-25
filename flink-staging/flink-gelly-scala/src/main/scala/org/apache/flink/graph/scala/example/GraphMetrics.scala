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
package org.apache.flink.graph.scala.example

import org.apache.flink.api.scala._
import org.apache.flink.graph.scala._
import org.apache.flink.types.NullValue
import org.apache.flink.graph.Edge
import org.apache.flink.util.Collector

/**
 * This example illustrates how to use Gelly metrics methods and get simple statistics
 * from the input graph.  
 * 
 * The program creates a random graph and computes and prints
 * the following metrics:
 * - number of vertices
 * - number of edges
 * - average node degree
 * - the vertex ids with the max/min in- and out-degrees
 *
 * The input file is expected to contain one edge per line,
 * with long IDs and no values, in the following format:
 * {{{
 *   <sourceVertexID>\t<targetVertexID>
 * }}}
 * If no arguments are provided, the example runs with a random graph of 100 vertices.
 *
 */
object GraphMetrics {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    /** create the graph **/
    val graph: Graph[Long, NullValue, NullValue] = Graph.fromDataSet(getEdgeDataSet(env), env)

    /** get the number of vertices **/
    val numVertices = graph.numberOfVertices;

    /** get the number of edges **/
    val numEdges = graph.numberOfEdges;

    /** compute the average node degree **/
    val verticesWithDegrees = graph.getDegrees;
    val avgDegree = verticesWithDegrees.sum(1).map(in => (in._2 / numVertices).toDouble)

    /** find the vertex with the maximum in-degree **/
    val maxInDegreeVertex = graph.inDegrees.max(1).map(in => in._1)

    /** find the vertex with the minimum in-degree **/
    val minInDegreeVertex = graph.inDegrees.min(1).map(in => in._1)

    /** find the vertex with the maximum out-degree **/
    val maxOutDegreeVertex = graph.outDegrees.max(1).map(in => in._1)

    /** find the vertex with the minimum out-degree **/
    val minOutDegreeVertex = graph.outDegrees.min(1).map(in => in._1)

    /** print the results **/
    env.fromElements(numVertices).printOnTaskManager("Total number of vertices")
    env.fromElements(numEdges).printOnTaskManager("Total number of edges")
    avgDegree.printOnTaskManager("Average node degree")
    maxInDegreeVertex.printOnTaskManager("Vertex with Max in-degree")
    minInDegreeVertex.printOnTaskManager("Vertex with Max in-degree")
    maxOutDegreeVertex.printOnTaskManager("Vertex with Max out-degree")
    minOutDegreeVertex.printOnTaskManager("Vertex with Max out-degree")

  }

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 1) {
        edgesPath = args(0)
        true
      } else {
        System.err.println("Usage: GraphMetrics <edges path>")
        false
      }
    } else {
      System.out.println("Executing GraphMetrics example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: GraphMetrics <edges path>")
      true
    }
  }

  private def getEdgeDataSet(env: ExecutionEnvironment): DataSet[Edge[Long, NullValue]] = {
    if (fileOutput) {
      env.readCsvFile[(Long, Long)](
          edgesPath,
          fieldDelimiter = "\t").map(
          in => new Edge[Long, NullValue](in._1, in._2, NullValue.getInstance()))
    }
    else {
      env.generateSequence(1, numVertices).flatMap[Edge[Long, NullValue]](
         (key: Long, out: Collector[Edge[Long, NullValue]]) => {
         val numOutEdges: Int = (Math.random() * (numVertices / 2)).toInt
          for ( i <- 0 to numOutEdges ) {
            var target: Long = ((Math.random() * numVertices) + 1).toLong
              new Edge[Long, NullValue](key, target, NullValue.getInstance())
          }
        })
      }
    }

  private var fileOutput: Boolean = false
  private var edgesPath: String = null
  private var outputPath: String = null
  private val numVertices = 100
}
