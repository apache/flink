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

package org.apache.flink.graph.scala.examples

import java.lang.Long

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.examples.data.ConnectedComponentsDefaultData
import org.apache.flink.graph.library.GSAConnectedComponents
import org.apache.flink.graph.scala._
import org.apache.flink.types.NullValue

/**
 * This example shows how to use Gelly's library methods.
 * You can find all available library methods in [[org.apache.flink.graph.library]]. 
 * 
 * In particular, this example uses the
 * [[GSAConnectedComponents]]
 * library method to compute the connected components of the input graph.
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId which are
 * separated by tabs. Edges themselves are separated by newlines.
 * For example: <code>1\t2\n1\t3\n</code> defines two edges,
 * 1-2 and 1-3.
 *
 * Usage {{
 *   ConnectedComponents <edge path> <result path> <number of iterations>
 *   }}
 * If no parameters are provided, the program is run with default data from
 * [[ConnectedComponentsDefaultData]]
 */
object ConnectedComponents {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val edges: DataSet[Edge[Long, NullValue]] = getEdgesDataSet(env)
    val graph = Graph.fromDataSet[Long, Long, NullValue](edges, new InitVertices, env)

    val components = graph.run(new GSAConnectedComponents[Long, Long, NullValue](maxIterations))


    // emit result
    if (fileOutput) {
      components.writeAsCsv(outputPath, "\n", ",")
      env.execute("Connected Components Example")
    } else {
      components.print()
    }
  }

  private final class InitVertices extends MapFunction[Long, Long] {
    override def map(id: Long) = id
  }

  // ***********************************************************************
  // UTIL METHODS
  // ***********************************************************************

    private var fileOutput = false
    private var edgesInputPath: String = null
    private var outputPath: String = null
    private var maxIterations = ConnectedComponentsDefaultData.MAX_ITERATIONS

    private def parseParameters(args: Array[String]): Boolean = {
      if(args.length > 0) {
        if(args.length != 3) {
          System.err.println("Usage ConnectedComponents <edge path> <output path> " +
            "<num iterations>")
        }
        fileOutput = true
        edgesInputPath = args(0)
        outputPath = args(1)
        maxIterations = 2
      } else {
        System.out.println("Executing ConnectedComponents example with default parameters" +
          " and built-in default data.")
        System.out.println("  Provide parameters to read input data from files.")
        System.out.println("  See the documentation for the correct format of input files.")
        System.out.println("Usage ConnectedComponents <edge path> <output path> " +
          "<num iterations>")
      }
      true
    }

    private def getEdgesDataSet(env: ExecutionEnvironment): DataSet[Edge[Long, NullValue]] = {
      if (fileOutput) {
        env.readCsvFile[(Long, Long)](edgesInputPath,
          lineDelimiter = "\n",
          fieldDelimiter = "\t")
          .map(edge => new Edge[Long, NullValue](edge._1, edge._2, NullValue.getInstance))
      } else {
        val edgeData = ConnectedComponentsDefaultData.DEFAULT_EDGES map {
          case Array(x, y) => (x.asInstanceOf[Long], y.asInstanceOf[Long])
        }
        env.fromCollection(edgeData).map(
        edge => new Edge[Long, NullValue](edge._1, edge._2, NullValue.getInstance))
      }
    }
}
