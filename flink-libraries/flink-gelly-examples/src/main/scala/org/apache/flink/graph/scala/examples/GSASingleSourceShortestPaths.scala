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

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala._
import org.apache.flink.graph.Edge
import org.apache.flink.graph.examples.data.SingleSourceShortestPathsData
import org.apache.flink.graph.gsa.{ApplyFunction, GatherFunction, Neighbor, SumFunction}
import org.apache.flink.graph.scala._
import org.apache.flink.graph.scala.utils.Tuple3ToEdgeMap

/**
 * This example shows how to use Gelly's gather-sum-apply iterations.
 * 
 * It is an implementation of the Single-Source-Shortest-Paths algorithm. 
 *
 * The input file is a plain text file and must be formatted as follows:
 * Edges are represented by tuples of srcVertexId, trgVertexId, distance which are
 * separated by tabs. Edges themselves are separated by newlines.
 * For example: <code>1\t2\t0.1\n1\t3\t1.4\n</code> defines two edges,
 * edge 1-2 with distance 0.1, and edge 1-3 with distance 1.4.
 *
 * If no parameters are provided, the program is run with default data from
 * [[SingleSourceShortestPathsData]]
 */
object GSASingleSourceShortestPaths {
  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val edges: DataSet[Edge[Long, Double]] = getEdgesDataSet(env)
    val graph = Graph.fromDataSet[Long, Double, Double](edges, new InitVertices(srcVertexId), env)

    // Execute the gather-sum-apply iteration
    val result = graph.runGatherSumApplyIteration(new CalculateDistances, new ChooseMinDistance,
      new UpdateDistance, maxIterations)

    // Extract the vertices as the result
    val singleSourceShortestPaths = result.getVertices

    // emit result
    if (fileOutput) {
      singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",")
      env.execute("GSA Single Source Shortest Paths Example")
    } else {
      singleSourceShortestPaths.print()
    }
  }

  // --------------------------------------------------------------------------------------------
  //  Single Source Shortest Path UDFs
  // --------------------------------------------------------------------------------------------

  private final class InitVertices(srcId: Long) extends MapFunction[Long, Double] {

    override def map(id: Long) = {
      if (id.equals(srcId)) {
        0.0
      } else {
        Double.PositiveInfinity
      }
    }
  }

  private final class CalculateDistances extends GatherFunction[Double, Double, Double] {
    override def gather(neighbor: Neighbor[Double, Double]) = {
      neighbor.getNeighborValue + neighbor.getEdgeValue
    }
  }

  private final class ChooseMinDistance extends SumFunction[Double, Double, Double] {
    override def sum(newValue: Double, currentValue: Double) = {
      Math.min(newValue, currentValue)
    }
  }

  private final class UpdateDistance extends ApplyFunction[Long, Double, Double] {
    override def apply(newDistance: Double, oldDistance: Double) = {
      if (newDistance < oldDistance) {
        setResult(newDistance)
      }
    }
  }

  // **************************************************************************
  // UTIL METHODS
  // **************************************************************************

  private var fileOutput = false
  private var srcVertexId = 1L
  private var edgesInputPath: String = null
  private var outputPath: String = null
  private var maxIterations = 5

  private def parseParameters(args: Array[String]): Boolean = {
    if(args.length > 0) {
      if(args.length != 4) {
        System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
          " <input edges path> <output path> <num iterations>")
      }
      fileOutput = true
      srcVertexId = args(0).toLong
      edgesInputPath = args(1)
      outputPath = args(2)
      maxIterations = 3
    } else {
      System.out.println("Executing Single Source Shortest Paths example "
        + "with default parameters and built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("Usage: SingleSourceShortestPaths <source vertex id>" +
        " <input edges path> <output path> <num iterations>")
    }
    true
  }

  private def getEdgesDataSet(env: ExecutionEnvironment): DataSet[Edge[Long, Double]] = {
    if (fileOutput) {
      env.readCsvFile[(Long, Long, Double)](edgesInputPath,
        lineDelimiter = "\n",
        fieldDelimiter = "\t")
        .map(new Tuple3ToEdgeMap[Long, Double]())
    } else {
      val edgeData = SingleSourceShortestPathsData.DEFAULT_EDGES map {
        case Array(x, y, z) => (x.asInstanceOf[Long], y.asInstanceOf[Long],
          z.asInstanceOf[Double])
      }
      env.fromCollection(edgeData).map(new Tuple3ToEdgeMap[Long, Double]())
    }
  }
}
