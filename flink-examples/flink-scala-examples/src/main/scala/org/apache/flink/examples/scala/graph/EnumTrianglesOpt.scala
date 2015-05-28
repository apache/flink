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

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.scala._
import scala.collection.JavaConverters._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.util.Collector
import org.apache.flink.examples.java.graph.util.EnumTrianglesData
import org.apache.flink.api.common.operators.Order
import scala.collection.mutable.MutableList

import scala.collection.mutable


/**
 * Triangle enumeration is a pre-processing step to find closely connected parts in graphs.
 * A triangle consists of three edges that connect three vertices with each other.
 *
 * The basic algorithm works as follows:
 * It groups all edges that share a common vertex and builds triads, i.e., triples of vertices 
 * that are connected by two edges. Finally, all triads are filtered for which no third edge exists 
 * that closes the triangle.
 *
 * For a group of ''i'' edges that share a common vertex, the number of built triads is quadratic
 * ''(n*(n-1))/2)''. Therefore, an optimization of the algorithm is to group edges on the vertex
 * with the smaller output degree to reduce the number of triads.
 * This implementation extends the basic algorithm by computing output degrees of edge vertices and 
 * grouping on edges on the vertex with the smaller degree.
 *
 * Input files are plain text files and must be formatted as follows:
 *
 *  - Edges are represented as pairs for vertex IDs which are separated by space
 *    characters. Edges are separated by new-line characters.
 *    For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (undirected) edges (1)-(2), (2)-(12),
 *    (1)-(12), and (42)-(63) that include a triangle
 *
 * <pre>
 *     (1)
 *     /  \
 *   (2)-(12)
 * </pre>
 *
 * Usage:
 * {{{
 *   EnumTriangleOpt <edge path> <result path>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.graph.util.EnumTrianglesData]].
 *
 * This example shows how to use:
 *
 *  - Custom Java objects which extend Tuple
 *  - Group Sorting
 *
 */
object EnumTrianglesOpt {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read input data
    val edges = getEdgeDataSet(env)

    val edgesWithDegrees = edges
      // duplicate and switch edges
      .flatMap(e => Seq(e, Edge(e.v2, e.v1)))
      // add degree of first vertex
      .groupBy("v1").sortGroup("v2", Order.ASCENDING).reduceGroup(new DegreeCounter())
      // join degrees of vertices
      .groupBy("v1", "v2").reduce {
        (e1, e2) =>
          if (e1.d2 == 0) {
            new EdgeWithDegrees(e1.v1, e1.d1, e1.v2, e2.d2)
          } else {
            new EdgeWithDegrees(e1.v1, e2.d1, e1.v2, e1.d2)
          }
      }.withForwardedFields("v1;v2")

    // project edges by degrees, vertex with smaller degree comes first
    val edgesByDegree = edgesWithDegrees
      .map(e => if (e.d1 <= e.d2) Edge(e.v1, e.v2) else Edge(e.v2, e.v1))
    // project edges by Id, vertex with smaller Id comes first
    val edgesById = edgesByDegree
      .map(e => if (e.v1 < e.v2) e else Edge(e.v2, e.v1))

    val triangles = edgesByDegree
      // build triads
      .groupBy("v1").sortGroup("v2", Order.ASCENDING).reduceGroup(new TriadBuilder())
      // filter triads
      .join(edgesById).where("v2", "v3").equalTo("v1", "v2") { (t, _) => t}
        .withForwardedFieldsFirst("*")

    // emit result
    if (fileOutput) {
      triangles.writeAsCsv(outputPath, "\n", ",")
      // execute program
      env.execute("TriangleEnumeration Example")
    } else {
      triangles.print()
    }

  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Edge(v1: Int, v2: Int) extends Serializable

  case class Triad(v1: Int, v2: Int, v3: Int) extends Serializable

  case class EdgeWithDegrees(v1: Int, d1: Int, v2: Int, d2: Int) extends Serializable


  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   * Counts the number of edges that share a common vertex.
   * Emits one edge for each input edge with a degree annotation for the shared vertex.
   * For each emitted edge, the first vertex is the vertex with the smaller id.
   */
  class DegreeCounter extends GroupReduceFunction[Edge, EdgeWithDegrees] {

    val vertices = mutable.MutableList[Integer]()
    var groupVertex = 0

    override def reduce(edges: java.lang.Iterable[Edge], out: Collector[EdgeWithDegrees]) = {

      // empty vertex list
      vertices.clear()

      // collect all vertices
      for (e <- edges.asScala) {
        groupVertex = e.v1
        if (!vertices.contains(e.v2) && e.v1 != e.v2) {
          vertices += e.v2
        }
      }

      // count vertices to obtain degree of groupVertex
      val degree = vertices.length

      // create and emit edges with degrees
      for (v <- vertices) {
        if (v < groupVertex) {
          out.collect(new EdgeWithDegrees(v, 0, groupVertex, degree))
        } else {
          out.collect(new EdgeWithDegrees(groupVertex, degree, v, 0))
        }
      }
    }
  }

  /**
   * Builds triads (triples of vertices) from pairs of edges that share a vertex.
   * The first vertex of a triad is the shared vertex, the second and third vertex are ordered by
   * vertexId.
   * Assumes that input edges share the first vertex and are in ascending order of the second
   * vertex.
   */
  @ForwardedFields(Array("v1"))
  class TriadBuilder extends GroupReduceFunction[Edge, Triad] {

    val vertices = mutable.MutableList[Integer]()

    override def reduce(edges: java.lang.Iterable[Edge], out: Collector[Triad]) = {

      // clear vertex list
      vertices.clear()

      // build and emit triads
      for (e <- edges.asScala) {
        // combine vertex with all previously read vertices
        for (v <- vertices) {
          out.collect(Triad(e.v1, v, e.v2))
        }
        vertices += e.v2
      }
    }
  }

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        edgePath = args(0)
        outputPath = args(1)

        true
      } else {
        System.err.println("Usage: EnumTriangleOpt <edge path> <result path>")

        false
      }
    } else {
      System.out.println("Executing Enum Triangles Optimized example with built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  Usage: EnumTriangleBasic <edge path> <result path>")

      true
    }
  }

  private def getEdgeDataSet(env: ExecutionEnvironment): DataSet[Edge] = {
    if (fileOutput) {
      env.readCsvFile[Edge](
        edgePath,
        fieldDelimiter = " ",
        includedFields = Array(0, 1))
    } else {
      val edges = EnumTrianglesData.EDGES.map {
        case Array(v1, v2) => new Edge(v1.asInstanceOf[Int], v2.asInstanceOf[Int])}
      env.fromCollection(edges)
    }
  }


  private var fileOutput: Boolean = false
  private var edgePath: String = null
  private var outputPath: String = null

}
