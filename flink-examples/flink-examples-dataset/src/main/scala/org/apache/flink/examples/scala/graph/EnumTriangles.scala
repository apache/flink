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

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.examples.java.graph.util.EnumTrianglesData
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Triangle enumeration is a pre-processing step to find closely connected parts in graphs.
 * A triangle consists of three edges that connect three vertices with each other.
 * 
 * The algorithm works as follows:
 * It groups all edges that share a common vertex and builds triads, i.e., triples of vertices 
 * that are connected by two edges. Finally, all triads are filtered for which no third edge exists 
 * that closes the triangle.
 *  
 * Input files are plain text files and must be formatted as follows:
 *
 *  - Edges are represented as pairs for vertex IDs which are separated by space
 *   characters. Edges are separated by new-line characters.
 *   For example `"1 2\n2 12\n1 12\n42 63"` gives four (undirected) edges (1)-(2), (2)-(12),
 *   (1)-(12), and (42)-(63) that include a triangle
 *
 * <pre>
 *     (1)
 *     /  \
 *   (2)-(12)
 * </pre>
 * 
 * Usage: 
 * {{{
 * EnumTriangleBasic <edge path> <result path>
 * }}}
 * <br>
 * If no parameters are provided, the program is run with default data from 
 * [[org.apache.flink.examples.java.graph.util.EnumTrianglesData]]
 * 
 * This example shows how to use:
 *
 *  - Custom Java objects which extend Tuple
 *  - Group Sorting
 *
 */
object EnumTriangles {

  def main(args: Array[String]) {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    // read input data
    val edges =
      if (params.has("edges")) {
        env.readCsvFile[Edge](
          filePath = params.get("edges"),
          fieldDelimiter = " ",
          includedFields = Array(0, 1))
      } else {
        println("Executing EnumTriangles example with default edges data set.")
        println("Use --edges to specify file input.")
        val edges = EnumTrianglesData.EDGES.map {
          case Array(v1, v2) => new Edge(v1.asInstanceOf[Int], v2.asInstanceOf[Int])
        }
        env.fromCollection(edges)
      }
    
    // project edges by vertex id
    val edgesById = edges map(e => if (e.v1 < e.v2) e else Edge(e.v2, e.v1) )
    
    val triangles = edgesById
            // build triads
            .groupBy("v1").sortGroup("v2", Order.ASCENDING).reduceGroup(new TriadBuilder())
            // filter triads
            .join(edgesById).where("v2", "v3").equalTo("v1", "v2") { (t, _) => t }
              .withForwardedFieldsFirst("*")
    
    // emit result
    if (params.has("output")) {
      triangles.writeAsCsv(params.get("output"), "\n", ",")
      // execute program
      env.execute("TriangleEnumeration Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      triangles.print()
    }
    

  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Edge(v1: Int, v2: Int) extends Serializable
  case class Triad(v1: Int, v2: Int, v3: Int) extends Serializable
  
    
  // *************************************************************************
  //     USER FUNCTIONS
  // *************************************************************************

  /**
   *  Builds triads (triples of vertices) from pairs of edges that share a vertex. The first vertex
   *  of a triad is the shared vertex, the second and third vertex are ordered by vertexId. Assumes
   *  that input edges share the first vertex and are in ascending order of the second vertex.
   */
  @ForwardedFields(Array("v1->v1"))
  class TriadBuilder extends GroupReduceFunction[Edge, Triad] {

    val vertices = mutable.MutableList[Integer]()
    
    override def reduce(edges: java.lang.Iterable[Edge], out: Collector[Triad]) = {
      
      // clear vertex list
      vertices.clear()

      // build and emit triads
      for(e <- edges.asScala) {
      
        // combine vertex with all previously read vertices
        for(v <- vertices) {
          out.collect(Triad(e.v1, v, e.v2))
        }
        vertices += e.v2
      }
    }
  }
}
