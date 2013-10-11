/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.examples.graph

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.ScalaPlanAssembler
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.scala.operators.optionToIterator
import eu.stratosphere.scala.operators.RecordDataSourceFormat
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat


object RunEnumTrianglesOnEdgesWithDegrees {
  def main(args: Array[String]) {
    
    val plan = EnumTrianglesOnEdgesWithDegrees.getPlan(
      "file:///tmp/edgeDegrees",
      "file:///tmp/triangles")

    GlobalSchemaPrinter.printSchema(plan)
    LocalExecutor.execute(plan)

    System.exit(0)
  }
}

class EnumTrianglesOnEdgesWithDegreesDescriptor extends ScalaPlanAssembler with PlanAssemblerDescription {
  override def getDescription = "-input <file>  -output <file>"

  override def getScalaPlan(args: Args) = EnumTrianglesOnEdgesWithDegrees.getPlan(args("input"), args("output"))
}

/**
 * Enumerates all triangles build by three connected vertices in a graph.
 * The graph is represented as edges (pairs of vertices) with annotated vertex degrees. * 
 */
object EnumTrianglesOnEdgesWithDegrees {
  
  /*
   * Output formatting function for triangles.
   */
  def formatTriangle = (v1: Int, v2: Int, v3: Int) => "%d %d %d".format(v1, v2, v3)
  
  def getPlan(edgeInput: String, triangleOutput: String) = {
    
    /*
     * Input format for edges with degrees
     * Edges are separated by new line '\n'. 
     * An edge with degrees is represented by four Integers (vertex IDs) separated by one space ' '.
     * The first two Integers are the vertex IDs.
     * The third and forth Integer are the degrees of the first and second vertex, respectively. 
     */
    val edgeWithDegrees = DataSource(edgeInput, RecordDataSourceFormat[(Int, Int, Int, Int)]("\n", " "))

    /*
     * Project edges such that vertex with higher degree comes first (record position 1) and remove the degrees.
     */
    val projEdges = edgeWithDegrees map { (x) => if (x._3 > x._4) (x._1, x._2) else (x._2, x._1) }
    
    /*
     * Join projected edges with themselves on lower vertex id.
     * Emit a triad (triangle candidate with one missing edge) for each unique combination of edges. 
     * Ensure that vertices are ordered by vertex id. 
     */   
    val triads = projEdges join projEdges where { _._1 } isEqualTo { _._1 } flatMap { (e1, e2) =>
        (e1, e2) match {
          case ((v11, v12), (_, v22)) if v12 < v22 => Some((v11, v12, v22))
          case _ => None
        }
      }
    
    /*
     * Join triads with projected edges to 'close' triads.
     * This filters triads without a closing edge.
     */
    val triangles = triads join projEdges where { t => (t._2, t._3) } isEqualTo { e => (e._1, e._2) } map { (t, e) => t }
    
    /*
     * Emit triangles
     */
    val output = triangles.write(triangleOutput, DelimitedDataSinkFormat(formatTriangle.tupled))
  
    new ScalaPlan(Seq(output), "Enumerate Triangles on Edges with Degrees")
  }
}