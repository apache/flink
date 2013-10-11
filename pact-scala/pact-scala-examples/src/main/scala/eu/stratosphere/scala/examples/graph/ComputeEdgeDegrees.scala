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
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.ScalaPlanAssembler
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.scala.operators.RecordDataSourceFormat
import eu.stratosphere.scala.operators.optionToIterator
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat


object RunComputeEdgeDegrees {
  def main(args: Array[String]) {
    val plan = ComputeEdgeDegrees.getPlan(
      "file:///tmp/edges",
      "file:///tmp/edgeDegrees")

    GlobalSchemaPrinter.printSchema(plan)
    LocalExecutor.execute(plan)

    System.exit(0)
  }
}

class ComputeEdgeDegrees extends ScalaPlanAssembler with PlanAssemblerDescription {
  override def getDescription = "-input <file>  -output <file>"

  override def getScalaPlan(args: Args) = ComputeEdgeDegrees.getPlan(args("input"), args("output"))
}

/**
 * Annotates edges with associated vertice degrees.
 */
object ComputeEdgeDegrees {
  
  /*
   * Output formatting function for edges with annotated degrees
   */
  def formatEdgeWithDegrees = (v1: Int, v2: Int, c1: Int, c2: Int) => "%d %d %d %d".format(v1, v2, c1, c2)
  
  def getPlan(edgeInput: String, annotatedEdgeOutput: String) = {
    
    /*
     * Input format for edges. 
     * Edges are separated by new line '\n'. 
     * An edge is represented as two Integer vertex IDs which are separated by a blank ' '.
     */
    val edges = DataSource(edgeInput, RecordDataSourceFormat[(Int, Int)]("\n", " "))

    /*
     * Project all edges such that the lower vertex ID is the first vertex ID.
     */
    val projEdges = edges map { e => if (e._1 < e._2) e else (e._2, e._1) }
    
    /*
     * Remove duplicate edges by grouping on the whole edge and returning the first edge of the group.
     */
    val uniqEdges = projEdges groupBy { e => e } hadoopReduce { i => i.next }
    
    /*
     * Extract both vertex IDs from an edge.
     */
    val vertices = uniqEdges flatMap { (e) => Iterator(e._1, e._2) map {v => (v, 1)} }
    
    /*
     * Count the occurrences of each vertex ID.
     */
    val vertexCnts = vertices groupBy { _._1 } reduce { (v1, v2) => (v1._1, v1._2 + v2._2) } 
    
    /*
     * Join the first vertex of each edge with its count. 
     */
    val firstDegreeEdges = uniqEdges join vertexCnts where {_._1} isEqualTo {_._1} map { (e, vc) => (e._1, e._2, vc._2) }
    
    /*
     * Join the second vertex of each edge with its count.
     */
    val bothDegreeEdges = firstDegreeEdges join vertexCnts where {_._2} isEqualTo {_._1} map { (e, vc) => (e._1, e._2, e._3, vc._2) }
    
    /*
     * Emit annotated edges.
     */
    val output = bothDegreeEdges.write(annotatedEdgeOutput, DelimitedDataSinkFormat(formatEdgeWithDegrees.tupled))
  
    new ScalaPlan(Seq(output), "Compute Edge Degrees")
  }
}