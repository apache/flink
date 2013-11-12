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

import eu.stratosphere.pact.common.`type`.base.PactInteger

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.plan.PlanAssembler
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription

import scala.math._
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

import eu.stratosphere.scala.analysis.GlobalSchemaPrinter

object RunEnumTrianglesOnEdgesWithDegrees {
  def main(args: Array[String]) {
    val enumTriangles = new EnumTrianglesOnEdgesWithDegrees
    if (args.size < 3) {
      println(enumTriangles.getDescription)
      return
    }
    val plan = enumTriangles.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

/**
 * Enumerates all triangles build by three connected vertices in a graph.
 * The graph is represented as edges (pairs of vertices) with annotated vertex degrees. * 
 */
class EnumTrianglesOnEdgesWithDegrees extends PlanAssembler with PlanAssemblerDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input file] [output file]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }

  /*
   * Output formatting function for triangles.
   */
  def formatTriangle = (v1: Int, v2: Int, v3: Int) => "%d,%d,%d".format(v1, v2, v3)
  
  /*
   * Extracts degree information and projects edges such that lower degree vertex comes first.
   */
  def projectVertexesWithDegrees(e: (String, String)): (Int, Int) = {
    val v1 = e._1.split(",")
    val v2 = e._2.split(",")
    if (v1(1).toInt <= v2(1).toInt)
      (v1(0).toInt, v2(0).toInt)
    else
      (v2(0).toInt, v1(0).toInt)
  } 
  
  /*
   * Joins projected edges on lower vertex id.
   * Emits a triad (triangle candidate with one missing edge) for each unique combination of edges.
   * Ensures that vertex 2 and 3 are ordered by vertex id.  
   */
  def buildTriads(eI : Iterator[(Int, Int)]): List[(Int, Int, Int)] = {
    val eL = eI.toList
    for (e1 <- eL; 
         e2 <- eL 
         if e1._2 < e2._2) yield
      (e1._1, e1._2, e2._2)
  }
  
  def getScalaPlan(numSubTasks: Int, edgeInput: String, triangleOutput: String) = {
    
    /*
     * Input format for edges with degrees
     * Edges are separated by new line '\n'. 
     * An edge is represented by two vertex IDs with associated vertex degrees.
     * The format of an edge is "<vertexID1>,<vertexDegree1>|<vertexID2>,<vertexDegree2>" 
     */
    val vertexesWithDegrees = DataSource(edgeInput, CsvInputFormat[(String, String)]("\n", "|"))

    /*
     * Project edges such that vertex with lower degree comes first (record position 1) and remove the degrees.
     */
    val edgesByDegree = vertexesWithDegrees map { projectVertexesWithDegrees }
    
    /*
     * Project edges such that vertex with lower ID comes first (record position) and remove degrees.
     */
    val edgesByID = edgesByDegree map { (x) => if (x._1 < x._2) (x._1, x._2) else (x._2, x._1) }
    
    /*
     * Build triads by joining edges on common vertex.
     */   
    val triads = edgesByDegree groupBy { _._1 } reduceGroup { buildTriads } flatMap {x => x.iterator }
    
    /*
     * Join triads with projected edges to 'close' triads.
     * This filters triads without a closing edge.
     */
    val triangles = triads join edgesByID where { t => (t._2, t._3) } isEqualTo { e => (e._1, e._2) } map { (t, e) => t }
    
    /*
     * Emit triangles
     */
    val output = triangles.write(triangleOutput, DelimitedOutputFormat(formatTriangle.tupled))
  
    val plan = new ScalaPlan(Seq(output), "Enumerate Triangles on Edges with Degrees")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}