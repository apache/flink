/**
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


package org.apache.flink.examples.scala.graph;

import org.apache.flink.client.LocalExecutor
import org.apache.flink.api.common.Program

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators._

object RunConnectedComponents {
 def main(pArgs: Array[String]) {
   
    if (pArgs.size < 5) {
      println("USAGE: <vertices input file> <edges input file> <output file> <max iterations> <degree of parallelism>")
      return
    }
    val plan = new ConnectedComponents().getPlan(pArgs(0), pArgs(1), pArgs(2), pArgs(3), pArgs(4))
    LocalExecutor.execute(plan)
  }
}

class ConnectedComponents extends Program with Serializable {
  
    override def getPlan(args: String*) = {
      val plan = getScalaPlan(args(0), args(1), args(2), args(3).toInt)
      plan.setDefaultParallelism(args(4).toInt)
      plan
  }
  
  def getScalaPlan(verticesInput: String, edgesInput: String, componentsOutput: String, maxIterations: Int) = {

  val vertices = DataSource(verticesInput, DelimitedInputFormat(parseVertex))
  val directedEdges = DataSource(edgesInput, DelimitedInputFormat(parseEdge))

  val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }

    def propagateComponent(s: DataSet[(Int, Int)], ws: DataSet[(Int, Int)]) = {

      val allNeighbors = ws join undirectedEdges where { case (v, _) => v } isEqualTo { case (from, _) => from } map { (w, e) => e._2 -> w._2 }
      val minNeighbors = allNeighbors groupBy { case (to, _) => to } reduceGroup { cs => cs minBy { _._2 } }

      // updated solution elements == new workset
      val s1 = s join minNeighbors where { _._1 } isEqualTo { _._1 } flatMap { (n, s) =>
        (n, s) match {
          case ((v, cOld), (_, cNew)) if cNew < cOld => Some((v, cNew))
          case _ => None
        }
      }
//      s1.left preserves({ case (v, _) => v }, { case (v, _) => v })
      s1.right preserves({ v=>v }, { v=>v })

      (s1, s1)
    }

    val components = vertices.iterateWithDelta(vertices, { _._1 }, propagateComponent, maxIterations)
    val output = components.write(componentsOutput, DelimitedOutputFormat(formatOutput.tupled))

    val plan = new ScalaPlan(Seq(output), "Connected Components")
    plan
  }

  def parseVertex = (line: String) => { val v = line.toInt; v -> v }

  val EdgeInputPattern = """(\d+) (\d+)""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput = (vertex: Int, component: Int) => "%d %d".format(vertex, component)
}

