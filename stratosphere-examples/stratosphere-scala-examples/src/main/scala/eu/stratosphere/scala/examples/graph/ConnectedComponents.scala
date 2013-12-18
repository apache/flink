package eu.stratosphere.scala.examples.graph;
/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/


import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.Program
import eu.stratosphere.api.ProgramDescription
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.api.operators.WorksetIteration

import scala.math._

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

object RunConnectedComponents {
 def main(pArgs: Array[String]) {
    if (pArgs.size < 3) {
      println("usage: -vertices <file> -edges <file> -output <file>")
      return
    }
    val args = Args.parse(pArgs)
    val plan = new ConnectedComponents().getPlan(args("vertices"), args("edges"), args("output"))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class ConnectedComponents extends Serializable {
  
  def getPlan(verticesInput: String, edgesInput: String, componentsOutput: String, maxIterations: Int = 10) = {

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

    val components = vertices.iterateWithWorkset(vertices, { _._1 }, propagateComponent, maxIterations)
    val output = components.write(componentsOutput, DelimitedOutputFormat(formatOutput.tupled))

    vertices.avgBytesPerRecord(8)
    directedEdges.avgBytesPerRecord(8)
    undirectedEdges.avgBytesPerRecord(8).avgRecordsEmittedPerCall(2)

    val plan = new ScalaPlan(Seq(output), "Connected Components")
    GlobalSchemaPrinter.printSchema(plan)
    plan
  }

  def parseVertex = (line: String) => { val v = line.toInt; v -> v }

  val EdgeInputPattern = """(\d+) (\d+)""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
  }

  def formatOutput = (vertex: Int, component: Int) => "%d %d".format(vertex, component)
}

