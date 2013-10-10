package eu.stratosphere.scala.examples.graph;
///**
// * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
// *
// * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
// * the License. You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
// * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
// * specific language governing permissions and limitations under the License.
// */
//
//package eu.stratosphere.pact4s.examples.graph
//
//import scala.math._
//
//import eu.stratosphere.pact4s.common._
//import eu.stratosphere.pact4s.common.operators._
//
//class ConnectedComponentsDescriptor extends PactDescriptor[ConnectedComponents] {
//  override val name = "Connected Components"
//  override val parameters = "-vertices <file> -edges <file> -output <file>"
//
//  override def createInstance(args: Pact4sArgs) = new ConnectedComponents(args("vertices"), args("edges"), args("output"))
//}
//
//class ConnectedComponents(verticesInput: String, edgesInput: String, componentsOutput: String) extends PactProgram {
//
//  val vertices = new DataSource(verticesInput, DelimetedDataSourceFormat(parseVertex))
//  val directedEdges = new DataSource(edgesInput, DelimetedDataSourceFormat(parseEdge))
//  val output = new DataSink(componentsOutput, DelimetedDataSinkFormat(formatOutput.tupled))
//
//  val undirectedEdges = directedEdges flatMap { case (from, to) => Seq(from -> to, to -> from) }
//  val components = propagateComponent iterate (s0 = vertices distinctBy { _._1 }, ws0 = vertices)
//
//  override def outputs = output <~ components
//
//  def propagateComponent = (s: DataStream[(Int, Int)], ws: DataStream[(Int, Int)]) => {
//
//    val allNeighbors = ws join undirectedEdges on { case (v, _) => v } isEqualTo { case (from, _) => from } map { (w, e) => e._2 -> w._2 }
//    val minNeighbors = allNeighbors groupBy { case (to, _) => to } combine { cs => cs minBy { _._2 } }
//
//    // updated solution elements == new workset
//    val s1 = minNeighbors join s on { _._1 } isEqualTo { _._1 } flatMap { (n, s) =>
//      (n, s) match {
//        case ((v, cNew), (_, cOld)) if cNew < cOld => Some((v, cNew))
//        case _                                     => None
//      }
//    }
//
//    (s1, s1)
//  }
//
//  vertices.avgBytesPerRecord(8)
//  directedEdges.avgBytesPerRecord(8)
//  undirectedEdges.avgBytesPerRecord(8).avgRecordsEmittedPerCall(2)
//  output.avgBytesPerRecord(8)
//
//  def parseVertex = (line: String) => { val v = line.toInt; v -> v }
//
//  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r
//
//  def parseEdge = (line: String) => line match {
//    case EdgeInputPattern(from, to) => from.toInt -> to.toInt
//  }
//
//  def formatOutput = (vertex: Int, component: Int) => "%d|%d".format(vertex, component)
//}
//
