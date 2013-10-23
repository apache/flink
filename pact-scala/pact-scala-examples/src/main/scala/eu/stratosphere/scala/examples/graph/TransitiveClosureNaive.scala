package eu.stratosphere.scala.examples.graph;
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


import scala.math._
import scala.math.Ordered._
import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._
import eu.stratosphere.scala.DataStream
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.pact.client.LocalExecutor

object RunTransitiveClosureNaive {
  def main(pArgs: Array[String]) {
    if (pArgs.size < 3) {
      println("usage: [-numIterations <int:2>] -vertices <file> -edges <file> -output <file>")
      return
    }
    val args = Args.parse(pArgs)
    val plan = new TransitiveClosureNaive().getPlan(args("numIterations", "2").toInt, args("vertices"), args("edges"), args("output"))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class TransitiveClosureNaive extends Serializable {

  def getPlan(numIterations: Int, verticesInput: String, edgesInput: String, pathsOutput: String) = {
    val vertices = DataSource(verticesInput, DelimitedDataSourceFormat(parseVertex))
    val edges = DataSource(edgesInput, DelimitedDataSourceFormat(parseEdge))

    def createClosure(paths: DataStream[Path]) = {

      val allNewPaths = paths join edges where { p => p.to } isEqualTo { p => p.from } map joinPaths
      val shortestPaths = allNewPaths union paths groupBy { p => (p.from, p.to) } reduceGroup { _ minBy { _.dist } }

//      val delta = paths cogroup shortestPaths where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } flatMap { (oldPaths, newPaths) =>
//        (oldPaths.toSeq.headOption, newPaths.next) match {
//          case (Some(Path(_, _, oldDist)), Path(_, _, newDist)) if oldDist <= newDist => None
//          case (_, p) => Some(p)
//        }
//      }
//
//      (shortestPaths, delta)
      shortestPaths
    }

    val transitiveClosure = vertices.iterate(numIterations, createClosure)

    val output = transitiveClosure.write(pathsOutput, DelimitedDataSinkFormat(formatOutput))

    vertices.avgBytesPerRecord(16)
    edges.avgBytesPerRecord(16)
    new ScalaPlan(Seq(output), "Transitive Closure (Naive)")

  }

  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
    case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }


  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }

  val EdgeInputPattern = """(\d+)\|(\d+)\|""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)
}

