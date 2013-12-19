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

package eu.stratosphere.examples.scala.graph;

import scala.math._
import scala.math.Ordered._
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

import eu.stratosphere.client.LocalExecutor

object RunTransitiveClosureRD {
  def main(pArgs: Array[String]) {
    if (pArgs.size < 3) {
      println("usage: -vertices <file> -edges <file> -output <file>")
      return
    }
    val args = Args.parse(pArgs)
    val plan = new TransitiveClosureRD().getPlan(args("vertices"), args("edges"), args("output"))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class TransitiveClosureRD extends Serializable {

  def getPlan(verticesInput: String, edgesInput: String, pathsOutput: String) = {
    val vertices = DataSource(verticesInput, DelimitedInputFormat(parseVertex))
    val edges = DataSource(edgesInput, DelimitedInputFormat(parseEdge))

    def createClosure = (c: DataSet[Path], x: DataSet[Path]) => {

      val cNewPaths = x join c where { p => p.to } isEqualTo { p => p.from } map joinPaths
      val c1 = cNewPaths cogroup c where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } map selectShortestDistance

      val xNewPaths = x join x where { p => p.to } isEqualTo { p => p.from } map joinPaths
      val x1 = xNewPaths cogroup c1 where { p => (p.from, p.to) } isEqualTo { p => (p.from, p.to) } flatMap excludeKnownPaths

      (c1, x1)
    }
    val transitiveClosure = vertices.iterateWithWorkset(edges, { p => (p.from, p.to) }, createClosure, 10)

    val output = transitiveClosure.write(pathsOutput, DelimitedOutputFormat(formatOutput))

    vertices.avgBytesPerRecord(16)
    edges.avgBytesPerRecord(16)

    new ScalaPlan(Seq(output), "Transitive Closure with Recursive Doubling")
  }

  def selectShortestDistance = (dist1: Iterator[Path], dist2: Iterator[Path]) => (dist1 ++ dist2) minBy { _.dist }

  def excludeKnownPaths = (x: Iterator[Path], c: Iterator[Path]) => if (c.isEmpty) x else Iterator.empty

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

