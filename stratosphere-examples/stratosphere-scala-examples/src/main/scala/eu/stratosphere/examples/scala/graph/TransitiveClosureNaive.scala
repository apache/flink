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

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

class TransitiveClosureNaive extends Program with ProgramDescription with Serializable {

  def getScalaPlan(numSubTasks: Int, numIterations: Int, verticesInput: String, edgesInput: String, pathsOutput: String) = {
    val vertices = DataSource(verticesInput, DelimitedInputFormat(parseVertex))
    val edges = DataSource(edgesInput, DelimitedInputFormat(parseEdge))

    def createClosure(paths: DataSet[Path]) = {

      val allNewPaths = paths join edges where { p => p.to } isEqualTo { e => e.from } map joinPaths
      val shortestPaths = allNewPaths union paths groupBy { p => (p.from, p.to) } reduceGroup { _ minBy { _.dist } }

      shortestPaths
    }

    val transitiveClosure = vertices.iterate(numIterations, createClosure)

    val output = transitiveClosure.write(pathsOutput, DelimitedOutputFormat(formatOutput))

    val plan = new ScalaPlan(Seq(output), "Transitive Closure (Naive)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }

  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
      case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
  }

  case class Path(from: Int, to: Int, dist: Int)

  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }

  val EdgeInputPattern = """(\d+)\|(\d+)""".r

  def parseEdge = (line: String) => line match {
    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
  }

  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)

  override def getDescription() = {
    "Parameters: <numSubStasks> <numIterations> <vertices> <edges> <output>"
  }

  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1).toInt, args(2), args(3), args(4))
  }
}

object RunTransitiveClosureNaive {
  def main(pArgs: Array[String]) {
    if (pArgs.size < 3) {
      println("usage: [-numIterations <int:2>] -vertices <file> -edges <file> -output <file>")
      return
    }
    val args = Args.parse(pArgs)
    val plan = new TransitiveClosureNaive().getScalaPlan(2, args("numIterations", "10").toInt, args("vertices"), args("edges"), args("output"))
    LocalExecutor.execute(plan)
  }
}