/**
 * *********************************************************************************************************************
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
 * ********************************************************************************************************************
 */

package eu.stratosphere.examples.scala.graph;

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription
import eu.stratosphere.api.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.api.common.operators.DeltaIteration
import scala.math._
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.Plan

class PageRank extends Program with Serializable {

  def getScalaPlan(verticesPath: String, edgesPath: String, outputPath: String, numVertices: Long, maxIterations: Int) = {

    case class PageWithRank(pageId: Long, rank: Double)
    case class Edge(from: Long, to: Long)
    case class Adjacency(vertex: Long, neighbors: List[Long])

    val pages = DataSource(verticesPath, CsvInputFormat[Long]())
    val edges = DataSource(edgesPath, CsvInputFormat[Edge]("\n", ' '))

    val dampening = 0.85
    val randomJump = (1.0 - dampening) / numVertices
    val initialRank = 1.0 / numVertices

    val pagesWithRank = pages map { p => PageWithRank(p, initialRank) }
    
    val adjacencies = edges.groupBy(_.from).reduceGroup(x => x.foldLeft(Adjacency(0, List[Long]()))((a, e) => Adjacency(e.from, e.to :: a.neighbors)));

    def computeRank(ranks: DataSet[PageWithRank]) = {

      val ranksForNeighbors = ranks join adjacencies where { _.pageId } isEqualTo { _.vertex } flatMap ( (p, e) => {
        val numNeighbors = e.neighbors.length
        
        for (target <- e.neighbors)
          yield (target, p.rank / numNeighbors)
          
      });

      ranksForNeighbors.groupBy { case (node, rank) => node }
        .reduce { (a, b) => (a._1, a._2 + b._2) }
        .map { case (node, rank) => PageWithRank(node, rank * dampening + randomJump) }
    }

    val finalRanks = pagesWithRank.iterate(maxIterations, computeRank)

    val output = finalRanks.write(outputPath, CsvOutputFormat())

    new ScalaPlan(Seq(output), "Connected Components")
  }

  override def getPlan(args: String*) = {
    val planArgs: Array[String] = if (args.length < 5) Array[String]("", "", "", "", "") else args.toArray
    val dop = if (args.size > 5) args(5).toInt else 1

    val plan = getScalaPlan(planArgs(0), planArgs(1), planArgs(2), planArgs(3).toLong, planArgs(4).toInt)
    plan.setDefaultParallelism(dop)
    plan
  }
}

/**
 * Executable entry point to run the program locally.
 */
object RunPageRank {

  def main(args: Array[String]) {
    if (args.size < 5) {
      println("PageRank <vertices> <edges> <result> <numVertices> <numIterations> [<parallelism=1>]")
      return
    }

    val dop = if (args.length > 5) args(5).toInt else 1
    val plan = new PageRank().getScalaPlan(args(0), args(1), args(2), args(3).toLong, args(4).toInt);

    plan.setDefaultParallelism(dop)
    LocalExecutor.execute(plan)
  }
}

