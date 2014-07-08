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
import scala.math._
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.api.common.Plan
import eu.stratosphere.api.java.record.operators.DeltaIteration

/**
 * An implementation of the PageRank algorithm for graph vertex ranking. Runs a specified fix number
 * of iterations. This version of page rank expects the edges to define a transition
 * probability and hence allows to model situations where not all outgoing links are equally probable.
 * 
 * <p>
 * 
 * Expects inputs are:
 *  1. Path to a file of node ids, as a sequence of Longs, line delimited.
 *  2. Path to a csv file of edges in the format <tt>sourceId targetId transitionProbability</tt> (fields separated by spaces).
 *    The ids are expected to be Longs, the transition probability a float or double.
 *  3. Path to where the output should be written
 *  4. The number of vertices
 *  5. The number of iterations
 */
class PageRankWithWeight extends Program with Serializable {

  def getScalaPlan(verticesPath: String, edgesPath: String, outputPath: String, numVertices: Long, maxIterations: Int) = {

    case class PageWithRank(pageId: Long, rank: Double)
    case class Edge(from: Long, to: Long, transitionProb: Double)

    val pages = DataSource(verticesPath, CsvInputFormat[Long]())
    val edges = DataSource(edgesPath, CsvInputFormat[Edge]("\n", ' ')) // line delimiter (\n), field delimiter (' ')

    val dampening = 0.85
    val randomJump = (1.0 - dampening) / numVertices
    val initialRank = 1.0 / numVertices

    val pagesWithRank = pages map { p => PageWithRank(p, initialRank) }

    def computeRank(ranks: DataSet[PageWithRank]) = {

      val ranksForNeighbors = ranks join edges where { _.pageId } isEqualTo { _.from } map { (p, e) => (e.to, p.rank * e.transitionProb) }

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
object RunPageRankWithWeight {

  def main(args: Array[String]) {
    if (args.size < 5) {
      println("PageRank <vertices> <edges> <result> <numVertices> <numIterations> [<parallelism=1>]")
      return
    }

    val dop = if (args.length > 5) args(5).toInt else 1
    val plan = new PageRankWithWeight().getScalaPlan(args(0), args(1), args(2), args(3).toLong, args(4).toInt);

    plan.setDefaultParallelism(dop)
    LocalExecutor.execute(plan)
  }
}

