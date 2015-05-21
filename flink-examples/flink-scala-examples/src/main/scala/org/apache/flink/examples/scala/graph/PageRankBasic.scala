/*
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
package org.apache.flink.examples.scala.graph

import java.lang.Iterable

import org.apache.flink.api.common.functions.GroupReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.graph.util.PageRankData
import org.apache.flink.api.java.aggregation.Aggregations.SUM

import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

/**
 * A basic implementation of the Page Rank algorithm using a bulk iteration.
 * 
 * This implementation requires a set of pages and a set of directed links as input and works as
 * follows.
 *
 * In each iteration, the rank of every page is evenly distributed to all pages it points to. Each
 * page collects the partial ranks of all pages that point to it, sums them up, and applies a
 * dampening factor to the sum. The result is the new rank of the page. A new iteration is started
 * with the new ranks of all pages. This implementation terminates after a fixed number of
 * iterations. This is the Wikipedia entry for the
 * [[http://en.wikipedia.org/wiki/Page_rank Page Rank algorithm]]
 * 
 * Input files are plain text files and must be formatted as follows:
 *
 *  - Pages represented as an (long) ID separated by new-line characters.
 *    For example `"1\n2\n12\n42\n63\n"` gives five pages with IDs 1, 2, 12, 42, and 63.
 *  - Links are represented as pairs of page IDs which are separated by space  characters. Links
 *    are separated by new-line characters.
 *    For example `"1 2\n2 12\n1 12\n42 63\n"` gives four (directed) links (1)->(2), (2)->(12),
 *    (1)->(12), and (42)->(63). For this simple implementation it is required that each page has
 *    at least one incoming and one outgoing link (a page can point to itself).
 *
 * Usage:
 * {{{
 *   PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.graph.util.PageRankData]] and 10 iterations.
 * 
 * This example shows how to use:
 *
 *  - Bulk Iterations
 *  - Default Join
 *  - Configure user-defined functions using constructor parameters.
 *
 */
object PageRankBasic {

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val EPSILON: Double = 0.0001

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // read input data
    val pages = getPagesDataSet(env)
    val links = getLinksDataSet(env)

    // assign initial ranks to pages
    val pagesWithRanks = pages.map(p => Page(p, 1.0 / numPages)).withForwardedFields("*->pageId")

    // build adjacency list from link input
    val adjacencyLists = links
      .groupBy("sourceId").reduceGroup( new GroupReduceFunction[Link, AdjacencyList] {
        override def reduce(values: Iterable[Link], out: Collector[AdjacencyList]): Unit = {
          var outputId = -1L
          val outputList = values.asScala map { t => outputId = t.sourceId; t.targetId }
          out.collect(new AdjacencyList(outputId, outputList.toArray))
        }
      })

    // start iteration
    val finalRanks = pagesWithRanks.iterateWithTermination(maxIterations) {
      currentRanks =>
        val newRanks = currentRanks
          // distribute ranks to target pages
          .join(adjacencyLists).where("pageId").equalTo("sourceId") {
            (page, adjacent, out: Collector[Page]) =>
              val targets = adjacent.targetIds
              val len = targets.length
              adjacent.targetIds foreach { t => out.collect(Page(t, page.rank /len )) }
          }
          // collect ranks and sum them up
          .groupBy("pageId").aggregate(SUM, "rank")
          // apply dampening factor
          .map { p =>
            Page(p.pageId, (p.rank * DAMPENING_FACTOR) + ((1 - DAMPENING_FACTOR) / numPages))
          }.withForwardedFields("pageId")

        // terminate if no rank update was significant
        val termination = currentRanks.join(newRanks).where("pageId").equalTo("pageId") {
          (current, next, out: Collector[Int]) =>
            // check for significant update
            if (math.abs(current.rank - next.rank) > EPSILON) out.collect(1)
        }
        (newRanks, termination)
    }

    val result = finalRanks

    // emit result
    if (fileOutput) {
      result.writeAsCsv(outputPath, "\n", " ")
      // execute program
      env.execute("Basic PageRank Example")
    } else {
      result.print()
    }
  }

  // *************************************************************************
  //     USER TYPES
  // *************************************************************************

  case class Link(sourceId: Long, targetId: Long)

  case class Page(pageId: Long, rank: Double)

  case class AdjacencyList(sourceId: Long, targetIds: Array[Long])

  // *************************************************************************
  //     UTIL METHODS
  // *************************************************************************

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 5) {
        pagesInputPath = args(0)
        linksInputPath = args(1)
        outputPath = args(2)
        numPages = args(3).toLong
        maxIterations = args(4).toInt

        true
      } else {
        System.err.println("Usage: PageRankBasic <pages path> <links path> <output path> <num " +
          "pages> <num iterations>")

        false
      }
    } else {
      System.out.println("Executing PageRank Basic example with default parameters and built-in " +
        "default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  Usage: PageRankBasic <pages path> <links path> <output path> <num " +
        "pages> <num iterations>")

      numPages = PageRankData.getNumberOfPages

      true
    }
  }

  private def getPagesDataSet(env: ExecutionEnvironment): DataSet[Long] = {
    if (fileOutput) {
      env.readCsvFile[Tuple1[Long]](pagesInputPath, fieldDelimiter = " ", lineDelimiter = "\n")
        .map(x => x._1)
    } else {
      env.generateSequence(1, 15)
    }
  }

  private def getLinksDataSet(env: ExecutionEnvironment): DataSet[Link] = {
    if (fileOutput) {
      env.readCsvFile[Link](linksInputPath, fieldDelimiter = " ",
        includedFields = Array(0, 1))
    } else {
      val edges = PageRankData.EDGES.map { case Array(v1, v2) => Link(v1.asInstanceOf[Long],
        v2.asInstanceOf[Long])}
      env.fromCollection(edges)
    }
  }

  private var fileOutput: Boolean = false
  private var pagesInputPath: String = null
  private var linksInputPath: String = null
  private var outputPath: String = null
  private var numPages: Long = 0
  private var maxIterations: Int = 10

}
