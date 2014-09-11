/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.examples.scala.graph

import scala.collection.JavaConverters._
import org.apache.flink.api.scala._
import org.apache.flink.example.java.graph.util.PageRankData
import org.apache.flink.util.Collector
import org.apache.flink.api.java.aggregation.Aggregations.SUM


/**
 * A basic implementation of the Page Rank algorithm using a bulk iteration.
 * 
 * <p>
 * This implementation requires a set of pages and a set of directed links as input and works as follows. <br> 
 * In each iteration, the rank of every page is evenly distributed to all pages it points to.
 * Each page collects the partial ranks of all pages that point to it, sums them up, and applies a dampening factor to the sum.
 * The result is the new rank of the page. A new iteration is started with the new ranks of all pages.
 * This implementation terminates after a fixed number of iterations.<br>
 * This is the Wikipedia entry for the <a href="http://en.wikipedia.org/wiki/Page_rank">Page Rank algorithm</a>. 
 * 
 * <p>
 * Input files are plain text files and must be formatted as follows:
 * <ul>
 * <li>Pages represented as an (long) ID separated by new-line characters.<br> 
 * For example <code>"1\n2\n12\n42\n63\n"</code> gives five pages with IDs 1, 2, 12, 42, and 63.
 * <li>Links are represented as pairs of page IDs which are separated by space 
 * characters. Links are separated by new-line characters.<br>
 * For example <code>"1 2\n2 12\n1 12\n42 63\n"</code> gives four (directed) links (1)->(2), (2)->(12), (1)->(12), and (42)->(63).<br>
 * For this simple implementation it is required that each page has at least one incoming and one outgoing link (a page can point to itself).
 * </ul>
 * 
 * <p>
 * Usage: <code>PageRankBasic &lt;pages path&gt; &lt;links path&gt; &lt;output path&gt; &lt;num pages&gt; &lt;num iterations&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link PageRankData} and 10 iterations.
 * 
 * <p>
 * This example shows how to use:
 * <ul>
 * <li>Bulk Iterations
 * <li>Default Join
 * <li>Configure user-defined functions using constructor parameters.
 * </ul> 
 *
 */
object PageRankBasic {
	
	private final val DAMPENING_FACTOR: Double = 0.85;
	private final val EPSILON: Double = 0.0001;

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
		val pagesWithRanks = pages.map(p => Page(p, (1.0/numPages)))
		
		// build adjacency list from link input
		val adjacencyLists = links
								// initialize lists
								.map( e => AdjacencyList(e.sourceId, Array[java.lang.Long](e.targetId) ))
								// concatenate lists
								.groupBy(0).reduce( (l1, l2) => AdjacencyList(l1.sourceId, l1.targetIds ++ l2.targetIds))
		
		// start iteration
		val finalRanks = pagesWithRanks.iterateWithTermination(maxIterations) {
			currentRanks => 
				val newRanks = currentRanks
								// distribute ranks to target pages
								.join(adjacencyLists).where(0).equalTo(0)
								.flatMap { x => for(targetId <- x._2.targetIds) yield Page(targetId, (x._1.rank / x._2.targetIds.length))}
								// collect ranks and sum them up
								.groupBy(0).aggregate(SUM, 1)
								// apply dampening factor
								.map { p => Page(p.pageId, (p.rank * DAMPENING_FACTOR) + ((1 - DAMPENING_FACTOR) / numPages) ) }
				
				// terminate if no rank update was significant
				val termination = currentRanks
									.join(newRanks).where(0).equalTo(0)
									// check for significant update
									.filter( x => math.abs(x._1.rank - x._2.rank) > EPSILON )
				
				(newRanks, termination)
		}
		
		val result = finalRanks;
								
		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", " ")
		} else {
			result.print()
		}
		
		// execute program
		env.execute("Basic PageRank Example")
	}
	
	// *************************************************************************
	//     USER TYPES
	// *************************************************************************

	case class Link(sourceId: Long, targetId: Long)
	case class Page(pageId: java.lang.Long, rank: Double)
	case class AdjacencyList(sourceId: java.lang.Long, targetIds: Array[java.lang.Long])
	
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
			} else {
				System.err.println("Usage: PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>");
				false
			}
		} else {
			System.out.println("Executing PageRank Basic example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>");
			
			numPages = PageRankData.getNumberOfPages();
		}
		true
	}

	private def getPagesDataSet(env: ExecutionEnvironment): DataSet[Long] = {
		if(fileOutput) {
			env.readCsvFile[Tuple1[Long]](pagesInputPath, fieldDelimiter = ' ', lineDelimiter = "\n")
				.map(x => x._1)
		} else {
			env.fromCollection(Seq.range(1, PageRankData.getNumberOfPages()+1))
		}
	}
	
	private def getLinksDataSet(env: ExecutionEnvironment): DataSet[Link] = {
		if (fileOutput) {
			env.readCsvFile[(Long, Long)](linksInputPath, fieldDelimiter = ' ', includedFields = Array(0, 1))
				.map { x => Link(x._1, x._2) }
		} else {
			val edges = PageRankData.EDGES.map{ case Array(v1, v2) => Link(v1.asInstanceOf[Long], v2.asInstanceOf[Long]) }
			env.fromCollection(edges)
		}
	}	
	
	private var fileOutput: Boolean = false
	private var pagesInputPath: String = null
	private var linksInputPath: String = null
	private var outputPath: String = null
	private var numPages: Long = 0;
	private var maxIterations: Int = 10;

}