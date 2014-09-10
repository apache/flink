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

import org.apache.flink.api.scala._
import org.apache.flink.example.java.graph.util.ConnectedComponentsData
import org.apache.flink.util.Collector

object TransitiveClosureNaive {


	def main (args: Array[String]): Unit = {
		if (!parseParameters(args)) {
			return
		}

		val env = ExecutionEnvironment.getExecutionEnvironment

		val edges = getEdgesDataSet(env)

		val paths = edges.iterateWithTermination(maxIterations) { prevPaths: DataSet[(Long,Long)] =>

			val nextPaths = prevPaths
				.join(edges)
				.where(1).equalTo(0) {
					(left,right) => Some((left._1,right._2))
				}
				.union(prevPaths)
				.groupBy(0,1)
				.reduce((l,r) => l)

			val terminate = prevPaths
				.coGroup(nextPaths)
				.where(0).equalTo(0) {
					(prev, next, out: Collector[(Long, Long)]) => {
						val prevPaths = prev.toList
						for (n <- next)
							if (!prevPaths.contains(n))
								out.collect(n)
					}
			}
			(nextPaths, terminate)
		}

		if (fileOutput)
			paths.writeAsCsv(outputPath, "\n", " ")
		else
			paths.print()

		env.execute("Scala Transitive Closure Example")


	}


	private var fileOutput: Boolean = false
	private var edgesPath: String = null
	private var outputPath: String = null
	private var maxIterations: Int = 10

	private def parseParameters(programArguments: Array[String]): Boolean = {
		if (programArguments.length > 0) {
			fileOutput = true
			if (programArguments.length == 3) {
				edgesPath = programArguments(0)
				outputPath = programArguments(1)
				maxIterations = Integer.parseInt(programArguments(2))
			}
			else {
				System.err.println("Usage: TransitiveClosure <edges path> <result path> <max number of iterations>")
				return false
			}
		}
		else {
			System.out.println("Executing TransitiveClosure example with default parameters and built-in default data.")
			System.out.println("  Provide parameters to read input data from files.")
			System.out.println("  See the documentation for the correct format of input files.")
			System.out.println("  Usage: TransitiveClosure <edges path> <result path> <max number of iterations>")
		}
		return true
	}

	private def getEdgesDataSet(env: ExecutionEnvironment): DataSet[(Long, Long)] = {
		if (fileOutput) {
			env.readCsvFile[(Long, Long)](
				edgesPath,
				fieldDelimiter = ' ',
				includedFields = Array(0, 1))
				.map { x => (x._1, x._2)}
		}
		else {
			val edgeData = ConnectedComponentsData.EDGES map {
				case Array(x, y) => (x.asInstanceOf[Long], y.asInstanceOf[Long])
			}
			env.fromCollection(edgeData)
		}
	}
}






///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//
//package org.apache.flink.examples.scala.graph;
//
//import org.apache.flink.client.LocalExecutor
//import org.apache.flink.api.common.Program
//import org.apache.flink.api.common.ProgramDescription
//
//import org.apache.flink.api.scala._
//import org.apache.flink.api.scala.operators._
//
//class TransitiveClosureNaive extends Program with ProgramDescription with Serializable {
//
//  def getScalaPlan(numSubTasks: Int, numIterations: Int, verticesInput: String, edgesInput: String, pathsOutput: String) = {
//    val vertices = DataSource(verticesInput, DelimitedInputFormat(parseVertex))
//    val edges = DataSource(edgesInput, DelimitedInputFormat(parseEdge))
//
//    def createClosure(paths: DataSetOLD[Path]) = {
//
//      val allNewPaths = paths join edges where { p => p.to } isEqualTo { e => e.from } map joinPaths
//      val shortestPaths = allNewPaths union paths groupBy { p => (p.from, p.to) } reduceGroup { _ minBy { _.dist } }
//
//      shortestPaths
//    }
//
//    val transitiveClosure = vertices.iterate(numIterations, createClosure)
//
//    val output = transitiveClosure.write(pathsOutput, DelimitedOutputFormat(formatOutput))
//
//    val plan = new ScalaPlan(Seq(output), "Transitive Closure (Naive)")
//    plan.setDefaultParallelism(numSubTasks)
//    plan
//  }
//
//  def joinPaths = (p1: Path, p2: Path) => (p1, p2) match {
//      case (Path(from, _, dist1), Path(_, to, dist2)) => Path(from, to, dist1 + dist2)
//  }
//
//  case class Path(from: Int, to: Int, dist: Int)
//
//  def parseVertex = (line: String) => { val v = line.toInt; Path(v, v, 0) }
//
//  val EdgeInputPattern = """(\d+)\|(\d+)""".r
//
//  def parseEdge = (line: String) => line match {
//    case EdgeInputPattern(from, to) => Path(from.toInt, to.toInt, 1)
//  }
//
//  def formatOutput = (path: Path) => "%d|%d|%d".format(path.from, path.to, path.dist)
//
//  override def getDescription() = {
//    "Parameters: <numSubStasks> <numIterations> <vertices> <edges> <output>"
//  }
//
//  override def getPlan(args: String*) = {
//    getScalaPlan(args(0).toInt, args(1).toInt, args(2), args(3), args(4))
//  }
//}
//
//object RunTransitiveClosureNaive {
//  def main(pArgs: Array[String]) {
//    if (pArgs.size < 3) {
//      println("usage: [-numIterations <int:2>] -vertices <file> -edges <file> -output <file>")
//      return
//    }
//    val args = Args.parse(pArgs)
//    val plan = new TransitiveClosureNaive().getScalaPlan(2, args("numIterations", "10").toInt, args("vertices"), args("edges"), args("output"))
//    LocalExecutor.execute(plan)
//  }
//}