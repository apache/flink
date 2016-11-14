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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData
import org.apache.flink.util.Collector

object TransitiveClosureNaive {

  def main (args: Array[String]): Unit = {

    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val edges =
      if (params.has("edges")) {
        env.readCsvFile[(Long, Long)](
          filePath = params.get("edges"),
          fieldDelimiter = " ",
          includedFields = Array(0, 1))
          .map { x => (x._1, x._2)}
      } else {
        println("Executing TransitiveClosure example with default edges data set.")
        println("Use --edges to specify file input.")
        val edgeData = ConnectedComponentsData.EDGES map {
          case Array(x, y) => (x.asInstanceOf[Long], y.asInstanceOf[Long])
        }
        env.fromCollection(edgeData)
      }

    val maxIterations = params.getInt("iterations", 10)

    val paths = edges.iterateWithTermination(maxIterations) { prevPaths: DataSet[(Long, Long)] =>

      val nextPaths = prevPaths
        .join(edges)
        .where(1).equalTo(0) {
          (left, right) => (left._1,right._2)
        }.withForwardedFieldsFirst("_1").withForwardedFieldsSecond("_2")
        .union(prevPaths)
        .groupBy(0, 1)
        .reduce((l, r) => l).withForwardedFields("_1; _2")

      val terminate = prevPaths
        .coGroup(nextPaths)
        .where(0).equalTo(0) {
          (prev, next, out: Collector[(Long, Long)]) => {
            val prevPaths = prev.toSet
            for (n <- next)
              if (!prevPaths.contains(n)) out.collect(n)
          }
      }.withForwardedFieldsSecond("*")
      (nextPaths, terminate)
    }

    if (params.has("output")) {
      paths.writeAsCsv(params.get("output"), "\n", " ")
      env.execute("Scala Transitive Closure Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      paths.print()
    }

  }
}
