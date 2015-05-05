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

import org.apache.flink.api.scala._
import org.apache.flink.examples.java.graph.util.ConnectedComponentsData
import org.apache.flink.util.Collector

object  TransitiveClosureNaive {

  def main (args: Array[String]): Unit = {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment

    val edges = getEdgesDataSet(env)

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

    if (fileOutput) {
      paths.writeAsCsv(outputPath, "\n", " ")
      env.execute("Scala Transitive Closure Example")
    } else {
      paths.print()
    }



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
        System.err.println("Usage: TransitiveClosure <edges path> <result path> <max number of " +
          "iterations>")
        return false
      }
    }
    else {
      System.out.println("Executing TransitiveClosure example with default parameters and " +
        "built-in default data.")
      System.out.println("  Provide parameters to read input data from files.")
      System.out.println("  See the documentation for the correct format of input files.")
      System.out.println("  Usage: TransitiveClosure <edges path> <result path> <max number of " +
        "iterations>")
    }
    true
  }

  private def getEdgesDataSet(env: ExecutionEnvironment): DataSet[(Long, Long)] = {
    if (fileOutput) {
      env.readCsvFile[(Long, Long)](
        edgesPath,
        fieldDelimiter = " ",
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
