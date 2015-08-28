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
import org.apache.flink.util.Collector
import org.apache.flink.core.fs.FileSystem.WriteMode

object DeltaPageRank {

  private final val DAMPENING_FACTOR: Double = 0.85
  private final val NUM_VERTICES = 5
  private final val INITIAL_RANK = 1.0 / NUM_VERTICES
  private final val RANDOM_JUMP = (1 - DAMPENING_FACTOR) / NUM_VERTICES
  private final val THRESHOLD = 0.0001 / NUM_VERTICES

  type Page = (Long, Double)
  type Adjacency = (Long, Array[Long])

  def main(args: Array[String]) {

    val maxIterations = 100

    val env = ExecutionEnvironment.getExecutionEnvironment

    val rawLines: DataSet[String] = env.fromElements(
                                                      "1 2 3 4",
                                                      "2 1",
                                                      "3 5",
                                                      "4 2 3",
                                                      "5 2 4")
    val adjacency: DataSet[Adjacency] = rawLines
      .map(str => {
        val elements = str.split(' ')
        val id = elements(0).toLong
        val neighbors = elements.slice(1, elements.length).map(_.toLong)
        (id, neighbors)
      })

    val initialRanks: DataSet[Page] = adjacency.flatMap {
      (adj, out: Collector[Page]) =>
        {
          val targets = adj._2
          val rankPerTarget = INITIAL_RANK * DAMPENING_FACTOR / targets.length

          // dampend fraction to targets
          for (target <- targets) {
            out.collect((target, rankPerTarget))
          }

          // random jump to self
          out.collect((adj._1, RANDOM_JUMP))
        }
    }
      .groupBy(0).sum(1)

    val initialDeltas = initialRanks.map { (page) => (page._1, page._2 - INITIAL_RANK) }
                                      .withForwardedFields("_1")

    val iteration = initialRanks.iterateDelta(initialDeltas, maxIterations, Array(0)) {

      (solutionSet, workset) =>
        {
          val deltas = workset.join(adjacency).where(0).equalTo(0) {
            (lastDeltas, adj, out: Collector[Page]) =>
              {
                val targets = adj._2
                val deltaPerTarget = DAMPENING_FACTOR * lastDeltas._2 / targets.length

                for (target <- targets) {
                  out.collect((target, deltaPerTarget))
                }
              }
          }
            .groupBy(0).sum(1)
            .filter(x => Math.abs(x._2) > THRESHOLD)

          val rankUpdates = solutionSet.join(deltas).where(0).equalTo(0) {
            (current, delta) => (current._1, current._2 + delta._2)
          }.withForwardedFieldsFirst("_1")

          (rankUpdates, deltas)
        }
    }

    iteration.print()

  }
}
