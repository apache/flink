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

package eu.stratosphere.examples.scala.iterative

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._

/**
 * Example of using the bulk iteration with termination criterion with the
 * scala api.
 */
class TerminationCriterion extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: <maxNumberIterations> <output>"
  }

  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1))
  }

  def getScalaPlan(maxNumberIterations: Int, resultOutput: String) = {
    val dataSource = CollectionDataSource[Double](List(1.0))

    val halve = (partialSolution: DataSet[Double]) => {
      partialSolution map { x => x /2 }
    }

    val terminationCriterion = (prev: DataSet[Double], cur: DataSet[Double]) => {
      val diff = prev cross cur map { (valuePrev, valueCurrent) => math.abs(valuePrev - valueCurrent) }
      diff filter {
        difference => difference > 0.1
      }
    }

    val iteration = dataSource.iterateWithTermination(maxNumberIterations, halve, terminationCriterion)


    val sink = iteration.write(resultOutput, CsvOutputFormat())

    val plan = new ScalaPlan(Seq(sink))
    plan.setDefaultParallelism(1)
    plan
  }
}

object RunTerminationCriterion {
  def main(args: Array[String]) {
    val tc = new TerminationCriterion

    if(args.size < 2) {
      println(tc.getDescription())
      return
    }
    val plan = tc.getScalaPlan(args(0).toInt, args(1))
    LocalExecutor.execute(plan)
  }
}
