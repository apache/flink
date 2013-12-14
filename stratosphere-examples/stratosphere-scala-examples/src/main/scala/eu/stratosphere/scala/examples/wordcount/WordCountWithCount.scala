/**
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package eu.stratosphere.scala.examples.wordcount

import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.api.plan.PlanAssembler
import eu.stratosphere.api.plan.PlanAssemblerDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

object RunWordCountWithCount {
  def main(args: Array[String]) {
    val wc = new WordCountWithCount
    if (args.size < 3) {
      println(wc.getDescription)
      return
    }
    val plan = wc.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class WordCountWithCount extends WordCount {

  override def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } }
    val counts = words groupBy { x => x } count()

    val output = counts.write(wordsOutput, CsvOutputFormat[(String, Int)]("\n", " "))
  
    val plan = new ScalaPlan(Seq(output), "Word Count")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
