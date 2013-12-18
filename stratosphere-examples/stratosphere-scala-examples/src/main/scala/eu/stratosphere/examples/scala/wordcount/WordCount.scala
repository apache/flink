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

package eu.stratosphere.examples.scala.wordcount

import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.Program
import eu.stratosphere.api.ProgramDescription

import eu.stratosphere.scala._
import eu.stratosphere.scala.operators._

object RunWordCount {
  def main(args: Array[String]) {
    val wc = new WordCount
    if (args.size < 3) {
      println(wc.getDescription)
      return
    }
    val plan = wc.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
  }
}

class WordCount extends Program with ProgramDescription with Serializable {
  override def getDescription() = {
    "Parameters: [numSubStasks] [input] [output]"
  }
  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }

  def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }
    val counts = words groupBy { case (word, _) => word } reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    val output = counts.write(wordsOutput, CsvOutputFormat("\n", " "));
  
    val plan = new ScalaPlan(Seq(output), "Word Count (immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}
