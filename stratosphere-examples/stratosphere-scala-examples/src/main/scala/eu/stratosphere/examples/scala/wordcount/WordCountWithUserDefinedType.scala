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
import eu.stratosphere.api.common.Program
import eu.stratosphere.api.common.ProgramDescription

import eu.stratosphere.types.IntValue
import eu.stratosphere.types.StringValue

import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._


/**
 * Implementation of word count in Scala, using a user defined type rather than one of the
 * built-in supported types like primitives, tuples, or other (nested) case classes.
 */
class WordCountWithUserDefinedType extends Program with Serializable {

  def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { w => (new StringValue(w), new IntValue(1)) } }
    
    val counts = words
      .groupBy { case (word, _) => word }
      .reduce { (w1, w2) => (w1._1, new IntValue(w1._2.getValue + w2._2.getValue)) }

    val output = counts.write(wordsOutput, CsvOutputFormat("\n", " "))
  
    val plan = new ScalaPlan(Seq(output), "Word Count (immutable)")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
  

  override def getPlan(args: String*) = {
    getScalaPlan(args(0).toInt, args(1), args(2))
  }
}
