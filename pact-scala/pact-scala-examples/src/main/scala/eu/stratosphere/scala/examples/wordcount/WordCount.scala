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

import scala.Array.canBuildFrom
import eu.stratosphere.pact.client.LocalExecutor
import eu.stratosphere.pact.common.`type`.base.PactInteger
import eu.stratosphere.pact.common.`type`.base.PactString
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.ScalaPlanAssembler
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.scala.operators.arrayToIterator
import eu.stratosphere.scala.operators.DelimitedDataSourceFormat
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat

object RunWordCount {
  def main(args: Array[String]) {
    val plan = WordCount.getPlan(
      "file:///home/aljoscha/dummy-input",
      "file:///home/aljoscha/wordcount-output")

    GlobalSchemaPrinter.printSchema(plan)
    LocalExecutor.execute(plan)

    System.exit(0)
  }
}

class WordCountDescriptor extends ScalaPlanAssembler with PlanAssemblerDescription {
  override def getDescription = "-input <file>  -output <file>"

  override def getScalaPlan(args: Args) = WordCount.getPlan(args("input"), args("output"))
}

object WordCount {
  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getPlan(textInput: String, wordsOutput: String) = {
    val input = DataSource(textInput, DelimitedDataSourceFormat(identity[String] _))

    val words = input flatMap { _.toLowerCase().split("""\W+""") map { (_, 1) } }
    val counts = words groupBy { case (word, _) => word } reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    counts neglects { case (word, _) => word }
    counts preserves({ case (word, _) => word }, { case (word, _) => word })
    val output = counts.write(wordsOutput, DelimitedDataSinkFormat(formatOutput.tupled))
  
    new ScalaPlan(Seq(output), "Word Count (immutable)")
  }
}
