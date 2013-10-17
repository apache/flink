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
import eu.stratosphere.scala.Args
import eu.stratosphere.scala.DataSource
import eu.stratosphere.scala.ScalaPlan
import eu.stratosphere.scala.analysis.GlobalSchemaPrinter
import eu.stratosphere.scala.operators.arrayToIterator
import eu.stratosphere.scala.operators.DelimitedDataSourceFormat
import eu.stratosphere.scala.operators.DelimitedDataSinkFormat
import eu.stratosphere.scala.TextFile

object RunWordCount {
  def main(pArgs: Array[String]) {
    if (pArgs.size < 3) {
      println("usage: -input <file>  -output <file>")
      return
    }
    val args = Args.parse(pArgs)
    val plan = new WordCount().getPlan(args("input"), args("output"))
    LocalExecutor.execute(plan)
    System.exit(0)
  }
}

class WordCount extends Serializable {

  def formatOutput = (word: String, count: Int) => "%s %d".format(word, count)

  def getPlan(textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } map { (_, 1) } }
    val counts = words groupBy { case (word, _) => word } reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    counts neglects { case (word, _) => word }
    counts preserves({ case (word, _) => word }, { case (word, _) => word })
    val output = counts.write(wordsOutput, DelimitedDataSinkFormat(formatOutput.tupled))
  
    new ScalaPlan(Seq(output), "Word Count (immutable)")
  }
}
