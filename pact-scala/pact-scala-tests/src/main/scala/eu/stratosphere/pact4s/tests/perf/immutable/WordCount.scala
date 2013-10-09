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

package eu.stratosphere.pact4s.tests.perf.immutable

import eu.stratosphere.pact4s.common._
import eu.stratosphere.pact4s.common.operators._

class WordCountDescriptor extends PactDescriptor[WordCount] {
  override val name = "Word Count (Immutable)"
  override val parameters = "-input <file> -output <file>"

  override def createInstance(args: Pact4sArgs) = new WordCount(args("input"), args("output"))
}

class WordCount(textInput: String, wordsOutput: String) extends PactProgram {

  import WordCount._
  
  val input = new DataSource(textInput, new TextDataSourceFormat("ASCII"))
  val output = new DataSink(wordsOutput, new RecordDataSinkFormat[WordWithCount]("\n", " ", true))

  val words = input flatMap { _.toLowerCase().split("\\W+") map { WordWithCount(_, 1) } }
  
  val counts = words groupBy { _.word } combine { wcs =>
    
    var wc: WordWithCount = null
    var count = 0
    
    while (wcs.hasNext) {
      wc = wcs.next
      count += wc.count
    }
    
    WordWithCount(wc.word, count)
  }

  override def outputs = output <~ counts

  counts neglects { wc => wc.word }
  counts preserves { wc => wc.word } as { wc => wc.word }
}

object WordCount {
  case class WordWithCount(val word: String, val count: Int)
}
