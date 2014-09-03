/**
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


package org.apache.flink.examples.scala.wordcount

import org.apache.flink.client.LocalExecutor
import org.apache.flink.api.common.Program
import org.apache.flink.api.common.ProgramDescription

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.operators._


/**
 * Implementation of word count in Scala. This example uses the built in count function for tuples.
 */
class WordCountWithCount extends WordCount {

  override def getScalaPlan(numSubTasks: Int, textInput: String, wordsOutput: String) = {
    val input = TextFile(textInput)

    val words = input flatMap { _.toLowerCase().split("""\W+""") filter { _ != "" } }
    val counts = words groupBy { x => x } count()

    val output = counts.write(wordsOutput, CsvOutputFormat("\n", " "))
  
    val plan = new ScalaPlan(Seq(output), "Word Count")
    plan.setDefaultParallelism(numSubTasks)
    plan
  }
}


/**
 * Entry point to make the example standalone runnable with the local executor.
 */
object RunWordCountWithCount {
  def main(args: Array[String]) {
    val wc = new WordCountWithCount
    if (args.size < 3) {
      println(wc.getDescription)
      return
    }
    val plan = wc.getScalaPlan(args(0).toInt, args(1), args(2))
    LocalExecutor.execute(plan)
  }
}
