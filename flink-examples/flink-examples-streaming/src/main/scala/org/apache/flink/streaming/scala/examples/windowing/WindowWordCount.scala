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

package org.apache.flink.streaming.scala.examples.windowing

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.wordcount.util.WordCountData
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * Implements a windowed version of the streaming "WordCount" program.
  *
  * <p>
  * The input is a plain text file with lines separated by newline characters.
  *
  * <p>
  * Usage: <code>WordCount
  * --input &lt;path&gt;
  * --output &lt;path&gt;
  * --window &lt;n&gt;
  * --slide &lt;n&gt;
  * </code><br>
  * If no parameters are provided, the program is run with default data from
  * {@link org.apache.flink.examples.java.wordcount.util.WordCountData}.
  *
  * <p>
  * This example shows how to:
  * <ul>
  * <li>write a simple Flink Streaming program,
  * <li>use tuple data types,
  * <li>use basic windowing abstractions.
  * </ul>
  *
  */
object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data
    val text =
    if (params.has("input")) {
      // read the text file from given input path
      env.readTextFile(params.get("input"))
    } else {
      println("Executing WindowWordCount example with default input data set.")
      println("Use --input to specify file input.")
      // get default test text data
      env.fromElements(WordCountData.WORDS: _*)
    }

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val windowSize = params.getInt("window", 250)
    val slideSize = params.getInt("slide", 150)

    val counts = text
      // split up the lines in pairs (2-tuple) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
      .countWindow(windowSize, slideSize)
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1)

    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("WindowWordCount")
  }

}
