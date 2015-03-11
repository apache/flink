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
package org.apache.flink.examples.scala.wordcount

import java.util
import java.util.regex.{Matcher, Pattern}

import org.apache.flink.api.scala._
import org.apache.flink.examples.java.wordcount.util.WordCountData

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files. 
 *
 * The input is a plain text file with lines separated by newline characters.
 *
 * Usage:
 * {{{
 *   WordCount <text path> <result path>>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[org.apache.flink.examples.java.wordcount.util.WordCountData]]
 *
 * This example shows how to:
 *
 *   - write a simple Flink program.
 *   - use Tuple data types.
 *   - write and use user-defined functions.
 *
 */


object top_page {
  def parseLong(s: String) = try {
    s.toLong
  } catch {
    case _ => 0L
  }

  def main(args: Array[String]) {
    val env = ExecutionEnvironment.getExecutionEnvironment
    var t = "a b c d"
    var el = t.split(" ")
    println("el", el.deep.mkString(", "))
    var p = Pattern.compile(" ");
    var m = p.matcher(t);
    println("Groups:")
    while(m.find()) {
      println(m.group())
    }


    System.exit(0)

    env.readTextFile(args(0))
      .map(_.split(" "))
      .filter(_.length == 4)
      .map { fields => (fields(1), parseLong(fields(2)))}
      .groupBy(0)
      .sum(1)
      .print()

    env.execute("Top Page")
  }
}

object WordCount {




  /*def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = getTextDataSet(env)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    if (fileOutput) {
      counts.writeAsCsv(outputPath, "\n", " ")
    } else {
      counts.print()
    }

    env.execute("Scala WordCount Example")
  } */

  private def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      fileOutput = true
      if (args.length == 2) {
        textPath = args(0)
        outputPath = args(1)
        true
      } else {
        System.err.println("Usage: WordCount <text path> <result path>")
        false
      }
    } else {
      System.out.println("Executing WordCount example with built-in default data.")
      System.out.println("  Provide parameters to read input data from a file.")
      System.out.println("  Usage: WordCount <text path> <result path>")
      true
    }
  }

  private def getTextDataSet(env: ExecutionEnvironment): DataSet[String] = {
    if (fileOutput) {
      env.readTextFile(textPath)
    }
    else {
      env.fromCollection(WordCountData.WORDS)
    }
  }

  private var fileOutput: Boolean = false
  private var textPath: String = null
  private var outputPath: String = null
}


