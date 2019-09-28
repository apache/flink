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
package org.apache.flink.streaming.api.scala


import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.FileSystem

import scala.language.existentials

/**
 * Test programs for built in output formats. Invoked from OutputFormatTest.
 */
object OutputFormatTestPrograms {

  def wordCountProgram(input: DataStream[String]): DataStream[(String, Int)] = {
    input.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)
  }

  def wordCountToText(input: String, outputPath : String) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(input)

    val counts = wordCountProgram(text)

    counts.writeAsText(outputPath)

    env.execute("Scala WordCountToText")
  }


  def wordCountToText(
      input : String,
      outputPath : String,
      writeMode : FileSystem.WriteMode) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(input)

    val counts = wordCountProgram(text)

    counts.writeAsText(outputPath, writeMode)

    env.execute("Scala WordCountToText")
  }


  def wordCountToCsv(input : String, outputPath : String) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(input)

    val counts = wordCountProgram(text)

    counts.writeAsCsv(outputPath)

    env.execute("Scala WordCountToCsv")
  }


  def wordCountToCsv(
      input : String,
      outputPath : String,
      writeMode : FileSystem.WriteMode) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(input)

    val counts = wordCountProgram(text)

    counts.writeAsCsv(outputPath, writeMode)

    env.execute("Scala WordCountToCsv")
  }


  def wordCountToCsv(
      input : String,
      outputPath : String,
      writeMode : FileSystem.WriteMode,
      rowDelimiter: String,
      fieldDelimiter: String) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.fromElements(input)

    val counts = wordCountProgram(text)

    counts.writeAsCsv(outputPath, writeMode, rowDelimiter, fieldDelimiter)

    env.execute("Scala WordCountToCsv")
  }

  def wordCountToSocket(input : String, outputHost : String, outputPort : Int) : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.fromElements(input)
    val counts = wordCountProgram(text)
      .map(tuple => tuple.toString() + "\n")

    counts.writeToSocket(outputHost, outputPort, new SimpleStringSchema())

    env.execute("Scala WordCountToCsv")
  }

}
