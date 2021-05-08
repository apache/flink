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

package org.apache.flink.streaming.scala.examples

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.examples.iteration.util.IterateExampleData
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData
import org.apache.flink.streaming.examples.windowing.util.SessionWindowingData
import org.apache.flink.streaming.scala.examples.iteration.IterateExample
import org.apache.flink.streaming.scala.examples.join.WindowJoin
import org.apache.flink.streaming.scala.examples.join.WindowJoin.{Grade, Salary}
import org.apache.flink.streaming.scala.examples.twitter.TwitterExample
import org.apache.flink.streaming.scala.examples.windowing.{SessionWindowing, WindowWordCount}
import org.apache.flink.streaming.scala.examples.wordcount.WordCount
import org.apache.flink.streaming.test.examples.join.WindowJoinData
import org.apache.flink.test.testdata.WordCountData
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.junit.Test

/**
 * Integration test for streaming programs in Scala examples.
 */
class StreamingExamplesITCase extends AbstractTestBase {

  @Test
  def testIterateExample(): Unit = {
    val inputPath = createTempFile("fibonacciInput.txt", IterateExampleData.INPUT_PAIRS)
    val resultPath = getTempDirPath("result")

    // the example is inherently non-deterministic. The iteration timeout of 5000 ms
    // is frequently not enough to make the test run stable on CI infrastructure
    // with very small containers, so we cannot do a validation here
    IterateExample.main(Array(
      "--input", inputPath,
      "--output", resultPath
    ))
  }

  @Test
  def testWindowJoin(): Unit = {
    val resultPath = File.createTempFile("result-path", "dir").toURI.toString
    try {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

      val grades = env
        .fromCollection(WindowJoinData.GRADES_INPUT.split("\n"))
        .map( line => {
          val fields = line.split(",")
          Grade(fields(1), fields(2).toInt)
        })

      val salaries = env
        .fromCollection(WindowJoinData.SALARIES_INPUT.split("\n"))
        .map( line => {
          val fields = line.split(",")
          Salary(fields(1), fields(2).toInt)
        })

      WindowJoin.joinStreams(grades, salaries, 100)
        .writeAsText(resultPath, WriteMode.OVERWRITE)

      env.execute()

      TestBaseUtils.checkLinesAgainstRegexp(resultPath, "^Person\\([a-z]+,(\\d),(\\d)+\\)")
    }
    finally try
      FileUtils.deleteDirectory(new File(resultPath))

    catch {
      case _: Throwable =>
    }
  }

  @Test
  def testTwitterExample(): Unit = {
    val resultPath = getTempDirPath("result")
    TwitterExample.main(Array("--output", resultPath))
    TestBaseUtils.compareResultsByLinesInMemory(
      TwitterExampleData.STREAMING_COUNTS_AS_TUPLES,
      resultPath)
  }

  @Test
  def testSessionWindowing(): Unit = {
    val resultPath = getTempDirPath("result")
    SessionWindowing.main(Array("--output", resultPath))
    TestBaseUtils.compareResultsByLinesInMemory(SessionWindowingData.EXPECTED, resultPath)
  }

  @Test
  def testWindowWordCount(): Unit = {
    val windowSize = "250"
    val slideSize = "150"
    val textPath = createTempFile("text.txt", WordCountData.TEXT)
    val resultPath = getTempDirPath("result")

    WindowWordCount.main(Array(
      "--input", textPath,
      "--output", resultPath,
      "--window", windowSize,
      "--slide", slideSize
    ))

    // since the parallel tokenizers might have different speed
    // the exact output can not be checked just whether it is well-formed
    // checks that the result lines look like e.g. (faust, 2)
    TestBaseUtils.checkLinesAgainstRegexp(resultPath, "^\\([a-z]+,(\\d)+\\)")
  }

  @Test
  def testWordCount(): Unit = {
    val textPath = createTempFile("text.txt", WordCountData.TEXT)
    val resultPath = getTempDirPath("result")

    WordCount.main(Array(
      "--input", textPath,
      "--output", resultPath
    ))

    TestBaseUtils.compareResultsByLinesInMemory(
      WordCountData.STREAMING_COUNTS_AS_TUPLES,
      resultPath)
  }
}
