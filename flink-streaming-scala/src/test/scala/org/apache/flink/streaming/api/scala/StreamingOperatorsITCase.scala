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

import org.apache.flink.api.common.functions.{FoldFunction, RichMapFunction}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.{AbstractTestBase, TestBaseUtils}
import org.apache.flink.util.MathUtils
import org.junit.rules.TemporaryFolder
import org.junit.{After, Before, Rule, Test}

class StreamingOperatorsITCase extends AbstractTestBase {

  var resultPath1: String = _
  var resultPath2: String = _
  var resultPath3: String = _
  var expected1: String = _
  var expected2: String = _
  var expected3: String = _

  val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  def before(): Unit = {
    val temp = tempFolder
    resultPath1 = temp.newFile.toURI.toString
    resultPath2 = temp.newFile.toURI.toString
    resultPath3 = temp.newFile.toURI.toString
    expected1 = ""
    expected2 = ""
    expected3 = ""
  }

  @After
  def after(): Unit = {
    TestBaseUtils.compareResultsByLinesInMemory(expected1, resultPath1)
    TestBaseUtils.compareResultsByLinesInMemory(expected2, resultPath2)
    TestBaseUtils.compareResultsByLinesInMemory(expected3, resultPath3)
  }

  /** Tests the streaming fold operation. For this purpose a stream of Tuple[Int, Int] is created.
    * The stream is grouped by the first field. For each group, the resulting stream is folded by
    * summing up the second tuple field.
    *
    * This test relies on the hash function used by the [[DataStream#keyBy]], which is
    * assumed to be [[MathUtils#murmurHash]].
    */
  @Test
  def testGroupedFoldOperator(): Unit = {
    val numElements = 10
    val numKeys = 2

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(2)
    env.getConfig.setMaxParallelism(2)

    val sourceStream = env.addSource(new SourceFunction[(Int, Int)] {

      override def run(ctx: SourceContext[(Int, Int)]): Unit = {
        0 until numElements foreach {
          // keys '1' and '2' hash to different buckets
          i => ctx.collect((1 + (MathUtils.murmurHash(i)) % numKeys, i))
        }
      }

      override def cancel(): Unit = {}
    })

    val splittedResult = sourceStream
      .keyBy(0)
      .fold(0, new FoldFunction[(Int, Int), Int] {
        override def fold(accumulator: Int, value: (Int, Int)): Int = {
          accumulator + value._2
        }
      })
      .map(new RichMapFunction[Int, (Int, Int)] {
        var key: Int = -1
        override def map(value: Int): (Int, Int) = {
          if (key == -1) {
            key = MathUtils.murmurHash(value) % numKeys
          }
          (key, value)
        }
      })
      .split{
        x =>
          Seq(x._1.toString)
      }

    splittedResult
      .select("0")
      .map(_._2)
      .javaStream
      .writeAsText(resultPath1, FileSystem.WriteMode.OVERWRITE)
    splittedResult
      .select("1")
      .map(_._2)
      .javaStream
      .writeAsText(resultPath2, FileSystem.WriteMode.OVERWRITE)

    val groupedSequence = 0 until numElements groupBy( MathUtils.murmurHash(_) % numKeys )

    expected1 = groupedSequence(0).scanLeft(0)(_ + _).tail.mkString("\n")
    expected2 = groupedSequence(1).scanLeft(0)(_ + _).tail.mkString("\n")

    env.execute()
  }

  @Test
  def testKeyedAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.getConfig.setMaxParallelism(1)

    val inp = env.fromElements(
      StreamingOperatorsITCase.Outer(1, StreamingOperatorsITCase.Inner(3, "alma"), true),
      StreamingOperatorsITCase.Outer(1, StreamingOperatorsITCase.Inner(6, "alma"), true),
      StreamingOperatorsITCase.Outer(2, StreamingOperatorsITCase.Inner(7, "alma"), true),
      StreamingOperatorsITCase.Outer(2, StreamingOperatorsITCase.Inner(8, "alma"), true)
    )

    inp
      .keyBy("a")
      .sum("i.c")
        .writeAsText(resultPath3, FileSystem.WriteMode.OVERWRITE)

    expected3 =
      "Outer(1,Inner(3,alma),true)\n" +
      "Outer(1,Inner(9,alma),true)\n" +
      "Outer(2,Inner(15,alma),true)\n" +
      "Outer(2,Inner(7,alma),true)"

    env.execute()
  }
}

object StreamingOperatorsITCase {
  case class Inner(c: Short, d: String)
  case class Outer(a: Int, i: StreamingOperatorsITCase.Inner, b: Boolean)
}

