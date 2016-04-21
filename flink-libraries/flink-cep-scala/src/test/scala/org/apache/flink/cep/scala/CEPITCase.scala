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
package org.apache.flink.cep.scala

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.test.util.TestBaseUtils
import org.junit.{After, Before, Rule, Test}
import org.junit.rules.TemporaryFolder
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;

import scala.collection.mutable

@SuppressWarnings(Array("serial"))
class CEPITCase extends ScalaStreamingMultipleProgramsTestBase {
  private var resultPath: String = null
  private var expected: String = null
  val _tempFolder = new TemporaryFolder

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Before
  @throws[Exception]
  def before {
    resultPath = tempFolder.newFile.toURI.toString
    expected = ""
  }

  @After
  @throws[Exception]
  def after {
    TestBaseUtils.compareResultsByLinesInMemory(expected, resultPath)
  }

  @Test
  @throws[Exception]
  def testSimplePatternCEP {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[Event] = env.fromElements(
      new Event(1, "barfoo", 1.0),
      new Event(2, "start", 2.0),
      new Event(3, "foobar", 3.0),
      new SubEvent(4, "foo", 4.0, 1.0),
      new Event(5, "middle", 5.0),
      new SubEvent(6, "middle", 6.0, 2.0),
      new SubEvent(7, "bar", 3.0, 3.0),
      new Event(42, "42", 42.0),
      new Event(8, "end", 1.0))
    val pattern: Pattern[Event, _] = Pattern.begin[Event]("start")
      .where((value: Event) => value.getName == "start")
      .followedBy("middle")
      .subtype(classOf[SubEvent])
      .where((value: SubEvent) => value.getName == "middle")
      .followedBy("end")
      .where((value: Event) => value.getName == "end")
    val result: DataStream[String] = CEP.pattern(input, pattern)
      .select((pattern: mutable.Map[String, Event]) => {
        val builder: StringBuilder = new StringBuilder
        builder.append(pattern.get("start").get.getId)
          .append(",")
          .append(pattern.get("middle").get.getId)
          .append(",")
          .append(pattern.get("end").get.getId)
          .toString
      })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "2,6,8"
    env.execute
  }

  @Test
  @throws[Exception]
  def testSimpleKeyedPatternCEP {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val input: DataStream[Event] = env.fromElements(
      new Event(1, "barfoo", 1.0),
      new Event(2, "start", 2.0),
      new Event(3, "start", 2.1),
      new Event(3, "foobar", 3.0),
      new SubEvent(4, "foo", 4.0, 1.0),
      new SubEvent(3, "middle", 3.2, 1.0),
      new Event(42, "start", 3.1),
      new SubEvent(42, "middle", 3.3, 1.2),
      new Event(5, "middle", 5.0),
      new SubEvent(2, "middle", 6.0, 2.0),
      new SubEvent(7, "bar", 3.0, 3.0),
      new Event(42, "42", 42.0),
      new Event(3, "end", 2.0),
      new Event(2, "end", 1.0),
      new Event(42, "end", 42.0))
      .keyBy((value: Event) => value.getId)
    val pattern: Pattern[Event, _] = Pattern.begin[Event]("start")
      .where((value: Event) => value.getName == "start")
      .followedBy("middle")
      .subtype(classOf[SubEvent])
      .where((value: SubEvent) => value.getName == "middle")
      .followedBy("end")
      .where((value: Event) => value.getName == "end")
    val result: DataStream[String] = CEP.pattern(input, pattern)
      .select((pattern: mutable.Map[String, Event]) => {
        val builder: StringBuilder = new StringBuilder
        builder
          .append(pattern.get("start").get.getId)
          .append(",")
          .append(pattern.get("middle").get.getId)
          .append(",")
          .append(pattern.get("end").get.getId)
          .toString
      })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "2,2,2\n3,3,3\n42,42,42"
    env.execute
  }

  @Test
  @throws[Exception]
  def testSimplePatternEventTime {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input: DataStream[Event] = env.fromElements(
      (new Event(1, "start", 1.0), 5L),
      (new Event(2, "middle", 2.0), 1L),
      (new Event(3, "end", 3.0), 3L),
      (new Event(4, "end", 4.0), 10L),
      (new Event(5, "middle", 5.0), 7L),
      (new Event(5, "middle", 5.0), 100L))
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(Event, Long)] {
        def extractTimestamp(element: (Event, Long), previousTimestamp: Long): Long = {
          element._2
        }

        def checkAndGetNextWatermark(lastElement: (Event, Long),
                                     extractedTimestamp: Long): Watermark = {
          new Watermark(lastElement._2 - 5)
        }
      }).map((value: (Event, Long)) => value._1)

    val pattern: Pattern[Event, _] = Pattern.begin[Event]("start")
      .where((value: Event) => value.getName == "start")
      .followedBy("middle")
      .where((value: Event) => value.getName == "middle")
      .followedBy("end")
      .where((value: Event) => value.getName == "end")

    val result: DataStream[String] = CEP.pattern(input, pattern)
      .select((pattern: mutable.Map[String, Event]) => {
        val builder: StringBuilder = new StringBuilder
        builder
          .append(pattern.get("start").get.getId)
          .append(",")
          .append(pattern.get("middle").get.getId)
          .append(",")
          .append(pattern.get("end").get.getId)
          .toString
      })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "1,5,4"
    env.execute
  }

  @Test
  @throws[Exception]
  def testSimpleKeyedPatternEventTime {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)
    val input: DataStream[Event] = env.fromElements(
      (new Event(1, "start", 1.0), 5L),
      (new Event(1, "middle", 2.0), 1L),
      (new Event(2, "middle", 2.0), 4L),
      (new Event(2, "start", 2.0), 3L),
      (new Event(1, "end", 3.0), 3L),
      (new Event(3, "start", 4.1), 5L),
      (new Event(1, "end", 4.0), 10L),
      (new Event(2, "end", 2.0), 8L),
      (new Event(1, "middle", 5.0), 7L),
      (new Event(3, "middle", 6.0), 9L),
      (new Event(3, "end", 7.0), 7L))
      .assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(Event, Long)] {
        def extractTimestamp(element: (Event, Long), currentTimestamp: Long): Long = {
          element._2
        }

        def checkAndGetNextWatermark(lastElement: (Event, Long),
                                     extractedTimestamp: Long): Watermark = {
          new Watermark(lastElement._2 - 5)
        }
      }).map((value: (Event, Long)) => value._1)
      .keyBy((value: Event) => value.getId)
    val pattern: Pattern[Event, _] = Pattern.begin[Event]("start")
      .where((value: Event) => value.getName == "start")
      .followedBy("middle")
      .where((value: Event) => value.getName == "middle")
      .followedBy("end")
      .where((value: Event) => value.getName == "end")
    val result: DataStream[String] = CEP.pattern(input, pattern)
      .select((pattern: mutable.Map[String, Event]) => {
      val builder: StringBuilder = new StringBuilder
      builder
        .append(pattern.get("start").get.getId)
        .append(",")
        .append(pattern.get("middle").get.getId)
        .append(",")
        .append(pattern.get("end").get.getId)
        .toString
    })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "1,1,1\n2,2,2"
    env.execute
  }

  @Test
  @throws[Exception]
  def testSimplePatternWithSingleState {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val input: DataStream[(Int, Int)] = env.fromElements((0, 1), (0, 2))
    val pattern: Pattern[(Int, Int), _] = Pattern.begin[(Int, Int)]("start")
      .where((rec: (Int, Int)) => rec._2 equals 1)
    val pStream: PatternStream[(Int, Int)] = CEP.pattern(input, pattern)
    val result: DataStream[(Int, Int)] = pStream
      .select((pattern: mutable.Map[String, (Int, Int)]) => {
        println(pattern)
        pattern.get("start").get
      })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "(0,1)"
    env.execute
  }

  @Test
  @throws[Exception]
  def testProcessingTimeWithWindow {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val input: DataStream[Int] = env.fromElements(1, 2)
    val pattern: Pattern[Int, _] = Pattern.begin[Int]("start")
      .followedBy("end").within(Time.days(1))
    val result: DataStream[Int] = CEP.pattern(input, pattern)
      .select[Int]((pattern: mutable.Map[String, Int]) => {
      pattern.get("start").get + pattern.get("end").get
    })
    result.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE)
    expected = "3"
    env.execute
  }
}
