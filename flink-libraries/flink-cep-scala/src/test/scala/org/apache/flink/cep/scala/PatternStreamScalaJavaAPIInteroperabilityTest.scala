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

import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.operators.{StreamFlatMap, StreamMap}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, TwoInputTransformation}
import org.apache.flink.util.{Collector, TestLogger}
import org.apache.flink.types.{Either => FEither}
import org.apache.flink.api.java.tuple.{Tuple2 => FTuple2}
import java.lang.{Long => JLong}
import java.util.{Map => JMap}
import java.util.{List => JList}

import org.apache.flink.cep.operator.{FlatSelectCepOperator, FlatSelectTimeoutCepOperator, SelectCepOperator}
import org.apache.flink.streaming.api.functions.co.CoMapFunction

import scala.collection.JavaConverters._
import scala.collection.Map
import org.junit.Assert._
import org.junit.Test

class PatternStreamScalaJavaAPIInteroperabilityTest extends TestLogger {

  @Test
  @throws[Exception]
  def testScalaJavaAPISelectFunForwarding {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[(Int, Int)] = env.fromElements()
    val pattern: Pattern[(Int, Int), (Int, Int)] = Pattern.begin[(Int, Int)]("dummy")
    val pStream: PatternStream[(Int, Int)] = CEP.pattern(dummyDataStream, pattern)
    val param = Map("begin" -> List((1, 2)))
    val result: DataStream[(Int, Int)] = pStream
      .select((pattern: Map[String, Iterable[(Int, Int)]]) => {
        //verifies input parameter forwarding
        assertEquals(param, pattern)
        param.get("begin").get(0)
      })
    val out = extractUserFunction[SelectCepOperator[(Int, Int), Byte, (Int, Int)]](result)
      .getUserFunction.select(param.mapValues(_.asJava).asJava)
    //verifies output parameter forwarding
    assertEquals(param.get("begin").get(0), out)
  }

  @Test
  @throws[Exception]
  def testScalaJavaAPIFlatSelectFunForwarding {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[List[Int]] = env.fromElements()
    val pattern: Pattern[List[Int], List[Int]] = Pattern.begin[List[Int]]("dummy")
    val pStream: PatternStream[List[Int]] = CEP.pattern(dummyDataStream, pattern)
    val inList = List(1, 2, 3)
    val inParam = Map("begin" -> List(inList))
    val outList = new java.util.ArrayList[List[Int]]
    val outParam = new ListCollector[List[Int]](outList)

    val result: DataStream[List[Int]] = pStream

      .flatSelect((pattern: Map[String, Iterable[List[Int]]], out: Collector[List[Int]]) => {
        //verifies input parameter forwarding
        assertEquals(inParam, pattern)
        out.collect(pattern.get("begin").get.head)
      })

    extractUserFunction[FlatSelectCepOperator[List[Int], Byte, List[Int]]](result).
      getUserFunction.flatSelect(inParam.mapValues(_.asJava).asJava, outParam)
    //verify output parameter forwarding and that flatMap function was actually called
    assertEquals(inList, outList.get(0))
  }

  @Test
  @throws[Exception]
  def testTimeoutHandling: Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[String] = env.fromElements()
    val pattern: Pattern[String, String] = Pattern.begin[String]("dummy")
    val pStream: PatternStream[String] = CEP.pattern(dummyDataStream, pattern)
    val inParam = Map("begin" -> List("barfoo"))
    val outList = new java.util.ArrayList[Either[String, String]]
    val output = new ListCollector[Either[String, String]](outList)
    val expectedOutput = List(Right("match"), Right("barfoo"), Left("timeout"), Left("barfoo"))
      .asJava

    val outputTag = OutputTag[Either[String, String]]("timeouted")
    val result: DataStream[Either[String, String]] = pStream.flatSelect(outputTag) {
        (pattern: Map[String, Iterable[String]], timestamp: Long,
         out: Collector[Either[String, String]]) =>
          out.collect(Left("timeout"))
          out.collect(Left(pattern("begin").head))
      } {
        (pattern: Map[String, Iterable[String]], out: Collector[Either[String, String]]) =>
          //verifies input parameter forwarding
          assertEquals(inParam, pattern)
          out.collect(Right("match"))
          out.collect(Right(pattern("begin").head))
      }

    val fun = extractUserFunction[FlatSelectTimeoutCepOperator[String, Either[String, String],
      Either[String, String], Byte]](
      result).getUserFunction

    fun.getFlatSelectFunction.flatSelect(inParam.mapValues(_.asJava).asJava, output)
    fun.getFlatTimeoutFunction.timeout(inParam.mapValues(_.asJava).asJava, 42L, output)

    assertEquals(expectedOutput, outList)
  }

  def extractUserFunction[T](dataStream: DataStream[_]) = {
    dataStream.javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[_, _]]
      .getOperator
      .asInstanceOf[T]
  }

}
