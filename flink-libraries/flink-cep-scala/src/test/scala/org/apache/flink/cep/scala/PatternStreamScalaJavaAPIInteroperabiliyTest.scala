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
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.util.{Collector, TestLogger}

import scala.collection.JavaConverters._
import scala.collection.mutable
import org.junit.Assert._
import org.junit.Test

class PatternStreamScalaJavaAPIInteroperabiliyTest extends TestLogger {

  @Test
  @throws[Exception]
  def testScalaJavaAPISelectFunForwarding {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[(Int, Int)] = env.fromElements()
    val pattern: Pattern[(Int, Int), _] = Pattern.begin[(Int, Int)]("dummy")
    val pStream: PatternStream[(Int, Int)] = CEP.pattern(dummyDataStream, pattern)
    val param = mutable.Map("begin" ->(1, 2)).asJava
    val result: DataStream[(Int, Int)] = pStream
      .select((pattern: mutable.Map[String, (Int, Int)]) => {
        //verifies input parameter forwarding
        assertEquals(param, pattern.asJava)
        param.get("begin")
      })
    val out = extractUserFunction[StreamMap[java.util.Map[String, (Int, Int)], (Int, Int)]](result)
      .getUserFunction.map(param)
    //verifies output parameter forwarding
    assertEquals(param.get("begin"), out)
  }

  @Test
  @throws[Exception]
  def testScalaJavaAPIFlatSelectFunForwarding {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[List[Int]] = env.fromElements()
    val pattern: Pattern[List[Int], _] = Pattern.begin[List[Int]]("dummy")
    val pStream: PatternStream[List[Int]] = CEP.pattern(dummyDataStream, pattern)
    val inList = List(1, 2, 3)
    val inParam = mutable.Map("begin" -> inList).asJava
    val outList = new java.util.ArrayList[List[Int]]
    val outParam = new ListCollector[List[Int]](outList)

    val result: DataStream[List[Int]] = pStream

      .flatSelect((pattern: mutable.Map[String, List[Int]], out: Collector[List[Int]]) => {
        //verifies input parameter forwarding
        assertEquals(inParam, pattern.asJava)
        out.collect(pattern.get("begin").get)
      })

    extractUserFunction[StreamFlatMap[java.util.Map[String, List[Int]], List[Int]]](result).
      getUserFunction.flatMap(inParam, outParam)
    //verify output parameter forwarding and that flatMap function was actually called
    assertEquals(inList, outList.get(0))
  }

  def extractUserFunction[T](dataStream: DataStream[_]) = {
    dataStream.javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[_, _]]
      .getOperator
      .asInstanceOf[T]
  }
}
