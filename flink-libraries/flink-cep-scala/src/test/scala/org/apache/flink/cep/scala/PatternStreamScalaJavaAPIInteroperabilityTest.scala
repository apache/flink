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

import java.lang

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.functions.util.{FunctionUtils, ListCollector}
import org.apache.flink.cep.functions.{PatternProcessFunction, TimedOutPartialMatchHandler}
import org.apache.flink.cep.operator.CepOperator
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.util
import org.apache.flink.util.{Collector, TestLogger}
import org.junit.Assert._
import org.junit.Test
import org.mockito.Mockito

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, mutable}

class PatternStreamScalaJavaAPIInteroperabilityTest extends TestLogger {

  @Test
  @throws[Exception]
  def testScalaJavaAPISelectFunForwarding() {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[(Int, Int)] = env.fromElements()
    val pattern: Pattern[(Int, Int), (Int, Int)] = Pattern.begin[(Int, Int)]("dummy")
    val pStream: PatternStream[(Int, Int)] = CEP.pattern(dummyDataStream, pattern)
    val param = Map("begin" -> List((1, 2)))
    val result: DataStream[(Int, Int)] = pStream
      .select((pattern: Map[String, Iterable[(Int, Int)]]) => {
        //verifies input parameter forwarding
        assertEquals(param, pattern)
        param("begin").head
      })

    val outList = new java.util.ArrayList[(Int, Int)]
    val outParam = new ListCollector[(Int, Int)](outList)

    val fun = extractFun[(Int, Int), (Int, Int)](result)
    fun.processMatch(param.mapValues(_.asJava).asJava, new ListTestContext, outParam)
    //verifies output parameter forwarding
    assertEquals(param("begin").head, outList.get(0))
  }

  @Test
  @throws[Exception]
  def testScalaJavaAPIFlatSelectFunForwarding() {
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
        out.collect(pattern("begin").head)
      })

    val fun = extractFun[List[Int], List[Int]](result)
    fun.processMatch(inParam.mapValues(_.asJava).asJava, new ListTestContext, outParam)
    //verify output parameter forwarding and that flatMap function was actually called
    assertEquals(inList, outList.get(0))
  }

  @Test
  @throws[Exception]
  def testTimeoutHandling(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val dummyDataStream: DataStream[String] = env.fromElements()
    val pattern: Pattern[String, String] = Pattern.begin[String]("dummy")
    val pStream: PatternStream[String] = CEP.pattern(dummyDataStream, pattern)
    val inParam = Map("begin" -> List("barfoo"))
    val outList = new java.util.ArrayList[Either[String, String]]
    val output = new ListCollector[Either[String, String]](outList)

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

    val fun = extractFun[String, Either[String, String]](result)

    val ctx = new ListTestContext
    fun.processMatch(inParam.mapValues(_.asJava).asJava, ctx, output)
    fun.asInstanceOf[TimedOutPartialMatchHandler[String]]
      .processTimedOutMatch(inParam.mapValues(_.asJava).asJava, ctx)

    assertEquals(List(Right("match"), Right("barfoo")).asJava, outList)
    assertEquals(List(Left("timeout"), Left("barfoo")).asJava, ctx.getElements(outputTag).asJava)
  }

  def extractFun[IN, OUT](dataStream: DataStream[OUT]): PatternProcessFunction[IN, OUT] = {
    val oper = dataStream.javaStream
      .getTransformation
      .asInstanceOf[OneInputTransformation[_, _]]
      .getOperator
      .asInstanceOf[CepOperator[IN, Byte, OUT]]

    val fun = oper.getUserFunction
    FunctionUtils.setFunctionRuntimeContext(fun, Mockito.mock(classOf[RuntimeContext]))
    FunctionUtils.openFunction(fun, new Configuration())
    fun
  }

  class ListTestContext extends PatternProcessFunction.Context {

    private val outputs = new mutable.HashMap[util.OutputTag[_], mutable.ListBuffer[Any]]()

    def getElements(outputTag: OutputTag[_]): ListBuffer[Any] = {
      outputs.getOrElse(outputTag, ListBuffer.empty)
    }

    override def output[X](outputTag: util.OutputTag[X], value: X): Unit = {
      outputs.getOrElseUpdate(outputTag, ListBuffer.empty).append(value)
    }

    override def timestamp(): Long = 0

    override def currentProcessingTime(): Long = System.currentTimeMillis()
  }

}
