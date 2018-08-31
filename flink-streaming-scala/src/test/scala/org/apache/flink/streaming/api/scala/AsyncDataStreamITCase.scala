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

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala.AsyncDataStreamITCase._
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.flink.test.util.AbstractTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

object AsyncDataStreamITCase {
  val timeout = 1000L
  private var testResult: mutable.ArrayBuffer[Int] = _
}

class AsyncDataStreamITCase extends AbstractTestBase {

  @Test
  def testOrderedWait(): Unit = {
    testAsyncWait(true)
  }

  @Test
  def testUnorderedWait(): Unit = {
    testAsyncWait(false)
  }

  private def testAsyncWait(ordered: Boolean): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.fromElements(1, 2)

    val asyncMapped = if (ordered) {
      AsyncDataStream.orderedWait(
        source, new MyAsyncFunction(), timeout, TimeUnit.MILLISECONDS)
    } else {
      AsyncDataStream.unorderedWait(
        source, new MyAsyncFunction(), timeout, TimeUnit.MILLISECONDS)
    }

    executeAndValidate(ordered, env, asyncMapped, mutable.ArrayBuffer[Int](2, 6))
  }

  private def executeAndValidate(ordered: Boolean,
      env: StreamExecutionEnvironment,
      dataStream: DataStream[Int],
      expectedResult: mutable.ArrayBuffer[Int]): Unit = {

    testResult = mutable.ArrayBuffer[Int]()
    dataStream.addSink(new SinkFunction[Int]() {
      override def invoke(value: Int) {
        testResult += value
      }
    })

    env.execute("testAsyncDataStream")

    if (ordered) {
      assertEquals(expectedResult, testResult)
    } else {
      assertEquals(expectedResult, testResult.sorted)
    }
  }

  @Test
  def testOrderedWaitUsingAnonymousFunction(): Unit = {
    testAsyncWaitUsingAnonymousFunction(true)
  }

  @Test
  def testUnorderedWaitUsingAnonymousFunction(): Unit = {
    testAsyncWaitUsingAnonymousFunction(false)
  }

  private def testAsyncWaitUsingAnonymousFunction(ordered: Boolean): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val source = env.fromElements(1, 2)

    val asyncFunction: (Int, ResultFuture[Int]) => Unit =
      (input, collector: ResultFuture[Int]) => Future {
          collector.complete(Seq(input * 2))
      }(ExecutionContext.global)
    val asyncMapped = if (ordered) {
      AsyncDataStream.orderedWait(source, timeout, TimeUnit.MILLISECONDS) {
        asyncFunction
      }
    } else {
      AsyncDataStream.unorderedWait(source, timeout, TimeUnit.MILLISECONDS) {
        asyncFunction
      }
    }

    executeAndValidate(ordered, env, asyncMapped, mutable.ArrayBuffer[Int](2, 4))
  }

}

class MyAsyncFunction extends AsyncFunction[Int, Int] {
  override def asyncInvoke(input: Int, resultFuture: ResultFuture[Int]): Unit = {
    Future {
      // trigger the timeout of the even input number
      if (input % 2 == 0) {
        Thread.sleep(AsyncDataStreamITCase.timeout + 1000)
      }

      resultFuture.complete(Seq(input * 2))
    } (ExecutionContext.global)
  }
  override def timeout(input: Int, resultFuture: ResultFuture[Int]): Unit = {
    resultFuture.complete(Seq(input * 3))
  }
}
