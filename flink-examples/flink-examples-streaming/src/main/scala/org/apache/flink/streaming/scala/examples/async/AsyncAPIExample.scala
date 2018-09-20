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

package org.apache.flink.streaming.scala.examples.async


import java.io.{BufferedReader, InputStreamReader}
import java.util.concurrent.TimeUnit

import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture}
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder

import scala.concurrent.{ExecutionContext, Future}

object AsyncAPIExample {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val input : DataStream[Int] = env.addSource(new SimpleSource())

    val quoteStream: DataStream[(Int, String)] = AsyncDataStream.unorderedWait(
      input,
      new AsyncQuoteRequest,
      1000,
      TimeUnit.MILLISECONDS,
      10)

    quoteStream.print()

    env.execute("Async API job")
  }
}

class AsyncQuoteRequest extends AsyncFunction[Int, (Int, String)] {

  /** The API specific client that can issue concurrent requests with callbacks */

  /** The context used for the future callbacks */
  implicit lazy val executor: ExecutionContext = ExecutionContext.global

  lazy val client = new Quote()

  override def asyncInvoke(input: Int, resultFuture: ResultFuture[(Int, String)]): Unit = {


    // issue the asynchronous request, receive a future for the result
    val resultFutureRequested: Future[String] = Future {
      client.getQuote(input.toString)
    }

    // set the callback to be executed once the request by the client is complete
    // the callback simply forwards the result to the result future
    resultFutureRequested.onSuccess {
      case result: String => {
        resultFuture.complete(Iterable((input, result)))
      }
    }
  }


}

class Quote {
  @throws[Exception]
  def getQuote(number: String) : String = {
    val url = "http://gturnquist-quoters.cfapps.io/api/" + number
    val client = HttpClientBuilder.create.build
    val request = new HttpGet(url)
    val response = client.execute(request)
    val rd = new BufferedReader(new InputStreamReader(response.getEntity.getContent))
    rd.readLine
  }
}
