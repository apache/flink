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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.examples.async.util.SimpleSource

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

object AsyncIOExample {

  /** An example of a [[RichAsyncFunction]] using an async client to query an external service. */
  class SampleAsyncFunction extends RichAsyncFunction[Int, String] {
    private var client: AsyncClient = _

    override def open(parameters: Configuration): Unit = {
      client = new AsyncClient
    }

    override def asyncInvoke(input: Int, resultFuture: ResultFuture[String]): Unit = {
      client.query(input).onComplete {
        case Success(value) => resultFuture.complete(Seq(value))
        case Failure(exception) => resultFuture.completeExceptionally(exception)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    var mode: String = null
    var timeout = 0L

    try {
      mode = params.get("waitMode", "ordered")
      timeout = params.getLong("timeout", 10000L)
    } catch {
      case e: Exception =>
        println("To customize example, use: AsyncIOExample [--waitMode <ordered or unordered>]")
        throw e
    }

    // obtain execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // create input stream of a single integer
    val inputStream = env.addSource(new SimpleSource).map(_.toInt)

    val function = new SampleAsyncFunction

    // add async operator to streaming job
    val result = mode.toUpperCase match {
      case "ORDERED" =>
        AsyncDataStream.orderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS, 20)
      case "UNORDERED" =>
        AsyncDataStream.unorderedWait(inputStream, function, timeout, TimeUnit.MILLISECONDS, 20)
      case _ => throw new IllegalStateException("Unknown mode: " + mode)
    }

    result.print()

    // execute the program
    env.execute("Async IO Example: " + mode)
  }
}
