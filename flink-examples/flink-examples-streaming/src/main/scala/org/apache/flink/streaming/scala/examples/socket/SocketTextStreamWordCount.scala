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

package org.apache.flink.streaming.scala.examples.socket

import org.apache.flink.streaming.api.scala._

import scala.language.postfixOps

/**
 * This example shows an implementation of WordCount with data from a text socket. 
 * To run the example make sure that the service providing the text data is already up and running.
 *
 * To start an example socket text stream on your local machine run netcat from a command line, 
 * where the parameter specifies the port number:
 *
 * {{{
 *   nc -lk 9999
 * }}}
 *
 * Usage:
 * {{{
 *   SocketTextStreamWordCount <hostname> <port> <output path>
 * }}}
 *
 * This example shows how to:
 *
 *   - use StreamExecutionEnvironment.socketTextStream
 *   - write a simple Flink Streaming program in scala.
 *   - write and use user-defined functions.
 */
object SocketTextStreamWordCount {

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Create streams for names and ages by mapping the inputs to the corresponding objects
    val text = env.socketTextStream(hostName, port)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .sum(1)

    if (fileOutput) {
      counts.writeAsText(outputPath, 1)
    } else {
      counts print
    }

    env.execute("Scala SocketTextStreamWordCount Example")
  }

  private def parseParameters(args: Array[String]): Boolean = {
      if (args.length == 3) {
        fileOutput = true
        hostName = args(0)
        port = args(1).toInt
        outputPath = args(2)
      } else if (args.length == 2) {
        hostName = args(0)
        port = args(1).toInt
      } else {
        System.err.println("Usage: SocketTextStreamWordCount <hostname> <port> [<output path>]")
        return false
      }
    true
  }

  private var fileOutput: Boolean = false
  private var hostName: String = null
  private var port: Int = 0
  private var outputPath: String = null

}
