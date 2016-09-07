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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Implements a streaming windowed version of the "WordCount" program.
 * 
 * This program connects to a server socket and reads strings from the socket.
 * The easiest way to try this out is to open a text sever (at port 12345) 
 * using the <i>netcat</i> tool via
 * <pre>
 * nc -l 12345
 * </pre>
 * and run this example with the port as an argument.
 */
object SocketWindowWordCount {

  /** Main program method */
  def main(args: Array[String]) : Unit = {
    
    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>', " +
          "where port is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }
    
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    
    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    // parse the data, group it, window it, and aggregate the counts 
    val windowCounts = text
          .flatMap { w => w.split("\\s") }
          .map { w => WordWithCount(w, 1) }
          .keyBy("word")
          .timeWindow(Time.seconds(5), Time.seconds(1))
          .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")
  }

  /** Data type for words with count */
  case class WordWithCount(word: String, count: Long)
}
