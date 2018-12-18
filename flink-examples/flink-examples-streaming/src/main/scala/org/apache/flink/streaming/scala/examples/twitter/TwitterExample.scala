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

package org.apache.flink.streaming.scala.examples.twitter

import java.util.StringTokenizer

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.streaming.examples.twitter.util.TwitterExampleData
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 *
 * The input is a Tweet stream from a TwitterSource.
 *
 * Usage:
 * {{{
 * TwitterExample [--output <path>]
 * [--twitter-source.consumerKey <key>
 * --twitter-source.consumerSecret <secret>
 * --twitter-source.token <token>
 * --twitter-source.tokenSecret <tokenSecret>]
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 *
 * This example shows how to:
 *
 *  - acquire external data,
 *  - use in-line defined functions,
 *  - handle flattened stream inputs.
 *
 */
object TwitterExample {

  def main(args: Array[String]): Unit = {

    // Checking input parameters
    val params = ParameterTool.fromArgs(args)
    println("Usage: TwitterExample [--output <path>] " +
      "[--twitter-source.consumerKey <key> " +
      "--twitter-source.consumerSecret <secret> " +
      "--twitter-source.token <token> " +
      "--twitter-source.tokenSecret <tokenSecret>]")

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    env.setParallelism(params.getInt("parallelism", 1))

    // get input data
    val streamSource: DataStream[String] =
    if (params.has(TwitterSource.CONSUMER_KEY) &&
      params.has(TwitterSource.CONSUMER_SECRET) &&
      params.has(TwitterSource.TOKEN) &&
      params.has(TwitterSource.TOKEN_SECRET)
    ) {
      env.addSource(new TwitterSource(params.getProperties))
    } else {
      print("Executing TwitterStream example with default props.")
      print("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
        "--twitter-source.token <token> " +
        "--twitter-source.tokenSecret <tokenSecret> specify the authentication info."
      )
      // get default test text data
      env.fromElements(TwitterExampleData.TEXTS: _*)
    }

    val tweets: DataStream[(String, Int)] = streamSource
      // selecting English tweets and splitting to (word, 1)
      .flatMap(new SelectEnglishAndTokenizeFlatMap)
      // group by words and sum their occurrences
      .keyBy(0).sum(1)

    // emit result
    if (params.has("output")) {
      tweets.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      tweets.print()
    }

    // execute program
    env.execute("Twitter Streaming Example")
  }

  /**
   * Deserialize JSON from twitter source
   *
   * Implements a string tokenizer that splits sentences into words as a
   * user-defined FlatMapFunction. The function takes a line (String) and
   * splits it into multiple pairs in the form of "(word,1)" ({{{ Tuple2<String, Integer> }}}).
   */
  private class SelectEnglishAndTokenizeFlatMap extends FlatMapFunction[String, (String, Int)] {
    lazy val jsonParser = new ObjectMapper()

    override def flatMap(value: String, out: Collector[(String, Int)]): Unit = {
      // deserialize JSON from twitter source
      val jsonNode = jsonParser.readValue(value, classOf[JsonNode])
      val isEnglish = jsonNode.has("user") &&
        jsonNode.get("user").has("lang") &&
        jsonNode.get("user").get("lang").asText == "en"
      val hasText = jsonNode.has("text")

      (isEnglish, hasText, jsonNode) match {
        case (true, true, node) => {
          val tokens = new ListBuffer[(String, Int)]()
          val tokenizer = new StringTokenizer(node.get("text").asText())

          while (tokenizer.hasMoreTokens) {
            val token = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase()
            if (token.nonEmpty)out.collect((token, 1))
          }
        }
        case _ =>
      }
    }
  }
}
