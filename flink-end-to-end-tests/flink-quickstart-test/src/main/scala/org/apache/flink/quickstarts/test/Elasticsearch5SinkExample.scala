/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.quickstarts.test

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions.mapAsJavaMap

import java.net.{InetAddress, InetSocketAddress}
import java.util


object Elasticsearch5SinkExample {
  def main(args: Array[String]) {

    val parameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 3) {
      println("Missing parameters!\n" + "Usage:" +
        " --numRecords <numRecords> --index <index> --type <type>")
      return
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    val source: DataStream[(String)] = env.generateSequence(0, 20 - 1)
      .map(v =>  "message #" + v.toString)

    val config = new util.HashMap[String, String]
    config.put("bulk.flush.max.actions", "1")
    config.put("cluster.name", "elasticsearch") //default cluster name: elasticsearch

    val transports = new util.ArrayList[InetSocketAddress]
    transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))

    source.addSink(new ElasticsearchSink(config, transports,
      new ElasticsearchSinkFunction[(String)] {
      def createIndexRequest(element: (String)): IndexRequest = {

        val json2 = Map(
          "data" -> element.asInstanceOf[AnyRef]
        )

        Requests.indexRequest.index(parameterTool.getRequired("index"))
          .`type`(parameterTool.getRequired("type")).source(mapAsJavaMap(json2))
      }

      override def process(element: (String), ctx: RuntimeContext, indexer: RequestIndexer) {
        indexer.add(createIndexRequest(element))
      }
    }))

    env.execute("Elasticsearch5.x end to end sink test example")
  }
}
