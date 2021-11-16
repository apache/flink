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

import org.apache.flink.api.connector.sink.SinkWriter
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, RequestIndexer}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions.mapAsJavaMap


object Elasticsearch7SinkExample {
  def main(args: Array[String]) {

    val parameterTool = ParameterTool.fromArgs(args)

    if (parameterTool.getNumberOfParameters < 3) {
      println("Missing parameters!\n" + "Usage:" +
        " --numRecords <numRecords> --index <index> --type <type>")
      return
    }
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(5000)

    val source: DataStream[(String)] = env.fromSequence(0, 20 - 1)
      .map(v =>  "message #" + v.toString)

    source.sinkTo(
      new Elasticsearch7SinkBuilder[String]
        // This instructs the sink to emit after every element, otherwise they would
        // be buffered
        .setBulkFlushMaxActions(1)
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) =>
          indexer.add(createIndexRequest(element, parameterTool)))
        .build())

    env.execute("Elasticsearch7.x end to end sink test example")
  }

  def createIndexRequest(element: (String), parameterTool: (ParameterTool)): IndexRequest = {

    val json2 = Map(
      "data" -> element.asInstanceOf[AnyRef]
    )

    Requests.indexRequest.index(parameterTool.getRequired("index"))
      .`type`(parameterTool.getRequired("type")).source(mapAsJavaMap(json2))
  }
}
