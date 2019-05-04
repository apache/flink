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

package org.apache.flink.examples.modelserving.scala.server

import java.util.Properties

import org.apache.flink.examples.modelserving.configuration.ModelServingConfiguration
import org.apache.flink.examples.modelserving.scala.model.WineFactoryResolver
import org.apache.flink.modelserving.scala.model.{DataToServe, ModelToServe}
import org.apache.flink.modelserving.scala.server.keyed.DataProcessorKeyed
import org.apache.flink.modelserving.scala.server.typeschema.ByteArraySchema
import org.apache.flink.configuration.{Configuration, JobManagerOptions, QueryableStateOptions, TaskManagerOptions}
import org.apache.flink.modelserving.wine.winerecord.WineRecord
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration, RpcServiceSharing}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * Complete model serving application (keyed)
  *
  *
  * This little application is based on a RichCoProcessFunction which works on a keyed streams. It
  * is applicable when a single applications serves multiple different models for different data
  * types. Every model is keyed with the type of data what it is designed for. Same key should be
  * present in the data, if it wants to use a specific model.
  * Scaling of the application is based on the data type - for every key there is a separate
  * instance of the RichCoProcessFunction dedicated to this type. All messages of the same type
  * are processed by the same instance of RichCoProcessFunction
  */
object ModelServingKeyedJob {

  import ModelServingConfiguration._

  /**
    *  Main mmethod.
    */
  def main(args: Array[String]): Unit = {
//    executeLocal()
    executeServer()
  }

  /**
    *  Execute on the local Flink server.
    */
  def executeServer() : Unit = {

    // We use a mini cluster here for sake of simplicity, because I don't want
    // to require a Flink installation to run this demo. Everything should be
    // contained in this JAR.

    val port = 6124
    val parallelism = 2

    val config = new Configuration()
    config.setInteger(JobManagerOptions.PORT, port)
    config.setString(JobManagerOptions.ADDRESS, "localhost")
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism)

    // In a non MiniCluster setup queryable state is enabled by default.
    config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true)
    config.setString(QueryableStateOptions.PROXY_PORT_RANGE, "9069")
    config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS, 2)

    config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9067")
    config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS, 2)


    // Create a local Flink server
    val flinkCluster = new MiniCluster(
      new MiniClusterConfiguration(config, 1, RpcServiceSharing.SHARED, null))
    try {
      // Start server and create environment
      flinkCluster.start()
      val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", port)
      // Build Graph
      buildGraph(env)
      val jobGraph = env.getStreamGraph.getJobGraph()
      // Submit to the server and wait for completion
      val submit = flinkCluster.submitJob(jobGraph).get()
      System.out.println(s"Job ID: ${submit.getJobID}")
      Thread.sleep(Long.MaxValue)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    *  Execute locally in the development environment.
    */
  def executeLocal() : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    buildGraph(env)
    System.out.println("[info] Job ID: " + env.getStreamGraph.getJobGraph().getJobID)
    env.execute()
  }

  /**
    *  Build execution graph.
    *
    *  @param env Flink execution environment
    */
  def buildGraph(env : StreamExecutionEnvironment) : Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(5000)

    // configure Kafka consumer
    // Data
    val dataKafkaProps = new Properties
    dataKafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER)
    dataKafkaProps.setProperty("group.id", DATA_GROUP)
    // always read the Kafka topic from the current location
    dataKafkaProps.setProperty("auto.offset.reset", "latest")

    // Model
    val modelKafkaProps = new Properties
    modelKafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER)
    modelKafkaProps.setProperty("group.id", MODELS_GROUP)
    // always read the Kafka topic from the current location
    modelKafkaProps.setProperty("auto.offset.reset", "earliest")

    // create a Kafka consumers
    // Data
    val dataConsumer = new FlinkKafkaConsumer[Array[Byte]](
      DATA_TOPIC,
      new ByteArraySchema,
      dataKafkaProps
    )

    // Model
    val modelConsumer = new FlinkKafkaConsumer[Array[Byte]](
      MODELS_TOPIC,
      new ByteArraySchema,
      modelKafkaProps
    )

    // Create input data streams
    val modelsStream = env.addSource(modelConsumer)
    val dataStream = env.addSource(dataConsumer)

    // Set modelToServe
    ModelToServe.setResolver(WineFactoryResolver)

    // Read models from streams
    val models = modelsStream.map(ModelToServe.fromByteArray(_))
      .flatMap(BadDataHandler[ModelToServe])
      .keyBy(_.dataType)
    // Read data from streams
    val data = dataStream.map(DataRecord.fromByteArray(_))
      .flatMap(BadDataHandler[DataToServe[WineRecord]])
      .keyBy(_.getType)

    // Merge streams
    data
      .connect(models)
      .process(DataProcessorKeyed[WineRecord, Double]())
      .map(result =>
        println(s"Model serving in ${result.duration} ms, with result ${result.result}"))
  }
}
