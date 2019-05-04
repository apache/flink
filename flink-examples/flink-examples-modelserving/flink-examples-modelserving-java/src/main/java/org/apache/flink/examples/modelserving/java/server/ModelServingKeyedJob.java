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

package org.apache.flink.examples.modelserving.java.server;

import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.examples.modelserving.java.configuration.ModelServingConfiguration;
import org.apache.flink.examples.modelserving.java.model.WineFactoryResolver;
import org.apache.flink.modelserving.java.model.DataConverter;
import org.apache.flink.modelserving.java.model.DataToServe;
import org.apache.flink.modelserving.java.model.ModelToServe;
import org.apache.flink.modelserving.java.model.ServingResult;
import org.apache.flink.modelserving.java.server.keyed.DataProcessorKeyed;
import org.apache.flink.modelserving.java.server.typeschema.ByteArraySchema;
import org.apache.flink.modelserving.wine.Winerecord;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Optional;
import java.util.Properties;


/**
 * Complete model serving application (keyed)
 * This little application is based on a RichCoProcessFunction which works on a keyed streams. It
 * is applicable when a single applications serves multiple different models for different data
 * types. Every model is keyed with the type of data what it is designed for. Same key should be
 * present in the data, if it wants to use a specific model.
 * Scaling of the application is based on the data type - for every key there is a separate
 * instance of the RichCoProcessFunction dedicated to this type. All messages of the same type
 * are processed by the same instance of RichCoProcessFunction
 */
public class ModelServingKeyedJob {
	/**
	 * Main method.
	 */
	public static void main(String[] args) {
//		executeLocal();
		executeServer();
	}


	/**
	 *  Execute on the local Flink server.
	 */
	private static void  executeServer() {

		// We use a mini cluster here for sake of simplicity, because I don't want
		// to require a Flink installation to run this demo. Everything should be
		// contained in this JAR.
		int port = 6124;
		int parallelism = 2;
		Configuration config = new Configuration();
		config.setInteger(JobManagerOptions.PORT, port);
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);
		// In a non MiniCluster setup queryable state is enabled by default.
		config.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);
		config.setString(QueryableStateOptions.PROXY_PORT_RANGE, "9069");
		config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 2);
		config.setInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS, 2);

		config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9067");
		config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 2);
		config.setInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS, 2);

		MiniClusterConfiguration clusterconfig =
			new MiniClusterConfiguration(config, 1, RpcServiceSharing.DEDICATED, null);
		try {
			// Create a local Flink server
			MiniCluster flinkCluster = new MiniCluster(clusterconfig);
			// Start server and create environment
			flinkCluster.start();
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", port);
			env.setParallelism(parallelism);
			// Build Graph
			buildGraph(env);
			JobGraph jobGraph = env.getStreamGraph().getJobGraph();
			// Submit to the server and wait for completion
			JobSubmissionResult result = flinkCluster.submitJob(jobGraph).get();
			System.out.println("Job ID : " + result.getJobID());
			Thread.sleep(Long.MAX_VALUE);
		} catch (Throwable t){
			t.printStackTrace();
		}
	}

	/**
	 *  Execute locally in the development environment.
	 */
	private static void  executeLocal(){
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		buildGraph(env);
		System.out.println("[info] Job ID: " + env.getStreamGraph().getJobGraph().getJobID());
		try {
			env.execute();
		}
		catch (Throwable t){
			t.printStackTrace();
		}
	}

	/**
	 *  Build execution graph.
	 *
	 *  @param env Flink execution environment
	 */
	private static void buildGraph(StreamExecutionEnvironment env) {
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.enableCheckpointing(5000);

		// configure Kafka consumer
		// Data
		Properties dataKafkaProps = new Properties();
		dataKafkaProps.setProperty("bootstrap.servers", ModelServingConfiguration.kafkaBrokers);
		dataKafkaProps.setProperty("group.id", ModelServingConfiguration.dataGroup);
		// always read the Kafka topic from the current location
		dataKafkaProps.setProperty("auto.offset.reset", "earliest");

		// Model
		Properties modelKafkaProps = new Properties();
		modelKafkaProps.setProperty("bootstrap.servers", ModelServingConfiguration.kafkaBrokers);
		modelKafkaProps.setProperty("group.id", ModelServingConfiguration.modelsGroup);
		// always read the Kafka topic from the current location
		modelKafkaProps.setProperty("auto.offset.reset", "earliest");

		// create a Kafka consumers
		// Data
		FlinkKafkaConsumer<byte[]> dataConsumer = new FlinkKafkaConsumer<>(
			ModelServingConfiguration.dataTopic,
			new ByteArraySchema(),
			dataKafkaProps);

		// Model
		FlinkKafkaConsumer<byte[]> modelConsumer = new FlinkKafkaConsumer<>(
			ModelServingConfiguration.modelsTopic,
			new ByteArraySchema(),
			modelKafkaProps);

		// Create input data streams
		DataStream<byte[]> modelsStream = env.addSource(modelConsumer, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
		DataStream<byte[]> dataStream = env.addSource(dataConsumer, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

		// Set DataConverter
		DataConverter.setResolver(new WineFactoryResolver());

		// Read data from streams
		DataStream<ModelToServe> models = modelsStream
			.flatMap((byte[] value, Collector<ModelToServe> out) -> {
				Optional<ModelToServe> model = DataConverter.convertModel(value);
				if (model.isPresent()){
					out.collect(model.get());
				} else {
					System.out.println("Failed to convert model input");
				}
			}).returns(ModelToServe.class)
			.keyBy(model -> model.getDataType());
		DataStream<DataToServe<Winerecord.WineRecord>> data = dataStream
			.flatMap((byte[] value, Collector<DataToServe<Winerecord.WineRecord>> out) -> {
				Optional<DataToServe<Winerecord.WineRecord>> record = DataRecord.convertData(value);
				if (record.isPresent()){
					out.collect(record.get());
				} else {
					System.out.println("Failed to convert data input");
				}
			}).returns(new TypeHint<DataToServe<Winerecord.WineRecord>>() {})
			.keyBy(record -> record.getType());
		// Merge streams
		data
			.connect(models)
			.process(new DataProcessorKeyed<Winerecord.WineRecord, Double>())
			.returns(new TypeHint<ServingResult<Double>>() {})
			.map(result -> {
				System.out.println(result);
				return result;
			}).returns(new TypeHint<ServingResult<Double>>() {});
	}
}
