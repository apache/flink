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

package org.apache.flink.streaming.examples.windowing.clickeventcount;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.examples.windowing.clickeventcount.functions.ClickEventStatisticsCollector;
import org.apache.flink.streaming.examples.windowing.clickeventcount.functions.CountingAggregator;
import org.apache.flink.streaming.examples.windowing.clickeventcount.records.ClickEvent;
import org.apache.flink.streaming.examples.windowing.clickeventcount.records.ClickEventDeserializationSchema;
import org.apache.flink.streaming.examples.windowing.clickeventcount.records.ClickEventStatistics;
import org.apache.flink.streaming.examples.windowing.clickeventcount.records.ClickEventStatisticsSerializationSchema;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A simple streaming job reading {@link ClickEvent}s from Kafka, counting events per 15 seconds and
 * writing the resulting {@link ClickEventStatistics} back to Kafka.
 *
 * <p> It can be run with or without checkpointing and with event time or processing time semantics.
 * </p>
 *
 * <p>The Job can be configured via the command line:</p>
 * * "--checkpointing": enables checkpointing
 * * "--event-time": set the StreamTimeCharacteristic to EventTime
 * * "--input-topic": the name of the Kafka Topic to consume {@link ClickEvent}s from
 * * "--output-topic": the name of the Kafka Topic to produce {@link ClickEventStatistics} to
 * * "--bootstrap.servers": comma-separated list of Kafka brokers
 *
 */
public class ClickEventCount {

	public static final String CHECKPOINTING_OPTION = "checkpointing";
	public static final String EVENT_TIME_OPTION = "event-time";

	public static final Time WINDOW_SIZE = Time.of(15, TimeUnit.SECONDS);

	public static void main(String[] args) throws Exception {
		final ParameterTool params = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		configureEnvironment(params, env);

		String inputTopic = params.get("input-topic", "input");
		String outputTopic = params.get("output-topic", "output");
		String brokers = params.get("bootstrap.servers", "localhost:9092");
		Properties kafkaProps = new Properties();
		kafkaProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "click-event-count");

		env.addSource(new FlinkKafkaConsumer<>(inputTopic, new ClickEventDeserializationSchema(), kafkaProps))
			.name("ClickEvent Source")
			.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ClickEvent>(Time.of(200, TimeUnit.MILLISECONDS)) {
				@Override
				public long extractTimestamp(final ClickEvent element) {
					return element.getTimestamp().getTime();
				}
			})
			.keyBy(ClickEvent::getPage)
			.timeWindow(WINDOW_SIZE)
			.aggregate(new CountingAggregator(),
				new ClickEventStatisticsCollector())
			.name("ClickEvent Counter")
			.addSink(new FlinkKafkaProducer<>(
				outputTopic,
				new ClickEventStatisticsSerializationSchema(outputTopic),
				kafkaProps,
				FlinkKafkaProducer.Semantic.AT_LEAST_ONCE))
			.name("ClickEventStatistics Sink");

		env.execute("Click Event Count");
	}

	private static void configureEnvironment(
			final ParameterTool params,
			final StreamExecutionEnvironment env) {

		boolean checkpointingEnabled = params.has(CHECKPOINTING_OPTION);
		boolean eventTimeSemantics = params.has(EVENT_TIME_OPTION);

		if (checkpointingEnabled) {
			env.enableCheckpointing(1000);
		}

		if (eventTimeSemantics) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}

		//disabling Operator chaining to make it easier to follow the Job in the WebUI
		env.disableOperatorChaining();
	}
}
