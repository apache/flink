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

package org.apache.flink.cep.examples;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.examples.events.MonitoringEvent;
import org.apache.flink.cep.examples.events.TemperatureAlert;
import org.apache.flink.cep.examples.events.TemperatureEvent;
import org.apache.flink.cep.examples.events.TemperatureWarning;
import org.apache.flink.cep.examples.functions.TemperatureAlertPatternFlatSelectFunction;
import org.apache.flink.cep.examples.functions.TemperatureWarningPatternSelectFunction;
import org.apache.flink.cep.examples.sources.MonitoringEventSource;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * CEP example monitoring program
 * <p>
 * This example program generates a stream of monitoring events which are analyzed using Flink's CEP library.
 * The input event stream consists of temperature and power events from a set of racks. The goal is to detect
 * when a rack is about to overheat. In order to do that, we create a CEP pattern which generates a
 * TemperatureWarning whenever it sees two consecutive temperature events in a given time interval whose temperatures
 * are higher than a given threshold value. A warning itself is not critical but if we see two warning for the same rack
 * whose temperatures are rising, we want to generate an alert. This is achieved by defining another CEP pattern which
 * analyzes the stream of generated temperature warnings.
 */
public class CEPMonitoringExample {
	private static final double TEMPERATURE_THRESHOLD = 100;

	private static final int MAX_RACK_ID = 10;
	private static final long PAUSE = 100;
	private static final double TEMPERATURE_RATIO = 0.5;
	private static final double POWER_STD = 10;
	private static final double POWER_MEAN = 100;
	private static final double TEMP_STD = 20;
	private static final double TEMP_MEAN = 80;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Input stream of monitoring events
		DataStream<MonitoringEvent> inputEventStream = env
			.addSource(new MonitoringEventSource(
				MAX_RACK_ID,
				PAUSE,
				TEMPERATURE_RATIO,
				POWER_STD,
				POWER_MEAN,
				TEMP_STD,
				TEMP_MEAN))
			.assignTimestampsAndWatermarks(new IngestionTimeExtractor<MonitoringEvent>());

		// Warning pattern: Two consecutive temperature events whose temperature is higher than the given threshold
		// appearing within a time interval of 10 seconds

		FilterFunction<TemperatureEvent>
			thresholdFilterFunction = new FilterFunction<TemperatureEvent>() {

			@Override
			public boolean filter(TemperatureEvent event) throws Exception {
				return event.getTemperature() > TEMPERATURE_THRESHOLD;
			}
		};

		Pattern<MonitoringEvent, ?> warningPattern = Pattern.<MonitoringEvent>begin("first")
			.subtype(TemperatureEvent.class)
			.where(thresholdFilterFunction)
			.next("second")
			.subtype(TemperatureEvent.class)
			.where(thresholdFilterFunction)
			.within(Time.seconds(10));

		// Create a pattern stream from our warning pattern
		PatternStream<MonitoringEvent> tempPatternStream = CEP.pattern(
			inputEventStream.keyBy("rackID"),
			warningPattern);

		// Generate temperature warnings for each matched warning pattern
		DataStream<TemperatureWarning> warnings = tempPatternStream.select(
			new TemperatureWarningPatternSelectFunction()
		);

		// Alert pattern: Two consecutive temperature warnings appearing within a time interval of 20 seconds
		Pattern<TemperatureWarning, ?> alertPattern = Pattern.<TemperatureWarning>begin("first")
			.next("second")
			.within(Time.seconds(20));

		// Create a pattern stream from our alert pattern
		PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
			warnings.keyBy("rackID"),
			alertPattern);

		// Generate a temperature alert only iff the second temperature warning's average temperature is higher than
		// first warning's temperature
		DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(
			new TemperatureAlertPatternFlatSelectFunction()
		);

		// Print the warning and alert events to stdout
		warnings.print();
		alerts.print();

		env.execute("CEP monitoring job");
	}
}
