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

package org.apache.flink.cep.examples.java.monitoring;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.examples.java.monitoring.events.MonitoringEvent;
import org.apache.flink.cep.examples.java.monitoring.events.TemperatureAlert;
import org.apache.flink.cep.examples.java.monitoring.events.TemperatureEvent;
import org.apache.flink.cep.examples.java.monitoring.events.TemperatureWarning;
import org.apache.flink.cep.examples.java.monitoring.sources.MonitoringEventSource;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * CEP example monitoring program.
 * This example program generates a stream of monitoring events which are analyzed using
 * Flink's CEP library. The input event stream consists of temperature and power events
 * from a set of racks. The goal is to detect when a rack is about to overheat.
 * In order to do that, we create a CEP pattern which generates a TemperatureWarning
 * whenever it sees two consecutive temperature events in a given time interval whose temperatures
 * are higher than a given threshold value. A warning itself is not critical but if we see
 * two warning for the same rack whose temperatures are rising, we want to generate an alert.
 * This is achieved by defining another CEP pattern which analyzes the stream of generated
 * temperature warnings.
 */
public class TemperatureMonitoring {

	private static final double TEMPERATURE_THRESHOLD = 100;

	public static void main(String[] args) throws Exception {
		System.out.println("Executing temperature monitoring Java example.");
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Input stream of monitoring events
		DataStream<MonitoringEvent> inputEventStream = env.addSource(new MonitoringEventSource())
			.assignTimestampsAndWatermarks(new IngestionTimeExtractor<>());

		// Warning pattern: Two consecutive temperature events whose temperature is higher
		// than the given threshold appearing within a time interval of 10 seconds
		Pattern<MonitoringEvent, ?> warningPattern = Pattern
			.<MonitoringEvent>begin("first")
				.subtype(TemperatureEvent.class)
				.where(new SimpleCondition<TemperatureEvent>() {
					@Override
					public boolean filter(TemperatureEvent event) throws Exception {
						return event.getTemperature() > TEMPERATURE_THRESHOLD;
					}
				})
			.next("second")
				.subtype(TemperatureEvent.class)
				.where(new SimpleCondition<TemperatureEvent>() {
					@Override
					public boolean filter(TemperatureEvent event) throws Exception {
						return event.getTemperature() > TEMPERATURE_THRESHOLD;
					}
				})
			.within(Time.seconds(10));

		// Create a pattern stream from our warning pattern
		PatternStream<MonitoringEvent> tempPatternStream = CEP.pattern(
			inputEventStream.keyBy("rackID"),
			warningPattern);

		// Generate temperature warnings for each matched warning pattern
		DataStream<TemperatureWarning> warnings = tempPatternStream.select(
			new PatternSelectFunction<MonitoringEvent, TemperatureWarning>() {
				@Override
				public TemperatureWarning select(
						Map<String, List<MonitoringEvent>> pattern) throws Exception {
					TemperatureEvent first = (TemperatureEvent) pattern.get("first").get(0);
					TemperatureEvent second = (TemperatureEvent) pattern.get("second").get(0);
					return new TemperatureWarning(
						first.getRackID(),
						(first.getTemperature() + second.getTemperature()) / 2
					);
				}
			}
		);

		// Alert pattern: Two consecutive temperature warnings
		// appearing within a time interval of 20 seconds
		Pattern<TemperatureWarning, ?> alertPattern = Pattern
			.<TemperatureWarning>begin("first")
			.next("second")
			.within(Time.seconds(20));

		// Create a pattern stream from our alert pattern
		PatternStream<TemperatureWarning> alertPatternStream = CEP.pattern(
			warnings.keyBy("rackID"),
			alertPattern);

		// Generate a temperature alert iff the second temperature warning's average temperature
		// is higher than first warning's temperature
		DataStream<TemperatureAlert> alerts = alertPatternStream.flatSelect(
			new PatternFlatSelectFunction<TemperatureWarning, TemperatureAlert>() {
				@Override
				public void flatSelect(Map<String, List<TemperatureWarning>> pattern,
						Collector<TemperatureAlert> out) throws Exception {
					TemperatureWarning first = pattern.get("first").get(0);
					TemperatureWarning second = pattern.get("second").get(0);
					if (first.getAverageTemperature() < second.getAverageTemperature()) {
						out.collect(new TemperatureAlert(first.getRackID(), second.getDatetime()));
					}
				}
			}
		);

		// Print the warning and alert events to the stdout
		System.out.println("Printing result to the stdout of task executors.");
		warnings.print();
		alerts.print();

		env.execute("CEP Temperature Monitoring Java Example");
	}

}
