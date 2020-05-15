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

package org.apache.flink.streaming.examples.cep;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.cep.util.CepData;
import org.apache.flink.streaming.examples.cep.util.Event;
import org.apache.flink.streaming.examples.cep.util.SubEvent;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;


/**
 * A Cep Example that teaches you how to detect event patterns in an endless stream of events
 */
public class CepExample {

	public static void main(String[] args) throws Exception {

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Event> input = env.fromElements(CepData.CEP_EVENTS).setParallelism(1);

		// define complex pattern sequences that you want to extract from your input stream.
		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
			new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event cepEvent) {
					return cepEvent.getId() == 42;
				}
			}
		).next("middle").subtype(SubEvent.class).where(
			new SimpleCondition<SubEvent>() {
				@Override
				public boolean filter(SubEvent subCepEvent) {
					return subCepEvent.getVolume() >= 10.0;
				}
			}
		).followedBy("end").where(
			new SimpleCondition<Event>() {
				@Override
				public boolean filter(Event cepEvent) {
					return cepEvent.getName().equals("end");
				}
			}
		);

		// apply pattern to your input stream to detect potential matches and create a PatternStream
		PatternStream<Event> patternStream = CEP.pattern(input, pattern);

		// it is the recommended to interact with matches by PatternProcessFunction introduced in Flink 1.8,
		// you can still use the old style API like select/flatSelect
		DataStream<String> result = patternStream.process(
			new PatternProcessFunction<Event, String>() {
				@Override
				public void processMatch(
					Map<String, List<Event>> pattern,
					Context ctx,
					Collector<String> out) {
					out.collect(pattern.toString());
				}
			});

		// emit result
		result.print();
		// execute program
		env.execute("Flink CEP Example");
	}

}
