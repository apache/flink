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

package org.apache.flink.cep;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for lambda support in CEP.
 */
public class CEPLambdaTest extends TestLogger {
	/**
	 * Test event class.
	 */
	public static class EventA {}

	/**
	 * Test event class.
	 */
	public static class EventB {}

	/**
	 * Tests that a Java8 lambda can be passed as a CEP select function.
	 */
	@Test
	public void testLambdaSelectFunction() {
		TypeInformation<EventA> eventTypeInformation = TypeExtractor.getForClass(EventA.class);
		TypeInformation<EventB> outputTypeInformation = TypeExtractor.getForClass(EventB.class);

		DataStream<EventA> inputStream = new DataStream<>(
			StreamExecutionEnvironment.getExecutionEnvironment(),
			new SourceTransformation<>(
				"source",
				null,
				eventTypeInformation,
				1));

		Pattern<EventA, ?> dummyPattern = Pattern.begin("start");

		PatternStream<EventA> patternStream = new PatternStream<>(inputStream, dummyPattern);

		DataStream<EventB> result = patternStream.select(
				(Map<String, List<EventA>> map) -> new EventB()
		);

		assertEquals(outputTypeInformation, result.getType());
	}

	/**
	 * Tests that a Java8 lambda can be passed as a CEP flat select function.
	 */
	@Test
	public void testLambdaFlatSelectFunction() {
		TypeInformation<EventA> eventTypeInformation = TypeExtractor.getForClass(EventA.class);
		TypeInformation<EventB> outputTypeInformation = TypeExtractor.getForClass(EventB.class);

		DataStream<EventA> inputStream = new DataStream<>(
			StreamExecutionEnvironment.getExecutionEnvironment(),
			new SourceTransformation<>(
				"source",
				null,
				eventTypeInformation,
				1));

		Pattern<EventA, ?> dummyPattern = Pattern.begin("start");

		PatternStream<EventA> patternStream = new PatternStream<>(inputStream, dummyPattern);

		DataStream<EventB> result = patternStream.flatSelect(
			(Map<String, List<EventA>> map, Collector<EventB> collector) -> collector.collect(new EventB())
		);

		assertEquals(outputTypeInformation, result.getType());
	}
}
