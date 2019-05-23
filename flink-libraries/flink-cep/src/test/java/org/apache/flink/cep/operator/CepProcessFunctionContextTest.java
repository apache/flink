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

package org.apache.flink.cep.operator;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.cep.operator.CepOperatorTestUtilities.getCepTestHarness;
import static org.apache.flink.cep.utils.EventBuilder.event;
import static org.apache.flink.cep.utils.OutputAsserter.assertOutput;

/**
 * Tests for {@link CepOperator} which check proper setting {@link PatternProcessFunction.Context}.
 */
public class CepProcessFunctionContextTest extends TestLogger {

	private static final boolean PROCESSING_TIME = true;
	private static final boolean EVENT_TIME = false;

	@Test
	public void testTimestampPassingInEventTime() throws Exception {

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractTimestampAndNames(1),
					new NFAForwardingFactory(),
					EVENT_TIME))) {
			harness.open();

			// events out of order to test if internal sorting does not mess up the timestamps
			harness.processElement(event().withName("A").withTimestamp(5).asStreamRecord());
			harness.processElement(event().withName("B").withTimestamp(3).asStreamRecord());

			harness.processWatermark(6);

			assertOutput(harness.getOutput())
				.nextElementEquals("3:B")
				.nextElementEquals("5:A")
				.watermarkEquals(6L)
				.hasNoMoreElements();
		}
	}

	@Test
	public void testTimestampPassingInProcessingTime() throws Exception {

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractTimestampAndNames(1),
					new NFAForwardingFactory(),
					PROCESSING_TIME))) {
			harness.open();

			harness.setProcessingTime(1);
			harness.processElement(event().withName("A").withTimestamp(5).asStreamRecord());
			harness.setProcessingTime(2);
			harness.processElement(event().withName("B").withTimestamp(3).asStreamRecord());
			harness.setProcessingTime(3);

			assertOutput(harness.getOutput())
				.nextElementEquals("1:A")
				.nextElementEquals("2:B")
				.hasNoMoreElements();
		}
	}

	@Test
	public void testCurrentProcessingTimeInProcessingTime() throws Exception {

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractCurrentProcessingTimeAndNames(1),
					new NFAForwardingFactory(),
					PROCESSING_TIME))) {
			harness.open();

			harness.setProcessingTime(15);
			harness.processElement(event().withName("A").asStreamRecord());
			harness.setProcessingTime(35);
			harness.processElement(event().withName("B").asStreamRecord());

			assertOutput(harness.getOutput())
				.nextElementEquals("15:A")
				.nextElementEquals("35:B")
				.hasNoMoreElements();
		}
	}

	@Test
	public void testCurrentProcessingTimeInEventTime() throws Exception {

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractCurrentProcessingTimeAndNames(1),
					new NFAForwardingFactory(),
					EVENT_TIME))) {
			harness.open();

			harness.setProcessingTime(10);
			harness.processElement(event().withName("A").withTimestamp(5).asStreamRecord());
			harness.setProcessingTime(100);
			harness.processWatermark(6);

			assertOutput(harness.getOutput())
				.nextElementEquals("100:A")
				.watermarkEquals(6)
				.hasNoMoreElements();
		}
	}

	@Test
	public void testTimestampPassingForTimedOutInEventTime() throws Exception {

		OutputTag<String> timedOut = new OutputTag<String>("timedOut") {};

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractTimestampAndNames(2, timedOut),
					new NFATimingOutFactory(),
					EVENT_TIME))) {
			harness.open();

			// events out of order to test if internal sorting does not mess up the timestamps
			harness.processElement(event().withName("A").withTimestamp(5).asStreamRecord());
			harness.processElement(event().withName("C").withTimestamp(20).asStreamRecord());
			harness.processElement(event().withName("B").withTimestamp(3).asStreamRecord());

			harness.processWatermark(22);

			assertOutput(harness.getOutput())
				.nextElementEquals("5:B:A")
				.watermarkEquals(22)
				.hasNoMoreElements();

			assertOutput(harness.getSideOutput(timedOut))
				.nextElementEquals("15:A")
				.hasNoMoreElements();
		}
	}

	@Test
	public void testTimestampPassingForTimedOutInProcessingTime() throws Exception {

		OutputTag<String> timedOut = new OutputTag<String>("timedOut") {};

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractTimestampAndNames(2, timedOut),
					new NFATimingOutFactory(),
					PROCESSING_TIME))) {
			harness.open();

			harness.setProcessingTime(3);
			harness.processElement(event().withName("A").withTimestamp(3).asStreamRecord());
			harness.setProcessingTime(5);
			harness.processElement(event().withName("C").withTimestamp(5).asStreamRecord());
			harness.setProcessingTime(20);
			harness.processElement(event().withName("B").withTimestamp(20).asStreamRecord());

			assertOutput(harness.getOutput())
				.nextElementEquals("5:A:C")
				.hasNoMoreElements();

			assertOutput(harness.getSideOutput(timedOut))
				.nextElementEquals("15:C")
				.hasNoMoreElements();
		}
	}

	@Test
	public void testCurrentProcessingTimeForTimedOutInEventTime() throws Exception {

		OutputTag<String> sideOutputTag = new OutputTag<String>("timedOut") {};

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractCurrentProcessingTimeAndNames(2, sideOutputTag),
					new NFATimingOutFactory(),
					EVENT_TIME))) {
			harness.open();

			// events out of order to test if internal sorting does not mess up the timestamps
			harness.processElement(event().withName("A").withTimestamp(5).asStreamRecord());
			harness.processElement(event().withName("B").withTimestamp(20).asStreamRecord());
			harness.processElement(event().withName("C").withTimestamp(3).asStreamRecord());

			harness.setProcessingTime(100);
			harness.processWatermark(22);

			assertOutput(harness.getOutput())
				.nextElementEquals("100:C:A")
				.watermarkEquals(22)
				.hasNoMoreElements();

			assertOutput(harness.getSideOutput(sideOutputTag))
				.nextElementEquals("100:A")
				.hasNoMoreElements();
		}
	}

	@Test
	public void testCurrentProcessingTimeForTimedOutInProcessingTime() throws Exception {

		OutputTag<String> sideOutputTag = new OutputTag<String>("timedOut") {};

		try (
			OneInputStreamOperatorTestHarness<Event, String> harness = getCepTestHarness(
				createCepOperator(
					extractCurrentProcessingTimeAndNames(2, sideOutputTag),
					new NFATimingOutFactory(),
					PROCESSING_TIME))) {
			harness.open();

			harness.setProcessingTime(3);
			harness.processElement(event().withName("A").asStreamRecord());
			harness.setProcessingTime(5);
			harness.processElement(event().withName("B").asStreamRecord());
			harness.setProcessingTime(20);
			harness.processElement(event().withName("C").asStreamRecord());

			assertOutput(harness.getOutput())
				.nextElementEquals("5:A:B")
				.hasNoMoreElements();

			// right now we time out only on next event in processing time, therefore the 20
			assertOutput(harness.getSideOutput(sideOutputTag))
				.nextElementEquals("20:B")
				.hasNoMoreElements();
		}
	}

	/* TEST UTILS */

	private <T> CepOperator<Event, Integer, T> createCepOperator(
			PatternProcessFunction<Event, T> processFunction,
			NFACompiler.NFAFactory<Event> nfaFactory,
			boolean isProcessingTime) throws Exception {
		return new CepOperator<>(
			Event.createTypeSerializer(),
			isProcessingTime,
			nfaFactory,
			null,
			null,
			processFunction,
			null);
	}

	/**
	 * Creates a {@link PatternProcessFunction} that as a result will produce Strings as follows:
	 * <pre>[timestamp]:[Event.getName]...</pre> The Event.getName will occur stateNumber times. If the match does not
	 * contain n-th pattern it will replace this position with "null".
	 *
	 * @param stateNumber number of states in the pattern
	 * @return created PatternProcessFunction
	 */
	private static PatternProcessFunction<Event, String> extractTimestampAndNames(int stateNumber) {
		return new AccessContextWithNames(stateNumber,
			context -> String.valueOf(context.timestamp()));
	}

	/**
	 * Creates a {@link PatternProcessFunction} that as a result will produce Strings as follows:
	 * <pre>[timestamp]:[Event.getName]...</pre> The Event.getName will occur stateNumber times. If the match does not
	 * contain n-th pattern it will replace this position with "null".
	 *
	 * <p>This function will also apply the same logic for timed out partial matches and emit those results into
	 * side output described with given output tag.
	 *
	 * @param stateNumber number of states in the pattern
	 * @param timedOutTag output tag where to emit timed out partial matches
	 * @return created PatternProcessFunction
	 */
	private static PatternProcessFunction<Event, String> extractTimestampAndNames(
			int stateNumber,
			OutputTag<String> timedOutTag) {
		return new AccessContextWithNamesWithTimedOut(stateNumber,
			timedOutTag,
			context -> String.valueOf(context.timestamp()));
	}

	/**
	 * Creates a {@link PatternProcessFunction} that as a result will produce Strings as follows:
	 * <pre>[currentProcessingTime]:[Event.getName]...</pre> The Event.getName will occur stateNumber times.
	 * If the match does not contain n-th pattern it will replace this position with "null".
	 *
	 * @param stateNumber number of states in the pattern
	 * @return created PatternProcessFunction
	 */
	private static PatternProcessFunction<Event, String> extractCurrentProcessingTimeAndNames(int stateNumber) {
		return new AccessContextWithNames(stateNumber, context -> String.valueOf(context.currentProcessingTime()));
	}

	/**
	 * Creates a {@link PatternProcessFunction} that as a result will produce Strings as follows:
	 * <pre>[currentProcessingTime]:[Event.getName]...</pre> The Event.getName will occur stateNumber times.
	 * If the match does not contain n-th pattern it will replace this position with "null".
	 *
	 * <p>This function will also apply the same logic for timed out partial matches and emit those results into
	 * side output described with given output tag.
	 *
	 * @param stateNumber number of states in the pattern
	 * @param timedOutTag output tag where to emit timed out partial matches
	 * @return created PatternProcessFunction
	 */
	private static PatternProcessFunction<Event, String> extractCurrentProcessingTimeAndNames(
			int stateNumber,
			OutputTag<String> timedOutTag) {
		return new AccessContextWithNamesWithTimedOut(stateNumber,
			timedOutTag,
			context -> String.valueOf(context.currentProcessingTime()));
	}

	private static final String NO_TIMESTAMP = "(NO_TIMESTAMP)";

	static class AccessContextWithNames extends PatternProcessFunction<Event, String> {

		private final int stateCount;
		private final Function<Context, String> contextAccessor;

		AccessContextWithNames(int stateCount, Function<Context, String> contextAccessor) {
			this.stateCount = stateCount;
			this.contextAccessor = contextAccessor;
		}

		@Override
		public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<String> out) throws Exception {
			out.collect(extractResult(match, ctx));
		}

		String extractResult(Map<String, List<Event>> match, Context ctx) {
			StringBuilder stringBuilder = new StringBuilder(contextAccessor.apply(ctx));
			for (int i = 1; i <= stateCount; i++) {
				List<Event> events = match.get("" + i);
				if (events != null) {
					stringBuilder.append(":").append(events.get(0).getName());
				}
			}
			return stringBuilder.toString();
		}
	}

	static final class AccessContextWithNamesWithTimedOut extends AccessContextWithNames
		implements TimedOutPartialMatchHandler<Event> {

		private OutputTag<String> outputTag;

		AccessContextWithNamesWithTimedOut(
				int stateCount,
				OutputTag<String> outputTag,
				Function<Context, String> contextAccessor) {
			super(stateCount, contextAccessor);
			this.outputTag = outputTag;
		}

		@Override
		public void processTimedOutMatch(Map<String, List<Event>> match, Context ctx) throws Exception {
			ctx.output(outputTag, extractResult(match, ctx));
		}
	}

	/**
	 * This NFA consists of one state accepting any element.
	 */
	private static class NFAForwardingFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.begin("1");

			return NFACompiler.compileFactory(pattern, false).createNFA();
		}
	}

	/**
	 * This NFA consists of two states accepting any element. It times out after 10 milliseconds
	 */
	private static class NFATimingOutFactory implements NFACompiler.NFAFactory<Event> {

		private static final long serialVersionUID = 1173020762472766713L;

		@Override
		public NFA<Event> createNFA() {

			Pattern<Event, ?> pattern = Pattern.<Event>begin("1").next("2").within(Time.milliseconds(10));

			return NFACompiler.compileFactory(pattern, true).createNFA();
		}
	}

}
