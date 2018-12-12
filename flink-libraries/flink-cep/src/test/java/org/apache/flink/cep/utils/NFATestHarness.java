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

package org.apache.flink.cep.utils;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Test harness for setting up {@link NFA}.
 */
public final class NFATestHarness {

	private final SharedBuffer<Event> sharedBuffer;
	private final NFA<Event> nfa;
	private final NFAState nfaState;
	private final AfterMatchSkipStrategy afterMatchSkipStrategy;
	private final TimerService timerService;

	private NFATestHarness(
			SharedBuffer<Event> sharedBuffer,
			NFA<Event> nfa,
			NFAState nfaState,
			AfterMatchSkipStrategy afterMatchSkipStrategy,
			TimerService timerService) {
		this.sharedBuffer = sharedBuffer;
		this.nfa = nfa;
		this.nfaState = nfaState;
		this.afterMatchSkipStrategy = afterMatchSkipStrategy;
		this.timerService = timerService;
	}

	/**
	 * Constructs a test harness starting from a given {@link Pattern}.
	 */
	public static NFATestHarnessBuilderPattern forPattern(Pattern<Event, ?> pattern) {
		return new NFATestHarnessBuilderPattern(pattern);
	}

	/**
	 * Constructs a test harness starting from a given {@link NFA}.
	 */
	public static NFATestHarnessBuilderNFA forNFA(NFA<Event> nfa) {
		return new NFATestHarnessBuilderNFA(nfa);
	}

	public List<List<Event>> feedRecords(List<StreamRecord<Event>> inputEvents) throws Exception {
		final List<List<Event>> resultingPatterns = new ArrayList<>();
		for (StreamRecord<Event> inputEvent : inputEvents) {
			resultingPatterns.addAll(feedRecord(inputEvent));
		}
		return resultingPatterns;
	}

	public List<List<Event>> feedRecord(StreamRecord<Event> inputEvent) throws Exception {
		final List<List<Event>> resultingPatterns = new ArrayList<>();
		final Collection<Map<String, List<Event>>> matches = consumeRecord(inputEvent);
		for (Map<String, List<Event>> p : matches) {
			List<Event> res = new ArrayList<>();
			for (List<Event> le : p.values()) {
				res.addAll(le);
			}
			resultingPatterns.add(res);
		}
		return resultingPatterns;
	}

	public Collection<Map<String, List<Event>>> consumeRecords(Collection<StreamRecord<Event>> inputEvents) throws Exception {
		final List<Map<String, List<Event>>> resultingPatterns = new ArrayList<>();
		for (StreamRecord<Event> inputEvent : inputEvents) {
			resultingPatterns.addAll(consumeRecord(inputEvent));
		}

		return resultingPatterns;
	}

	public Collection<Map<String, List<Event>>> consumeRecord(StreamRecord<Event> inputEvent) throws Exception {
		try (SharedBufferAccessor<Event> sharedBufferAccessor = sharedBuffer.getAccessor()) {
			nfa.advanceTime(sharedBufferAccessor, nfaState, inputEvent.getTimestamp());
			return nfa.process(
				sharedBufferAccessor,
				nfaState,
				inputEvent.getValue(),
				inputEvent.getTimestamp(),
				afterMatchSkipStrategy,
				timerService);
		}
	}

	/**
	 * Builder for {@link NFATestHarness} that encapsulates {@link Pattern}.
	 */
	public static class NFATestHarnessBuilderPattern extends NFATestHarnessBuilderBase {

		private final Pattern<Event, ?> pattern;
		private boolean timeoutHandling = false;

		NFATestHarnessBuilderPattern(Pattern<Event, ?> pattern) {
			super(pattern.getAfterMatchSkipStrategy());
			this.pattern = pattern;
		}

		public NFATestHarnessBuilderBase withTimeoutHandling() {
			this.timeoutHandling = true;
			return this;
		}

		@Override
		public NFATestHarness build() {
			final NFA<Event> nfa = NFAUtils.compile(pattern, timeoutHandling);
			return new NFATestHarness(
				sharedBuffer,
				nfa,
				nfa.createInitialNFAState(),
				afterMatchSkipStrategy,
				timerService);
		}
	}

	/**
	 * Builder for {@link NFATestHarness} that encapsulates {@link NFA}.
	 */
	public static class NFATestHarnessBuilderNFA extends NFATestHarnessBuilderBase {

		private final NFA<Event> nfa;
		private NFAState nfaState;

		NFATestHarnessBuilderNFA(NFA<Event> nfa) {
			super(AfterMatchSkipStrategy.noSkip());
			this.nfa = nfa;
			this.nfaState = nfa.createInitialNFAState();
		}

		public NFATestHarnessBuilderBase withNFAState(NFAState nfaState) {
			this.nfaState = nfaState;
			return this;
		}

		@Override
		public NFATestHarness build() {
			return new NFATestHarness(
				sharedBuffer,
				nfa,
				nfaState,
				afterMatchSkipStrategy,
				timerService);
		}
	}

	/**
	 * Common builder, which can be used independent if we start with {@link Pattern} or {@link NFA}.
	 * Enables to provide custom services like {@link SharedBuffer} etc.
	 */
	public abstract static class NFATestHarnessBuilderBase {

		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		AfterMatchSkipStrategy afterMatchSkipStrategy;
		TimerService timerService = new TestTimerService();

		NFATestHarnessBuilderBase(AfterMatchSkipStrategy skipStrategy) {
			this.afterMatchSkipStrategy = skipStrategy;
		}

		public NFATestHarnessBuilderBase withSharedBuffer(SharedBuffer<Event> sharedBuffer) {
			this.sharedBuffer = sharedBuffer;
			return this;
		}

		public NFATestHarnessBuilderBase withAfterMatchSkipStrategy(AfterMatchSkipStrategy afterMatchSkipStrategy) {
			this.afterMatchSkipStrategy = afterMatchSkipStrategy;
			return this;
		}

		public NFATestHarnessBuilderBase withTimerService(TimerService timerService) {
			this.timerService = timerService;
			return this;
		}

		public abstract NFATestHarness build();
	}
}
