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

package org.apache.flink.cep.nfa;

import org.apache.flink.cep.Event;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Base method for IT tests of {@link NFA}. It provides utility methods.
 */
public class NFATestUtilities {

	public static List<List<Event>> feedNFA(
		List<StreamRecord<Event>> inputEvents,
		NFA<Event> nfa) throws Exception {
		return feedNFA(inputEvents, nfa, nfa.createInitialNFAState(), AfterMatchSkipStrategy.noSkip());
	}

	public static List<List<Event>> feedNFA(
			List<StreamRecord<Event>> inputEvents,
			NFA<Event> nfa,
			NFAState nfaState) throws Exception {
		return feedNFA(inputEvents, nfa, nfaState, AfterMatchSkipStrategy.noSkip());
	}

	public static List<List<Event>> feedNFA(
		List<StreamRecord<Event>> inputEvents,
		NFA<Event> nfa,
		AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {
		return feedNFA(inputEvents, nfa, nfa.createInitialNFAState(), afterMatchSkipStrategy);
	}

	public static List<List<Event>> feedNFA(
			List<StreamRecord<Event>> inputEvents,
			NFA<Event> nfa,
			NFAState nfaState,
			AfterMatchSkipStrategy afterMatchSkipStrategy) throws Exception {
		List<List<Event>> resultingPatterns = new ArrayList<>();

		SharedBuffer<Event> sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
		for (StreamRecord<Event> inputEvent : inputEvents) {
			nfa.advanceTime(sharedBuffer, nfaState, inputEvent.getTimestamp());
			Collection<Map<String, List<Event>>> patterns = nfa.process(
				sharedBuffer,
				nfaState,
				inputEvent.getValue(),
				inputEvent.getTimestamp(),
				afterMatchSkipStrategy);

			for (Map<String, List<Event>> p: patterns) {
				List<Event> res = new ArrayList<>();
				for (List<Event> le: p.values()) {
					res.addAll(le);
				}
				resultingPatterns.add(res);
			}
		}
		return resultingPatterns;
	}

	public static void compareMaps(List<List<Event>> actual, List<List<Event>> expected) {
		Assert.assertEquals(expected.size(), actual.size());

		for (List<Event> p: actual) {
			Collections.sort(p, new EventComparator());
		}

		for (List<Event> p: expected) {
			Collections.sort(p, new EventComparator());
		}

		Collections.sort(actual, new ListEventComparator());
		Collections.sort(expected, new ListEventComparator());
		Assert.assertArrayEquals(expected.toArray(), actual.toArray());
	}

	private static class ListEventComparator implements Comparator<List<Event>> {

		@Override
		public int compare(List<Event> o1, List<Event> o2) {
			int sizeComp = Integer.compare(o1.size(), o2.size());
			if (sizeComp == 0) {
				EventComparator comp = new EventComparator();
				for (int i = 0; i < o1.size(); i++) {
					int eventComp = comp.compare(o1.get(i), o2.get(i));
					if (eventComp != 0) {
						return eventComp;
					}
				}
				return 0;
			} else {
				return sizeComp;
			}
		}
	}

	private static class EventComparator implements Comparator<Event> {

		@Override
		public int compare(Event o1, Event o2) {
			int nameComp = o1.getName().compareTo(o2.getName());
			int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
			int idComp = Integer.compare(o1.getId(), o2.getId());
			if (nameComp == 0) {
				if (priceComp == 0) {
					return idComp;
				} else {
					return priceComp;
				}
			} else {
				return nameComp;
			}
		}
	}

	private NFATestUtilities() {
	}
}
