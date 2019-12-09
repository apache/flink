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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.compareMaps;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link Pattern#notFollowedBy(String)} and {@link Pattern#notNext(String)}.
 */
@SuppressWarnings("unchecked")
public class NotPatternITCase extends TestLogger {

	@Test
	public void testNotNext() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5167288560432018992L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notNext("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2242479288129905510L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 1404509325548220892L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -8907427230007830915L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, c1, d),
			Lists.newArrayList(a1, c2, d)
		));
	}

	@Test
	public void testNotNextNoMatches() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c1 = new Event(41, "c", 2.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(b1, 2));
		inputEvents.add(new StreamRecord<>(c1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -339500190577666439L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notNext("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -6913980632538046451L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 3332196998905139891L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2086563479959018387L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		assertEquals(0, matches.size());
	}

	@Test
	public void testNotNextNoMatchesAtTheEnd() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);
		Event b1 = new Event(42, "b", 3.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(c2, 3));
		inputEvents.add(new StreamRecord<>(d, 4));
		inputEvents.add(new StreamRecord<>(b1, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 1672995058886176627L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6003621617520261554L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 887700237024758417L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		}).notNext("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5239529076086933032L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		assertEquals(0, matches.size());
	}

	@Test
	public void testNotFollowedBy() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -2641662468313191976L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -3632144132379494778L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 3818766882138348167L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2033204730795451288L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, c1, d)
		));
	}

	@Test
	public void testNotFollowedByBeforeOptional() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -2454396370205097543L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2749547391611263290L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -4989511337298217255L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).optional().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -8466223836652936608L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, c1, d)
		));
	}

	@Test
	public void testTimesWithNotFollowedBy() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event b1 = new Event(41, "b", 2.0);
		Event c = new Event(42, "c", 3.0);
		Event b2 = new Event(43, "b", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(b1, 2));
		inputEvents.add(new StreamRecord<>(c, 3));
		inputEvents.add(new StreamRecord<>(b2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -2568839911852184515L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -3632232424064269636L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).times(2).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 3685596793523534611L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 1960758663575587243L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList());
	}

	@Test
	public void testIgnoreStateOfTimesWithNotFollowedBy() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event e = new Event(41, "e", 2.0);
		Event c1 = new Event(42, "c", 3.0);
		Event b1 = new Event(43, "b", 4.0);
		Event c2 = new Event(44, "c", 5.0);
		Event d1 = new Event(45, "d", 6.0);
		Event d2 = new Event(46, "d", 7.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(d1, 2));
		inputEvents.add(new StreamRecord<>(e, 1));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d2, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2814850350025111940L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 4988756153568853834L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -225909103322018778L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).times(2).optional().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -924294627956373696L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, d1)
		));
	}

	@Test
	public void testTimesWithNotFollowedByAfter() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event e = new Event(41, "e", 2.0);
		Event c1 = new Event(42, "c", 3.0);
		Event b1 = new Event(43, "b", 4.0);
		Event b2 = new Event(44, "b", 5.0);
		Event d1 = new Event(46, "d", 7.0);
		Event d2 = new Event(47, "d", 8.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(d1, 2));
		inputEvents.add(new StreamRecord<>(e, 1));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(b2, 3));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(d2, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 6193105689601702341L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5195859580923169111L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).times(2).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 4973027956103783831L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 2724622546678984894L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList());
	}

	@Test
	public void testNotFollowedByBeforeOptionalAtTheEnd() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c2 = new Event(43, "c", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -4289351792573443294L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -4989574608417523507L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -5940131818629290579L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).optional();

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, c1),
			Lists.newArrayList(a1)
		));
	}

	@Test
	public void testNotFollowedByBeforeOptionalTimes() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event c1 = new Event(41, "c", 2.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c2 = new Event(43, "c", 4.0);
		Event d = new Event(43, "d", 4.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(c1, 2));
		inputEvents.add(new StreamRecord<>(b1, 3));
		inputEvents.add(new StreamRecord<>(c2, 4));
		inputEvents.add(new StreamRecord<>(d, 5));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -7885381452276160322L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 3471511260235826653L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedByAny("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 9073793782452363833L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).times(2).optional().followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 7972902718259767076L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a1, c1, c2, d)
		));
	}

	@Test
	public void testNotFollowedByWithBranchingAtStart() throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		Event a1 = new Event(40, "a", 1.0);
		Event b1 = new Event(42, "b", 3.0);
		Event c1 = new Event(41, "c", 2.0);
		Event a2 = new Event(41, "a", 4.0);
		Event c2 = new Event(43, "c", 5.0);
		Event d = new Event(43, "d", 6.0);

		inputEvents.add(new StreamRecord<>(a1, 1));
		inputEvents.add(new StreamRecord<>(b1, 2));
		inputEvents.add(new StreamRecord<>(c1, 3));
		inputEvents.add(new StreamRecord<>(a2, 4));
		inputEvents.add(new StreamRecord<>(c2, 5));
		inputEvents.add(new StreamRecord<>(d, 6));

		Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -7866220136345465444L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("a");
			}
		}).notFollowedBy("notPattern").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 4957837489028234932L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).followedBy("middle").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5569569968862808007L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("c");
			}
		}).followedBy("end").where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = -8579678167937416269L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("d");
			}
		});

		NFA<Event> nfa = compile(pattern, false);

		final List<List<Event>> matches = feedNFA(inputEvents, nfa);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(a2, c2, d)
		));
	}

	private static class NotFollowByData {
		static final Event A_1 = new Event(40, "a", 1.0);
		static final Event B_1 = new Event(41, "b", 2.0);
		static final Event B_2 = new Event(42, "b", 3.0);
		static final Event B_3 = new Event(42, "b", 4.0);
		static final Event C_1 = new Event(43, "c", 5.0);
		static final Event B_4 = new Event(42, "b", 6.0);
		static final Event B_5 = new Event(42, "b", 7.0);
		static final Event B_6 = new Event(42, "b", 8.0);
		static final Event D_1 = new Event(43, "d", 9.0);

		private NotFollowByData() {
		}
	}

	@Test
	public void testNotNextAfterOneOrMoreSkipTillNext() throws Exception {
		final List<List<Event>> matches = testNotNextAfterOneOrMore(false);
		assertEquals(0, matches.size());
	}

	@Test
	public void testNotNextAfterOneOrMoreSkipTillAny() throws Exception {
		final List<List<Event>> matches = testNotNextAfterOneOrMore(true);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_2, NotFollowByData.D_1)
		));
	}

	private List<List<Event>> testNotNextAfterOneOrMore(boolean allMatches) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		int i = 0;
		inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_2, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i++));

		Pattern<Event, ?> pattern = Pattern
			.<Event>begin("a").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			});

		pattern = (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*")).where(new SimpleCondition<Event>() {
			private static final long serialVersionUID = 5726188262756267490L;

			@Override
			public boolean filter(Event value) throws Exception {
				return value.getName().equals("b");
			}
		}).oneOrMore()
			.notNext("not c").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("c");
				}
			})
			.followedBy("d").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("d");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}

	@Test
	public void testNotFollowedByNextAfterOneOrMoreEager() throws Exception {
		final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(true, false);
		assertEquals(0, matches.size());
	}

	@Test
	public void testNotFollowedByAnyAfterOneOrMoreEager() throws Exception {
		final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(true, true);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_6, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByNextAfterOneOrMoreCombinations() throws Exception {
		final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(false, false);
		assertEquals(0, matches.size());
	}

	@Test
	public void testNotFollowedByAnyAfterOneOrMoreCombinations() throws Exception {
		final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(false, true);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_6, NotFollowByData.D_1)
		));
	}

	private List<List<Event>> testNotFollowedByAfterOneOrMore(boolean eager, boolean allMatches) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		int i = 0;
		inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_2, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_3, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_4, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_5, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_6, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i));

		Pattern<Event, ?> pattern = Pattern
			.<Event>begin("a").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			});

		pattern = (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
			.where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("b");
				}
			});

		pattern = (eager ? pattern.oneOrMore() : pattern.oneOrMore().allowCombinations())
			.notFollowedBy("not c").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("c");
				}
			})
			.followedBy("d").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("d");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}

	@Test
	public void testNotFollowedByAnyBeforeOneOrMoreEager() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(true, true);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByAnyBeforeOneOrMoreCombinations() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(false, true);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByBeforeOneOrMoreEager() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(true, false);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByBeforeOneOrMoreCombinations() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(false, false);

		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	private List<List<Event>> testNotFollowedByBeforeOneOrMore(boolean eager, boolean allMatches) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		int i = 0;
		inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_4, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_5, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_6, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i));

		Pattern<Event, ?> pattern = Pattern
			.<Event>begin("a").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			})
			.notFollowedBy("not c").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("c");
				}
			});

		pattern = (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
			.where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("b");
				}
			}).oneOrMore();

		pattern = (eager ? pattern : pattern.allowCombinations())
			.followedBy("d").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("d");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}

	@Test
	public void testNotFollowedByBeforeZeroOrMoreEagerSkipTillNext() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(true, false);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillNext() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(false, false);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_6, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByBeforeZeroOrMoreEagerSkipTillAny() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(true, true);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)
		));
	}

	@Test
	public void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillAny() throws Exception {
		final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(false, true);
		compareMaps(matches, Lists.<List<Event>>newArrayList(
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_4, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.B_6, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_5, NotFollowByData.D_1),
			Lists.newArrayList(NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.B_6, NotFollowByData.D_1)
		));
	}

	private List<List<Event>> testNotFollowedByBeforeZeroOrMore(boolean eager, boolean allMatches) throws Exception {
		List<StreamRecord<Event>> inputEvents = new ArrayList<>();

		int i = 0;
		inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_4, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_5, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.B_6, i++));
		inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i));

		Pattern<Event, ?> pattern = Pattern
			.<Event>begin("a").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("a");
				}
			})
			.notFollowedBy("not c").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("c");
				}
			});

		pattern = (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
			.where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("b");
				}
			}).oneOrMore().optional();

		pattern = (eager ? pattern : pattern.allowCombinations())
			.followedBy("d").where(new SimpleCondition<Event>() {
				private static final long serialVersionUID = 5726188262756267490L;

				@Override
				public boolean filter(Event value) throws Exception {
					return value.getName().equals("d");
				}
			});

		NFA<Event> nfa = compile(pattern, false);

		return feedNFA(inputEvents, nfa);
	}
}
