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
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/**
 * Tests for handling Events that are equal in case of {@link Object#equals(Object)} and have same
 * timestamps.
 */
@SuppressWarnings("unchecked")
public class SameElementITCase extends TestLogger {

    @Test
    public void testEagerZeroOrMoreSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(end1, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedBy("middle")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .followedBy("end1")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("b");
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent3,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1,
                                middleEvent1,
                                middleEvent2,
                                end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent1, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, end1)));
    }

    @Test
    public void testClearingBuffer() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(40, "a", 1.0);
        Event b1 = new Event(41, "b", 2.0);
        Event c1 = new Event(41, "c", 2.0);
        Event d = new Event(41, "d", 2.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(b1, 2));
        inputEvents.add(new StreamRecord<>(c1, 2));
        inputEvents.add(new StreamRecord<>(d, 2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .followedBy("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("b");
                                    }
                                })
                        .followedBy("c")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedBy("d")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("d");
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);
        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, b1, c1, d)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("a", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testClearingBufferWithUntilAtTheEnd() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(40, "a", 1.0);
        Event d1 = new Event(41, "d", 2.0);
        Event d2 = new Event(41, "d", 2.0);
        Event d3 = new Event(41, "d", 2.0);
        Event d4 = new Event(41, "d", 2.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(d1, 2));
        inputEvents.add(new StreamRecord<>(d2, 2));
        inputEvents.add(new StreamRecord<>(d3, 2));
        inputEvents.add(new StreamRecord<>(d4, 4));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .followedBy("d")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("d");
                                    }
                                })
                        .oneOrMore()
                        .until(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return Iterators.size(
                                                        ctx.getEventsForPattern("d").iterator())
                                                == 3;
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);
        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, d1, d2, d3),
                        Lists.newArrayList(a1, d1, d2),
                        Lists.newArrayList(a1, d1)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("a", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testZeroOrMoreSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent1a = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event middleEvent3a = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1a, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(middleEvent3a, 6));
        inputEvents.add(new StreamRecord<>(end1, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedByAny("middle")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .followedByAny("end1")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("b");
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1a,
                                middleEvent2,
                                middleEvent3,
                                middleEvent3a,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1a,
                                middleEvent2,
                                middleEvent3,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1a,
                                middleEvent2,
                                middleEvent3a,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent1a,
                                middleEvent3,
                                middleEvent3a,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent3a,
                                end1),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1a,
                                middleEvent2,
                                middleEvent3,
                                middleEvent3a,
                                end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent1a, middleEvent2, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent1a, middleEvent3, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent1a, middleEvent3a, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3a, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent3, middleEvent3a, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent2, middleEvent3, middleEvent3a, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1a, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1a, middleEvent2, middleEvent3a, end1),
                        Lists.newArrayList(
                                startEvent, middleEvent1a, middleEvent3, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, middleEvent1a, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1a, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent1a, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent2, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, middleEvent3, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent1a, end1),
                        Lists.newArrayList(startEvent, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent3a, end1),
                        Lists.newArrayList(startEvent, end1)));
    }

    @Test
    public void testSimplePatternWSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(end1, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedByAny("middle")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .followedBy("end1")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("b");
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1)));
    }

    @Test
    public void testIterativeConditionWSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent1a = new Event(41, "a", 2.0);
        Event middleEvent1b = new Event(41, "a", 2.0);
        final Event end = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1a, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1b, 3));
        inputEvents.add(new StreamRecord<>(end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedByAny("middle")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .followedBy("end")
                        .where(
                                new IterativeCondition<Event>() {

                                    private static final long serialVersionUID =
                                            -5566639743229703237L;

                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        double sum = 0.0;
                                        for (Event event : ctx.getEventsForPattern("middle")) {
                                            sum += event.getPrice();
                                        }
                                        return Double.compare(sum, 4.0) == 0;
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent1a, end),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent1a, middleEvent1b),
                        Lists.newArrayList(startEvent, middleEvent1a, middleEvent1b, end)));
    }

    @Test
    public void testEndWLoopingWSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent1a = new Event(41, "a", 2.0);
        Event middleEvent1b = new Event(41, "a", 2.0);
        final Event end = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1a, 3));
        inputEvents.add(new StreamRecord<>(middleEvent1b, 3));
        inputEvents.add(new StreamRecord<>(end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedByAny("middle")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .oneOrMore()
                        .optional();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent),
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent, middleEvent1a),
                        Lists.newArrayList(startEvent, middleEvent1b),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent1a),
                        Lists.newArrayList(startEvent, middleEvent1a, middleEvent1b),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent1a, middleEvent1b)));
    }

    @Test
    public void testRepeatingPatternWSameElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middle1Event1 = new Event(40, "a", 2.0);
        Event middle1Event2 = new Event(40, "a", 3.0);
        Event middle1Event3 = new Event(40, "a", 4.0);
        Event middle2Event1 = new Event(40, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middle1Event1, 3));
        inputEvents.add(new StreamRecord<>(middle1Event1, 3));
        inputEvents.add(new StreamRecord<>(middle1Event2, 3));
        inputEvents.add(new StreamRecord<>(new Event(40, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(middle2Event1, 6));
        inputEvents.add(new StreamRecord<>(middle1Event3, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("c");
                                    }
                                })
                        .followedBy("middle1")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .followedBy("middle2")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("b");
                                    }
                                })
                        .optional()
                        .followedBy("end")
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middle1Event1),
                        Lists.newArrayList(startEvent, middle1Event1, middle1Event1),
                        Lists.newArrayList(startEvent, middle2Event1, middle1Event3),
                        Lists.newArrayList(startEvent, middle1Event1, middle1Event1, middle1Event2),
                        Lists.newArrayList(startEvent, middle1Event1, middle2Event1, middle1Event3),
                        Lists.newArrayList(
                                startEvent,
                                middle1Event1,
                                middle1Event1,
                                middle1Event2,
                                middle1Event3),
                        Lists.newArrayList(
                                startEvent,
                                middle1Event1,
                                middle1Event1,
                                middle2Event1,
                                middle1Event3),
                        Lists.newArrayList(
                                startEvent,
                                middle1Event1,
                                middle1Event1,
                                middle1Event2,
                                middle2Event1,
                                middle1Event3)));
    }
}
