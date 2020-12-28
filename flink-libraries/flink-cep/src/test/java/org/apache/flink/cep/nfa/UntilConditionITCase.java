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

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/** Tests for {@link Pattern#until(IterativeCondition)}. */
public class UntilConditionITCase {

    /** Condition used for {@link Pattern#until(IterativeCondition)} clause. */
    public static final SimpleCondition<Event> UNTIL_CONDITION =
            new SimpleCondition<Event>() {
                private static final long serialVersionUID = 5726188262756267490L;

                @Override
                public boolean filter(Event value) throws Exception {
                    return value.getPrice() == 5.0;
                }
            };

    @Test
    public void testUntilConditionFollowedByOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking)));

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByOneOrMoreCombinations() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .allowCombinations()
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent3, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByOneOrMoreConsecutive() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(45, "a", 5.0);
        Event ignored = new Event(46, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 7));
        inputEvents.add(new StreamRecord<>(ignored, 8));

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
                        .consecutive()
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByOneOrMoreConsecutive2() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "b", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event breaking = new Event(45, "a", 5.0);
        Event ignored = new Event(46, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 7));
        inputEvents.add(new StreamRecord<>(ignored, 8));

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
                        .consecutive()
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking),
                        Lists.newArrayList(startEvent, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByZeroOrMoreCombinations() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .allowCombinations()
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent3, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking),
                        Lists.newArrayList(startEvent, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByZeroOrMoreConsecutive() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(45, "a", 5.0);
        Event ignored = new Event(46, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 7));
        inputEvents.add(new StreamRecord<>(ignored, 8));

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
                        .consecutive()
                        .until(UNTIL_CONDITION)
                        .followedBy("end")
                        .where(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, breaking),
                        Lists.newArrayList(startEvent, middleEvent1, breaking),
                        Lists.newArrayList(startEvent, breaking)));
        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testUntilConditionFollowedByAnyOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(44, "a", 5.0);
        Event middleEvent3 = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(middleEvent3, 7));

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
                        .until(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent3)));
    }

    @Test
    public void testUntilConditionFollowedByAnyZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event breaking = new Event(44, "a", 5.0);
        Event middleEvent3 = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(middleEvent3, 7));

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
                        .until(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent3),
                        Lists.newArrayList(startEvent)));
    }

    @Test
    public void testUntilConditionWithEmptyWhere() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(40, "d", 1.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .oneOrMore()
                        .until(UNTIL_CONDITION);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1)));

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testIterativeUntilConditionOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(40, "d", 1.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .oneOrMore()
                        .until(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {

                                        double sum = 0;
                                        for (Event middle : ctx.getEventsForPattern("middle")) {
                                            sum += middle.getPrice();
                                        }

                                        return sum == 6.0;
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1)));

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testIterativeUntilConditionZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(40, "d", 1.0);
        Event breaking = new Event(44, "a", 5.0);
        Event ignored = new Event(45, "a", 6.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(breaking, 6));
        inputEvents.add(new StreamRecord<>(ignored, 7));

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
                        .oneOrMore()
                        .optional()
                        .until(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {

                                        double sum = 0;
                                        for (Event middle : ctx.getEventsForPattern("middle")) {
                                            sum += middle.getPrice();
                                        }

                                        return sum == 6.0;
                                    }
                                });

        NFA<Event> nfa = compile(pattern, false);
        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent)));

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }
}
