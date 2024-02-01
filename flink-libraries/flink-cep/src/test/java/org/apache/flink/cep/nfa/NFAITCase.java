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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.cep.utils.TestTimerService;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyLong;

/**
 * General tests for {@link NFA} features. See also {@link IterativeConditionsITCase}, {@link
 * NotPatternITCase}, {@link SameElementITCase} for more specific tests.
 */
@SuppressWarnings("unchecked")
public class NFAITCase extends TestLogger {

    private SharedBuffer<Event> sharedBuffer;
    private SharedBufferAccessor<Event> sharedBufferAccessor;

    @Before
    public void init() {
        sharedBuffer = TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
        sharedBufferAccessor = sharedBuffer.getAccessor();
    }

    @After
    public void clear() throws Exception {
        sharedBufferAccessor.close();
    }

    @Test
    public void testNoConditionNFA() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a = new Event(40, "a", 1.0);
        Event b = new Event(41, "b", 2.0);
        Event c = new Event(42, "c", 3.0);
        Event d = new Event(43, "d", 4.0);
        Event e = new Event(44, "e", 5.0);

        inputEvents.add(new StreamRecord<>(a, 1));
        inputEvents.add(new StreamRecord<>(b, 2));
        inputEvents.add(new StreamRecord<>(c, 3));
        inputEvents.add(new StreamRecord<>(d, 4));
        inputEvents.add(new StreamRecord<>(e, 5));

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").followedBy("end");

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a, b),
                        Lists.newArrayList(b, c),
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(d, e)));
    }

    @Test
    public void testNoConditionLoopingNFA() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a = new Event(40, "a", 1.0);
        Event b = new Event(41, "b", 2.0);
        Event c = new Event(42, "c", 3.0);
        Event d = new Event(43, "d", 4.0);
        Event e = new Event(44, "e", 5.0);

        inputEvents.add(new StreamRecord<>(a, 1));
        inputEvents.add(new StreamRecord<>(b, 2));
        inputEvents.add(new StreamRecord<>(c, 3));
        inputEvents.add(new StreamRecord<>(d, 4));
        inputEvents.add(new StreamRecord<>(e, 5));

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").followedBy("end").oneOrMore();

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a, b, c, d, e),
                        Lists.newArrayList(a, b, c, d),
                        Lists.newArrayList(a, b, c),
                        Lists.newArrayList(a, b),
                        Lists.newArrayList(b, c, d, e),
                        Lists.newArrayList(b, c, d),
                        Lists.newArrayList(b, c),
                        Lists.newArrayList(c, d, e),
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(d, e)));
    }

    @Test
    public void testAnyWithNoConditionNFA() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a = new Event(40, "a", 1.0);
        Event b = new Event(41, "b", 2.0);
        Event c = new Event(42, "c", 3.0);
        Event d = new Event(43, "d", 4.0);
        Event e = new Event(44, "e", 5.0);

        inputEvents.add(new StreamRecord<>(a, 1));
        inputEvents.add(new StreamRecord<>(b, 2));
        inputEvents.add(new StreamRecord<>(c, 3));
        inputEvents.add(new StreamRecord<>(d, 4));
        inputEvents.add(new StreamRecord<>(e, 5));

        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").followedByAny("end");

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a, b),
                        Lists.newArrayList(a, c),
                        Lists.newArrayList(a, d),
                        Lists.newArrayList(a, e),
                        Lists.newArrayList(b, c),
                        Lists.newArrayList(b, d),
                        Lists.newArrayList(b, e),
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(c, e),
                        Lists.newArrayList(d, e)));
    }

    @Test
    public void testSimplePatternNFA() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(41, "start", 1.0);
        SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
        Event endEvent = new Event(43, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(43, "foobar", 1.0), 2));
        inputEvents.add(new StreamRecord<Event>(new SubEvent(41, "barfoo", 1.0, 5.0), 3));
        inputEvents.add(new StreamRecord<Event>(middleEvent, 3));
        inputEvents.add(new StreamRecord<>(new Event(43, "start", 1.0), 4));
        inputEvents.add(new StreamRecord<>(endEvent, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("middle")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getVolume() > 5.0))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent, endEvent)));
    }

    @Test
    public void testStrictContinuityWithResults() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event middleEvent1 = new Event(41, "a", 2.0);
        Event end = new Event(42, "b", 4.0);

        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(end, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .next("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(middleEvent1, end)));
    }

    @Test
    public void testStrictContinuityNoResults() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "c", 3.0);
        Event end = new Event(43, "b", 4.0);

        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(end, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .next("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList());
    }

    /**
     * Tests that the NFA successfully filters out expired elements with respect to the window
     * length between the first and last event.
     */
    @Test
    public void testSimplePatternWithTimeWindowNFAWithinFirstAndLast() throws Exception {
        List<StreamRecord<Event>> events = new ArrayList<>();

        final Event startEvent;
        final Event middleEvent;
        final Event endEvent;

        events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
        events.add(new StreamRecord<>(startEvent = new Event(2, "start", 1.0), 2));
        events.add(new StreamRecord<>(middleEvent = new Event(3, "middle", 1.0), 3));
        events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
        events.add(new StreamRecord<>(endEvent = new Event(5, "end", 1.0), 11));
        events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(10));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(events, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent, endEvent)));
    }

    /**
     * Tests that the NFA successfully filters out expired elements with respect to the window
     * length between the previous and current event.
     */
    @Test
    public void testSimplePatternWithTimeWindowNFAWithinPreviousAndCurrent() throws Exception {
        List<StreamRecord<Event>> events = new ArrayList<>();

        final Event startEvent1;
        final Event startEvent2;
        final Event middleEvent;
        final Event endEvent;

        events.add(new StreamRecord<>(startEvent1 = new Event(1, "start", 1.0), 1));
        events.add(new StreamRecord<>(startEvent2 = new Event(2, "start", 1.0), 2));
        events.add(new StreamRecord<>(middleEvent = new Event(3, "middle", 1.0), 3));
        events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
        events.add(new StreamRecord<>(endEvent = new Event(5, "end", 1.0), 11));
        events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(9), WithinType.PREVIOUS_AND_CURRENT);

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(events, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent1, middleEvent, endEvent),
                        Lists.newArrayList(startEvent2, middleEvent, endEvent)));
    }

    /**
     * Tests that the NFA successfully returns partially matched event sequences when they've timed
     * out within the first and last event.
     */
    @Test
    public void testSimplePatternWithTimeoutHandlingWithinFirstAndLast() throws Exception {
        List<StreamRecord<Event>> events = new ArrayList<>();
        List<Map<String, List<Event>>> resultingPatterns = new ArrayList<>();
        Set<Tuple2<Map<String, List<Event>>, Long>> resultingTimeoutPatterns = new HashSet<>();
        Set<Tuple2<Map<String, List<Event>>, Long>> expectedTimeoutPatterns = new HashSet<>();

        events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
        events.add(new StreamRecord<>(new Event(2, "start", 1.0), 2));
        events.add(new StreamRecord<>(new Event(3, "middle", 1.0), 3));
        events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
        events.add(new StreamRecord<>(new Event(5, "end", 1.0), 11));
        events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));

        Map<String, List<Event>> timeoutPattern1 = new HashMap<>();
        timeoutPattern1.put("start", Collections.singletonList(new Event(1, "start", 1.0)));
        timeoutPattern1.put("middle", Collections.singletonList(new Event(3, "middle", 1.0)));

        Map<String, List<Event>> timeoutPattern2 = new HashMap<>();
        timeoutPattern2.put("start", Collections.singletonList(new Event(2, "start", 1.0)));
        timeoutPattern2.put("middle", Collections.singletonList(new Event(3, "middle", 1.0)));

        Map<String, List<Event>> timeoutPattern3 = new HashMap<>();
        timeoutPattern3.put("start", Collections.singletonList(new Event(1, "start", 1.0)));

        Map<String, List<Event>> timeoutPattern4 = new HashMap<>();
        timeoutPattern4.put("start", Collections.singletonList(new Event(2, "start", 1.0)));

        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern1, 11L));
        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern2, 12L));
        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern3, 11L));
        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern4, 12L));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(10));

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        for (StreamRecord<Event> event : events) {

            Collection<Tuple2<Map<String, List<Event>>, Long>> timeoutPatterns =
                    nfa.advanceTime(
                                    sharedBufferAccessor,
                                    nfaState,
                                    event.getTimestamp(),
                                    AfterMatchSkipStrategy.noSkip())
                            .f1;
            Collection<Map<String, List<Event>>> matchedPatterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event.getValue(),
                            event.getTimestamp(),
                            AfterMatchSkipStrategy.noSkip(),
                            new TestTimerService());

            resultingPatterns.addAll(matchedPatterns);
            resultingTimeoutPatterns.addAll(timeoutPatterns);
        }

        assertEquals(1, resultingPatterns.size());
        assertEquals(expectedTimeoutPatterns.size(), resultingTimeoutPatterns.size());

        assertEquals(expectedTimeoutPatterns, resultingTimeoutPatterns);
    }

    /**
     * Tests that the NFA successfully returns partially matched event sequences when they've timed
     * out within the previous and current event.
     */
    @Test
    public void testSimplePatternWithTimeoutHandlingWithinPreviousAndCurrent() throws Exception {
        List<StreamRecord<Event>> events = new ArrayList<>();
        List<Map<String, List<Event>>> resultingPatterns = new ArrayList<>();
        Set<Tuple2<Map<String, List<Event>>, Long>> resultingTimeoutPatterns = new HashSet<>();
        Set<Tuple2<Map<String, List<Event>>, Long>> expectedTimeoutPatterns = new HashSet<>();

        events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
        events.add(new StreamRecord<>(new Event(2, "start", 1.0), 2));
        events.add(new StreamRecord<>(new Event(3, "middle", 1.0), 3));
        events.add(new StreamRecord<>(new Event(4, "foobar", 1.0), 4));
        events.add(new StreamRecord<>(new Event(5, "end", 1.0), 11));
        events.add(new StreamRecord<>(new Event(6, "end", 1.0), 13));

        Map<String, List<Event>> timeoutPattern1 = new HashMap<>();
        timeoutPattern1.put("start", Collections.singletonList(new Event(1, "start", 1.0)));
        timeoutPattern1.put("middle", Collections.singletonList(new Event(3, "middle", 1.0)));

        Map<String, List<Event>> timeoutPattern2 = new HashMap<>();
        timeoutPattern2.put("start", Collections.singletonList(new Event(2, "start", 1.0)));
        timeoutPattern2.put("middle", Collections.singletonList(new Event(3, "middle", 1.0)));

        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern1, 13L));
        expectedTimeoutPatterns.add(Tuple2.of(timeoutPattern2, 13L));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")))
                        .within(Time.milliseconds(10), WithinType.PREVIOUS_AND_CURRENT);

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        for (StreamRecord<Event> event : events) {

            Collection<Tuple2<Map<String, List<Event>>, Long>> timeoutPatterns =
                    nfa.advanceTime(
                                    sharedBufferAccessor,
                                    nfaState,
                                    event.getTimestamp(),
                                    AfterMatchSkipStrategy.noSkip())
                            .f1;
            Collection<Map<String, List<Event>>> matchedPatterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event.getValue(),
                            event.getTimestamp(),
                            AfterMatchSkipStrategy.noSkip(),
                            new TestTimerService());

            resultingPatterns.addAll(matchedPatterns);
            resultingTimeoutPatterns.addAll(timeoutPatterns);
        }

        assertEquals(2, resultingPatterns.size());
        assertEquals(expectedTimeoutPatterns.size(), resultingTimeoutPatterns.size());

        assertEquals(expectedTimeoutPatterns, resultingTimeoutPatterns);
    }

    @Test
    public void testPendingStateMatchesWithinFirstAndLast() throws Exception {
        testPendingStateMatches(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testPendingStateMatchesWithinPreviousAndCurrent() throws Exception {
        testPendingStateMatches(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testPendingStateMatches(WithinType withinType) throws Exception {
        List<StreamRecord<Event>> events = new ArrayList<>();
        Set<Map<String, List<Event>>> resultingPendingMatches = new HashSet<>();
        Set<Map<String, List<Event>>> expectedPendingMatches = new HashSet<>();

        events.add(new StreamRecord<>(new Event(1, "start", 1.0), 1));
        events.add(new StreamRecord<>(new Event(2, "middle", 1.0), 4));
        events.add(new StreamRecord<>(new Event(3, "start", 1.0), 5));
        events.add(new StreamRecord<>(new Event(4, "start", 1.0), 11));
        events.add(new StreamRecord<>(new Event(5, "middle", 1.0), 18));

        Map<String, List<Event>> pendingMatches1 = new HashMap<>();
        pendingMatches1.put("start", Collections.singletonList(new Event(3, "start", 1.0)));

        Map<String, List<Event>> pendingMatches2 = new HashMap<>();
        pendingMatches2.put("start", Collections.singletonList(new Event(4, "start", 1.0)));

        expectedPendingMatches.add(pendingMatches1);
        expectedPendingMatches.add(pendingMatches2);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .notFollowedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .within(Time.milliseconds(5), withinType);

        NFA<Event> nfa = compile(pattern, true);

        NFAState nfaState = nfa.createInitialNFAState();

        for (StreamRecord<Event> event : events) {
            Collection<Map<String, List<Event>>> pendingMatches =
                    nfa.advanceTime(
                                    sharedBufferAccessor,
                                    nfaState,
                                    event.getTimestamp(),
                                    AfterMatchSkipStrategy.noSkip())
                            .f0;
            resultingPendingMatches.addAll(pendingMatches);
            nfa.process(
                    sharedBufferAccessor,
                    nfaState,
                    event.getValue(),
                    event.getTimestamp(),
                    AfterMatchSkipStrategy.noSkip(),
                    new TestTimerService());
        }

        assertEquals(2, resultingPendingMatches.size());
        assertEquals(expectedPendingMatches.size(), resultingPendingMatches.size());
        assertEquals(expectedPendingMatches, resultingPendingMatches);
    }

    @Test
    public void testBranchingPattern() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "start", 1.0);
        SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
        SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
        SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
        SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
        Event endEvent = new Event(46, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
        inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
        inputEvents.add(new StreamRecord<>(endEvent, 8));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle-first")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getVolume() > 5.0))
                        .followedByAny("middle-second")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("next-one")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, nextOne1, endEvent),
                        Lists.newArrayList(startEvent, middleEvent2, nextOne1, endEvent),
                        Lists.newArrayList(startEvent, middleEvent3, nextOne1, endEvent),
                        Lists.newArrayList(startEvent, middleEvent1, nextOne2, endEvent),
                        Lists.newArrayList(startEvent, middleEvent2, nextOne2, endEvent),
                        Lists.newArrayList(startEvent, middleEvent3, nextOne2, endEvent)));
    }

    @Test
    public void testComplexBranchingAfterZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);
        Event end2 = new Event(45, "d", 6.0);
        Event end3 = new Event(46, "d", 7.0);
        Event end4 = new Event(47, "e", 8.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));
        inputEvents.add(new StreamRecord<>(end2, 7));
        inputEvents.add(new StreamRecord<>(end3, 8));
        inputEvents.add(new StreamRecord<>(end4, 9));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedByAny("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("end2")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedByAny("end3")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                end1,
                                end2,
                                end4),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, end1, end2, end4),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent3, end1, end2, end4),
                        Lists.newArrayList(
                                startEvent, middleEvent2, middleEvent3, end1, end2, end4),
                        Lists.newArrayList(startEvent, middleEvent1, end1, end2, end4),
                        Lists.newArrayList(startEvent, middleEvent2, end1, end2, end4),
                        Lists.newArrayList(startEvent, middleEvent3, end1, end2, end4),
                        Lists.newArrayList(startEvent, end1, end2, end4),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                end1,
                                end3,
                                end4),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, end1, end3, end4),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent3, end1, end3, end4),
                        Lists.newArrayList(
                                startEvent, middleEvent2, middleEvent3, end1, end3, end4),
                        Lists.newArrayList(startEvent, middleEvent1, end1, end3, end4),
                        Lists.newArrayList(startEvent, middleEvent2, end1, end3, end4),
                        Lists.newArrayList(startEvent, middleEvent3, end1, end3, end4),
                        Lists.newArrayList(startEvent, end1, end3, end4)));
    }

    @Test
    public void testZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent2, end1),
                        Lists.newArrayList(startEvent, end1)));
    }

    @Test
    public void testEagerZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(end1, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, end1)));
    }

    @Test
    public void testBeginWithZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event middleEvent1 = new Event(40, "a", 2.0);
        Event middleEvent2 = new Event(41, "a", 3.0);
        Event middleEvent3 = new Event(41, "a", 3.0);
        Event end = new Event(42, "b", 4.0);

        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(middleEvent1, middleEvent2, middleEvent3, end),
                        Lists.newArrayList(middleEvent1, middleEvent2, end),
                        Lists.newArrayList(middleEvent2, middleEvent3, end),
                        Lists.newArrayList(middleEvent1, end),
                        Lists.newArrayList(middleEvent2, end),
                        Lists.newArrayList(middleEvent3, end),
                        Lists.newArrayList(end)));
    }

    @Test
    public void testZeroOrMoreAfterZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "d", 3.0);
        Event middleEvent3 = new Event(43, "d", 4.0);
        Event end = new Event(44, "e", 4.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle-first")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("middle-second")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end),
                        Lists.newArrayList(startEvent, middleEvent2, middleEvent3, end),
                        Lists.newArrayList(startEvent, middleEvent2, end),
                        Lists.newArrayList(startEvent, middleEvent1, end),
                        Lists.newArrayList(startEvent, end)));
    }

    @Test
    public void testZeroOrMoreAfterBranching() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event merging = new Event(42, "f", 3.0);
        Event kleene1 = new Event(43, "d", 4.0);
        Event kleene2 = new Event(44, "d", 4.0);
        Event end = new Event(45, "e", 4.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(merging, 5));
        inputEvents.add(new StreamRecord<>(kleene1, 6));
        inputEvents.add(new StreamRecord<>(kleene2, 7));
        inputEvents.add(new StreamRecord<>(end, 8));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("branching")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("merging")
                        .where(SimpleCondition.of(value -> value.getName().equals("f")))
                        .followedByAny("kleene")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, merging, end),
                        Lists.newArrayList(startEvent, middleEvent1, merging, kleene1, end),
                        Lists.newArrayList(startEvent, middleEvent1, merging, kleene2, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, merging, kleene1, kleene2, end),
                        Lists.newArrayList(startEvent, middleEvent2, merging, end),
                        Lists.newArrayList(startEvent, middleEvent2, merging, kleene1, end),
                        Lists.newArrayList(startEvent, middleEvent2, merging, kleene2, end),
                        Lists.newArrayList(
                                startEvent, middleEvent2, merging, kleene1, kleene2, end)));
    }

    @Test
    public void testStrictContinuityNoResultsAfterZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event start = new Event(40, "d", 2.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 2.0);
        Event middleEvent3 = new Event(43, "c", 3.0);
        Event end = new Event(44, "b", 4.0);

        inputEvents.add(new StreamRecord<>(start, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(middleEvent2, 3));
        inputEvents.add(new StreamRecord<>(middleEvent3, 4));
        inputEvents.add(new StreamRecord<>(end, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .next("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList());
    }

    @Test
    public void testStrictContinuityResultsAfterZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event start = new Event(40, "d", 2.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 2.0);
        Event end = new Event(43, "b", 4.0);

        inputEvents.add(new StreamRecord<>(start, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(middleEvent2, 3));
        inputEvents.add(new StreamRecord<>(end, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .allowCombinations()
                        .next("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(start, middleEvent1, middleEvent2, end),
                        Lists.newArrayList(start, middleEvent2, end)));
    }

    @Test
    public void testAtLeastOne() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedByAny("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1),
                        Lists.newArrayList(startEvent, middleEvent2, end1)));
    }

    @Test
    public void testBeginWithAtLeastOne() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent1 = new Event(41, "a", 2.0);
        Event startEvent2 = new Event(42, "a", 3.0);
        Event startEvent3 = new Event(42, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent1, 3));
        inputEvents.add(new StreamRecord<>(startEvent2, 4));
        inputEvents.add(new StreamRecord<>(startEvent3, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent1, startEvent2, startEvent3, end1),
                        Lists.newArrayList(startEvent1, startEvent2, end1),
                        Lists.newArrayList(startEvent1, startEvent3, end1),
                        Lists.newArrayList(startEvent2, startEvent3, end1),
                        Lists.newArrayList(startEvent1, end1),
                        Lists.newArrayList(startEvent2, end1),
                        Lists.newArrayList(startEvent3, end1)));
    }

    @Test
    public void testNextZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "start", 1.0);
        Event middleEvent1 = new Event(40, "middle", 2.0);
        Event middleEvent2 = new Event(40, "middle", 3.0);
        Event middleEvent3 = new Event(40, "middle", 4.0);
        Event endEvent = new Event(46, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1L));
        inputEvents.add(new StreamRecord<>(new Event(1, "event", 1.0), 2L));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3L));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4L));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5L));
        inputEvents.add(new StreamRecord<>(endEvent, 6L));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("middle")))
                        .oneOrMore()
                        .optional()
                        .consecutive()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(startEvent, endEvent)));
    }

    @Test
    public void testAtLeastOneEager() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .followedByAny("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent2, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent3, end1),
                        Lists.newArrayList(startEvent, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, end1)));
    }

    @Test
    public void testOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent, end1),
                        Lists.newArrayList(startEvent, end1)));
    }

    @Test
    public void testTimes() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(middleEvent2, 3));
        inputEvents.add(new StreamRecord<>(middleEvent3, 4));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent3, end1)));
    }

    @Test
    public void testStartWithTimes() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(middleEvent2, 3));
        inputEvents.add(new StreamRecord<>(middleEvent3, 4));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .consecutive()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(middleEvent1, middleEvent2, end1),
                        Lists.newArrayList(middleEvent2, middleEvent3, end1)));
    }

    @Test
    public void testTimesNonStrictWithNext() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end)));
    }

    @Test
    public void testTimesNotStrictWithFollowedByEager() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end)));
    }

    @Test
    public void testTimesNotStrictWithFollowedByNotEager() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end)));
    }

    @Test
    public void testTimesStrictWithNextAndConsecutive() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .consecutive()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList());
    }

    @Test
    public void testStartWithOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event end1 = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(end1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, end1), Lists.newArrayList(end1)));
    }

    @Test
    public void testEndWithZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent)));
    }

    @Test
    public void testStartAndEndWithZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "d", 5.0);
        Event end2 = new Event(45, "d", 5.0);
        Event end3 = new Event(46, "d", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(end1, 6));
        inputEvents.add(new StreamRecord<>(end2, 6));
        inputEvents.add(new StreamRecord<>(end3, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(middleEvent1, middleEvent2),
                        Lists.newArrayList(middleEvent1),
                        Lists.newArrayList(middleEvent2, middleEvent3),
                        Lists.newArrayList(middleEvent2),
                        Lists.newArrayList(middleEvent3)));
    }

    @Test
    public void testEndWithOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .optional();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1),
                        Lists.newArrayList(startEvent)));
    }

    @Test
    public void testEndWithOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, middleEvent3),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2),
                        Lists.newArrayList(startEvent, middleEvent1)));
    }

    ///////////////////////////////         Optional
    // ////////////////////////////////////////

    @Test
    public void testTimesNonStrictOptional1() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(3)
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testTimesNonStrictOptional2() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testTimesNonStrictOptional3() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end), // this exists because of the optional()
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testTimesStrictOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .consecutive()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testOneOrMoreStrictOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .consecutive()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testTimesStrictOptional1() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .consecutive()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testOptionalTimesNonStrictWithNext() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 2));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }
    ///////////////////////////////         Consecutive
    // ////////////////////////////////////////

    private static class ConsecutiveData {
        private static final Event startEvent = new Event(40, "c", 1.0);
        private static final Event middleEvent1 = new Event(41, "a", 2.0);
        private static final Event middleEvent2 = new Event(42, "a", 3.0);
        private static final Event middleEvent3 = new Event(43, "a", 4.0);
        private static final Event middleEvent4 = new Event(43, "a", 5.0);
        private static final Event end = new Event(44, "b", 5.0);

        private ConsecutiveData() {}
    }

    @Test
    public void testStrictOneOrMore() throws Exception {
        List<List<Event>> resultingPatterns = testOneOrMore(Quantifier.ConsumingStrategy.STRICT);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end)));
    }

    @Test
    public void testSkipTillNextOneOrMore() throws Exception {
        List<List<Event>> resultingPatterns =
                testOneOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end)));
    }

    @Test
    public void testSkipTillAnyOneOrMore() throws Exception {
        List<List<Event>> resultingPatterns =
                testOneOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_ANY);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end)));
    }

    private List<List<Event>> testOneOrMore(Quantifier.ConsumingStrategy strategy)
            throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 4));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent4, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore();

        switch (strategy) {
            case STRICT:
                pattern = pattern.consecutive();
                break;
            case SKIP_TILL_NEXT:
                break;
            case SKIP_TILL_ANY:
                pattern = pattern.allowCombinations();
                break;
        }

        pattern =
                pattern.followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        return feedNFA(inputEvents, nfa);
    }

    @Test
    public void testStrictEagerZeroOrMore() throws Exception {
        List<List<Event>> resultingPatterns = testZeroOrMore(Quantifier.ConsumingStrategy.STRICT);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testSkipTillAnyZeroOrMore() throws Exception {
        List<List<Event>> resultingPatterns =
                testZeroOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_ANY);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    @Test
    public void testSkipTillNextZeroOrMore() throws Exception {
        List<List<Event>> resultingPatterns =
                testZeroOrMore(Quantifier.ConsumingStrategy.SKIP_TILL_NEXT);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.middleEvent4,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.end),
                        Lists.newArrayList(ConsecutiveData.startEvent, ConsecutiveData.end)));
    }

    private List<List<Event>> testZeroOrMore(Quantifier.ConsumingStrategy strategy)
            throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 4));
        inputEvents.add(new StreamRecord<>(new Event(50, "d", 6.0), 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent4, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional();

        switch (strategy) {
            case STRICT:
                pattern = pattern.consecutive();
                break;
            case SKIP_TILL_NEXT:
                break;
            case SKIP_TILL_ANY:
                pattern = pattern.allowCombinations();
                break;
        }

        pattern =
                pattern.followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        return feedNFA(inputEvents, nfa);
    }

    @Test
    public void testTimesStrict() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .consecutive()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end)));
    }

    @Test
    public void testTimesNonStrict() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 2));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(new Event(23, "f", 1.0), 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.end, 7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .allowCombinations()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent1,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end),
                        Lists.newArrayList(
                                ConsecutiveData.startEvent,
                                ConsecutiveData.middleEvent2,
                                ConsecutiveData.middleEvent3,
                                ConsecutiveData.end)));
    }

    @Test
    public void testStartWithZeroOrMoreStrict() throws Exception {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .consecutive();

        testStartWithOneOrZeroOrMoreStrict(pattern);
    }

    @Test
    public void testStartWithOneOrMoreStrict() throws Exception {

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .consecutive();

        testStartWithOneOrZeroOrMoreStrict(pattern);
    }

    private void testStartWithOneOrZeroOrMoreStrict(Pattern<Event, ?> pattern) throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 1));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.startEvent, 4));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent2, 5));
        inputEvents.add(new StreamRecord<>(ConsecutiveData.middleEvent3, 6));

        NFA<Event> nfa = compile(pattern, false);

        List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(ConsecutiveData.middleEvent1),
                        Lists.newArrayList(
                                ConsecutiveData.middleEvent2, ConsecutiveData.middleEvent3),
                        Lists.newArrayList(ConsecutiveData.middleEvent2),
                        Lists.newArrayList(ConsecutiveData.middleEvent3)));
    }

    ///////////////////////////////     Clearing SharedBuffer
    // ////////////////////////////////////////

    @Test
    public void testTimesClearingBufferWithinFirstAndLast() throws Exception {
        testTimesClearingBuffer(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testTimesClearingBufferWithinPreviousAndCurrent() throws Exception {
        testTimesClearingBuffer(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testTimesClearingBuffer(WithinType withinType) throws Exception {
        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event middleEvent3 = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2)
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(8), withinType);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();

        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        nfaTestHarness.feedRecord(new StreamRecord<>(startEvent, 1));
        nfaTestHarness.feedRecord(new StreamRecord<>(middleEvent1, 2));
        nfaTestHarness.feedRecord(new StreamRecord<>(middleEvent2, 3));
        nfaTestHarness.feedRecord(new StreamRecord<>(middleEvent3, 4));
        nfaTestHarness.feedRecord(new StreamRecord<>(end1, 6));

        // pruning element
        nfa.advanceTime(sharedBufferAccessor, nfaState, 10, AfterMatchSkipStrategy.noSkip());

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testOptionalClearingBufferWithinFirstAndLast() throws Exception {
        testOptionalClearingBuffer(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testOptionalClearingBufferWithinPreviousAndCurrent() throws Exception {
        testOptionalClearingBuffer(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testOptionalClearingBuffer(WithinType withinType) throws Exception {
        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent = new Event(43, "a", 4.0);
        Event end1 = new Event(44, "b", 5.0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(8), withinType);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        nfaTestHarness.feedRecord(new StreamRecord<>(startEvent, 1));
        nfaTestHarness.feedRecord(new StreamRecord<>(middleEvent, 5));
        nfaTestHarness.feedRecord(new StreamRecord<>(end1, 6));

        // pruning element
        nfa.advanceTime(sharedBufferAccessor, nfaState, 10, AfterMatchSkipStrategy.noSkip());

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testAtLeastOneClearingBufferWithinFirstAndLast() throws Exception {
        testAtLeastOneClearingBuffer(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testAtLeastOneClearingBufferWithPreviousAndCurrent() throws Exception {
        testAtLeastOneClearingBuffer(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testAtLeastOneClearingBuffer(WithinType withinType) throws Exception {
        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event end1 = new Event(44, "b", 5.0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(8));

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        nfaTestHarness.consumeRecord(new StreamRecord<>(startEvent, 1));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent1, 3));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent2, 4));
        nfaTestHarness.consumeRecord(new StreamRecord<>(end1, 6));

        // pruning element
        nfa.advanceTime(sharedBufferAccessor, nfaState, 10, AfterMatchSkipStrategy.noSkip());

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testZeroOrMoreClearingBufferWithinFirstAndLast() throws Exception {
        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event end1 = new Event(44, "b", 5.0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(8));

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        nfaTestHarness.consumeRecord(new StreamRecord<>(startEvent, 1));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent1, 3));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent2, 4));
        nfaTestHarness.consumeRecord(new StreamRecord<>(end1, 6));

        // pruning element
        nfa.advanceTime(sharedBufferAccessor, nfaState, 10, AfterMatchSkipStrategy.noSkip());

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testZeroOrMoreClearingBufferWithinPreviousAndCurrent() throws Exception {
        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(42, "a", 3.0);
        Event end1 = new Event(44, "b", 5.0);

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .optional()
                        .followedBy("end1")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(8), WithinType.PREVIOUS_AND_CURRENT);

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();
        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();

        nfaTestHarness.consumeRecord(new StreamRecord<>(startEvent, 1));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent1, 3));
        nfaTestHarness.consumeRecord(new StreamRecord<>(middleEvent2, 4));
        nfaTestHarness.consumeRecord(new StreamRecord<>(end1, 6));

        // pruning element
        nfa.advanceTime(sharedBufferAccessor, nfaState, 10, AfterMatchSkipStrategy.noSkip());

        assertEquals(3, nfaState.getPartialMatches().size());
        assertEquals(
                "middle:0middle:0start",
                nfaState.getPartialMatches().stream()
                        .map(c -> c.getCurrentStateName())
                        .collect(Collectors.joining()));
    }

    ///////////////////////////////////////   Skip till next     /////////////////////////////

    @Test
    public void testBranchingPatternSkipTillNext() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "start", 1.0);
        SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
        SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
        SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
        SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
        Event endEvent = new Event(46, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
        inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
        inputEvents.add(new StreamRecord<>(endEvent, 8));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("middle-first")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getVolume() > 5.0))
                        .followedBy("middle-second")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("next-one")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> patterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                patterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, nextOne1, endEvent)));
    }

    @Test
    public void testBranchingPatternMixedFollowedBy() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "start", 1.0);
        SubEvent middleEvent1 = new SubEvent(41, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
        SubEvent middleEvent3 = new SubEvent(43, "foo3", 1.0, 10.0);
        SubEvent nextOne1 = new SubEvent(44, "next-one", 1.0, 2.0);
        SubEvent nextOne2 = new SubEvent(45, "next-one", 1.0, 2.0);
        Event endEvent = new Event(46, "end", 1.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<Event>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<Event>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<Event>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<Event>(nextOne1, 6));
        inputEvents.add(new StreamRecord<Event>(nextOne2, 7));
        inputEvents.add(new StreamRecord<>(endEvent, 8));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedByAny("middle-first")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getVolume() > 5.0))
                        .followedBy("middle-second")
                        .subtype(SubEvent.class)
                        .where(SimpleCondition.of(value -> value.getName().equals("next-one")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> patterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                patterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(startEvent, middleEvent1, nextOne1, endEvent),
                        Lists.newArrayList(startEvent, middleEvent2, nextOne1, endEvent),
                        Lists.newArrayList(startEvent, middleEvent3, nextOne1, endEvent)));
    }

    @Test
    public void testMultipleTakesVersionCollision() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent = new Event(40, "c", 1.0);
        Event middleEvent1 = new Event(41, "a", 2.0);
        Event middleEvent2 = new Event(41, "a", 3.0);
        Event middleEvent3 = new Event(41, "a", 4.0);
        Event middleEvent4 = new Event(41, "a", 5.0);
        Event middleEvent5 = new Event(41, "a", 6.0);
        Event end = new Event(44, "b", 5.0);

        inputEvents.add(new StreamRecord<>(startEvent, 1));
        inputEvents.add(new StreamRecord<>(middleEvent1, 3));
        inputEvents.add(new StreamRecord<>(middleEvent2, 4));
        inputEvents.add(new StreamRecord<>(middleEvent3, 5));
        inputEvents.add(new StreamRecord<>(middleEvent4, 6));
        inputEvents.add(new StreamRecord<>(middleEvent5, 7));
        inputEvents.add(new StreamRecord<>(end, 10));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle1")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("middle2")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent3,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent4,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent3,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent,
                                middleEvent1,
                                middleEvent2,
                                middleEvent4,
                                middleEvent5,
                                end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent3, middleEvent4, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent4, middleEvent5, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent3, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent4, end),
                        Lists.newArrayList(
                                startEvent, middleEvent1, middleEvent2, middleEvent5, end),
                        Lists.newArrayList(startEvent, middleEvent1, middleEvent2, end)));
    }

    @Test
    public void testNFAResultOrdering() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event startEvent1 = new Event(41, "a-1", 2.0);
        Event startEvent2 = new Event(41, "a-2", 3.0);
        Event startEvent3 = new Event(41, "a-3", 4.0);
        Event startEvent4 = new Event(41, "a-4", 5.0);
        Event endEvent1 = new Event(41, "b-1", 6.0);
        Event endEvent2 = new Event(41, "b-2", 7.0);
        Event endEvent3 = new Event(41, "b-3", 8.0);

        inputEvents.add(new StreamRecord<>(startEvent1, 1));
        inputEvents.add(new StreamRecord<>(startEvent2, 3));
        inputEvents.add(new StreamRecord<>(startEvent3, 4));
        inputEvents.add(new StreamRecord<>(startEvent4, 5));
        inputEvents.add(new StreamRecord<>(endEvent1, 6));
        inputEvents.add(new StreamRecord<>(endEvent2, 7));
        inputEvents.add(new StreamRecord<>(endEvent3, 10));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(s -> s.getName().startsWith("a-")))
                        .times(4)
                        .allowCombinations()
                        .followedByAny("middle")
                        .where(SimpleCondition.of(s -> s.getName().startsWith("b-")))
                        .times(3)
                        .consecutive();

        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();
        Collection<Map<String, List<Event>>> resultingPatterns =
                nfaTestHarness.consumeRecords(inputEvents);

        Assert.assertEquals(1L, resultingPatterns.size());

        Map<String, List<Event>> match = resultingPatterns.iterator().next();
        Assert.assertArrayEquals(
                match.get("start").toArray(),
                Lists.newArrayList(startEvent1, startEvent2, startEvent3, startEvent4).toArray());

        Assert.assertArrayEquals(
                match.get("middle").toArray(),
                Lists.newArrayList(endEvent1, endEvent2, endEvent3).toArray());
    }

    @Test
    public void testNFAResultKeyOrdering() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(41, "b", 3.0);
        Event aa1 = new Event(41, "aa", 4.0);
        Event bb1 = new Event(41, "bb", 5.0);
        Event ab1 = new Event(41, "ab", 6.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(aa1, 4));
        inputEvents.add(new StreamRecord<>(bb1, 5));
        inputEvents.add(new StreamRecord<>(ab1, 6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(s -> s.getName().equals("a")))
                        .next("b")
                        .where(SimpleCondition.of(s -> s.getName().equals("b")))
                        .next("aa")
                        .where(SimpleCondition.of(s -> s.getName().equals("aa")))
                        .next("bb")
                        .where(SimpleCondition.of(s -> s.getName().equals("bb")))
                        .next("ab")
                        .where(SimpleCondition.of(s -> s.getName().equals("ab")));

        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();
        Collection<Map<String, List<Event>>> resultingPatterns =
                nfaTestHarness.consumeRecords(inputEvents);

        Assert.assertEquals(1L, resultingPatterns.size());

        Map<String, List<Event>> match = resultingPatterns.iterator().next();

        List<String> expectedOrder = Lists.newArrayList("a", "b", "aa", "bb", "ab");
        List<String> resultOrder = new ArrayList<>();
        for (String key : match.keySet()) {
            resultOrder.add(key);
        }

        Assert.assertEquals(expectedOrder, resultOrder);
    }

    @Test
    public void testSharedBufferClearing() throws Exception {
        Pattern<Event, ?> pattern = Pattern.<Event>begin("start").followedBy("end");

        Event a = new Event(40, "a", 1.0);
        Event b = new Event(41, "b", 2.0);

        NFA<Event> nfa = compile(pattern, false);
        TestTimerService timerService = new TestTimerService();
        try (SharedBufferAccessor<Event> accessor = Mockito.spy(sharedBuffer.getAccessor())) {
            nfa.process(
                    accessor,
                    nfa.createInitialNFAState(),
                    a,
                    1,
                    AfterMatchSkipStrategy.noSkip(),
                    timerService);
            nfa.process(
                    accessor,
                    nfa.createInitialNFAState(),
                    b,
                    2,
                    AfterMatchSkipStrategy.noSkip(),
                    timerService);
            Mockito.verify(accessor, Mockito.never()).advanceTime(anyLong());
            nfa.advanceTime(
                    accessor, nfa.createInitialNFAState(), 2, AfterMatchSkipStrategy.noSkip());
            Mockito.verify(accessor, Mockito.times(1)).advanceTime(2);
        }
    }

    @Test
    public void testLoopClearingWithinFirstAndLast() throws Exception {
        testLoopClearing(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testLoopClearingWithinPreviousAndCurrent() throws Exception {
        testLoopClearing(WithinType.PREVIOUS_AND_CURRENT);
    }

    private void testLoopClearing(WithinType withinType) throws Exception {
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .times(4)
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .within(Time.milliseconds(3), withinType);

        Event a1 = new Event(40, "a", 1.0);
        Event a2 = new Event(40, "a", 1.0);

        NFA<Event> nfa = compile(pattern, false);
        TestTimerService timerService = new TestTimerService();
        NFAState nfaState = nfa.createInitialNFAState();
        try (SharedBufferAccessor<Event> accessor = sharedBuffer.getAccessor()) {
            nfa.process(accessor, nfaState, a1, 1, AfterMatchSkipStrategy.noSkip(), timerService);
            nfa.process(accessor, nfaState, a2, 2, AfterMatchSkipStrategy.noSkip(), timerService);
            nfa.advanceTime(accessor, nfaState, 4, AfterMatchSkipStrategy.noSkip());
        }

        assertThat(
                sharedBuffer.getEventsBufferSize(),
                equalTo(withinType.equals(WithinType.FIRST_AND_LAST) ? 1 : 2));
    }
}
