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
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.WithinType;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/** Tests for {@link Pattern#notFollowedBy(String)} and {@link Pattern#notNext(String)}. */
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notNext("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, c1, d), Lists.newArrayList(a1, c2, d)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notNext("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .notNext("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, c1, d)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, c1, d)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .times(2)
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList());
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .times(2)
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, d1)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .times(2)
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList());
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .optional();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, c1), Lists.newArrayList(a1)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedByAny("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .times(2)
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(
                matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, c1, c2, d)));
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a2, c2, d)));
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

        private NotFollowByData() {}
    }

    @Test
    public void testNotNextAfterOneOrMoreSkipTillNext() throws Exception {
        final List<List<Event>> matches = testNotNextAfterOneOrMore(false);
        assertEquals(0, matches.size());
    }

    @Test
    public void testNotNextAfterOneOrMoreSkipTillAny() throws Exception {
        final List<List<Event>> matches = testNotNextAfterOneOrMore(true);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_2, NotFollowByData.D_1)));
    }

    private List<List<Event>> testNotNextAfterOneOrMore(boolean allMatches) throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        int i = 0;
        inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_2, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i++));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")));

        pattern =
                (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore()
                        .notNext("not c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

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
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_6, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByNextAfterOneOrMoreCombinations() throws Exception {
        final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(false, false);
        assertEquals(0, matches.size());
    }

    @Test
    public void testNotFollowedByAnyAfterOneOrMoreCombinations() throws Exception {
        final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(false, true);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_4, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_5, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_6, NotFollowByData.D_1)));
    }

    private List<List<Event>> testNotFollowedByAfterOneOrMore(boolean eager, boolean allMatches)
            throws Exception {
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

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")));

        pattern =
                (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
                        .where(SimpleCondition.of(value -> value.getName().equals("b")));

        pattern =
                (eager ? pattern.oneOrMore() : pattern.oneOrMore().allowCombinations())
                        .notFollowedBy("not c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        return feedNFA(inputEvents, nfa);
    }

    @Test
    public void testNotFollowedByAnyBeforeOneOrMoreEager() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(true, true);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByAnyBeforeOneOrMoreCombinations() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(false, true);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByBeforeOneOrMoreEager() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(true, false);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByBeforeOneOrMoreCombinations() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeOneOrMore(false, false);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    private List<List<Event>> testNotFollowedByBeforeOneOrMore(boolean eager, boolean allMatches)
            throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        int i = 0;
        inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_4, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_5, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_6, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("not c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")));

        pattern =
                (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore();

        pattern =
                (eager ? pattern : pattern.allowCombinations())
                        .followedBy("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        return feedNFA(inputEvents, nfa);
    }

    @Test
    public void testNotFollowedByBeforeZeroOrMoreEagerSkipTillNext() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(true, false);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillNext() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(false, false);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByBeforeZeroOrMoreEagerSkipTillAny() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(true, true);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1)));
    }

    @Test
    public void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillAny() throws Exception {
        final List<List<Event>> matches = testNotFollowedByBeforeZeroOrMore(false, true);
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_4,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1, NotFollowByData.B_1, NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_5,
                                NotFollowByData.D_1),
                        Lists.newArrayList(
                                NotFollowByData.A_1,
                                NotFollowByData.B_1,
                                NotFollowByData.B_6,
                                NotFollowByData.D_1)));
    }

    private List<List<Event>> testNotFollowedByBeforeZeroOrMore(boolean eager, boolean allMatches)
            throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        int i = 0;
        inputEvents.add(new StreamRecord<>(NotFollowByData.A_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.C_1, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_4, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_5, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.B_6, i++));
        inputEvents.add(new StreamRecord<>(NotFollowByData.D_1, i));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("not c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")));

        pattern =
                (allMatches ? pattern.followedByAny("b*") : pattern.followedBy("b*"))
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .oneOrMore()
                        .optional();

        pattern =
                (eager ? pattern : pattern.allowCombinations())
                        .followedBy("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        return feedNFA(inputEvents, nfa);
    }

    @Test
    public void testNotFollowedByWithinFirstAndLastAtEnd() throws Exception {
        testNotFollowedByWithinAtEnd(WithinType.FIRST_AND_LAST);
    }

    @Test
    public void testNotFollowedByWithinPreviousAndCurrentAtEnd() throws Exception {
        testNotFollowedByWithinAtEnd(WithinType.PREVIOUS_AND_CURRENT);
    }

    public void testNotFollowedByWithinAtEnd(WithinType withinType) throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(40, "a", 1.0);
        Event b1 = new Event(41, "b", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event c = new Event(43, "c", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event a3 = new Event(45, "a", 7.0);
        Event b3 = new Event(46, "b", 8.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(b1, 2));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(c, 5));
        inputEvents.add(new StreamRecord<>(b2, 10));
        inputEvents.add(new StreamRecord<>(a3, 11));
        inputEvents.add(new StreamRecord<>(b3, 13));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(3), withinType);

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a2)));
    }

    @Test
    public void testNotFollowByBeforeTimesWithin() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(40, "a", 1.0);
        Event b1 = new Event(41, "b", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event c1 = new Event(43, "c", 4.0);
        Event c2 = new Event(44, "c", 5.0);
        Event a3 = new Event(45, "a", 7.0);
        Event c3 = new Event(46, "c", 8.0);
        Event c4 = new Event(47, "c", 8.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(b1, 2));
        inputEvents.add(new StreamRecord<>(a2, 10));
        inputEvents.add(new StreamRecord<>(c1, 11));
        inputEvents.add(new StreamRecord<>(c2, 12));
        inputEvents.add(new StreamRecord<>(a3, 20));
        inputEvents.add(new StreamRecord<>(c3, 21));
        inputEvents.add(new StreamRecord<>(c4, 24));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedBy("c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .times(0, 2)
                        .within(Time.milliseconds(3));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1),
                        Lists.newArrayList(a2),
                        Lists.newArrayList(a2, c1),
                        Lists.newArrayList(a2, c1, c2),
                        Lists.newArrayList(a3),
                        Lists.newArrayList(a3, c3)));
    }

    @Test
    public void testNotFollowedByWithinAtEndAfterMatch() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(40, "a", 1.0);
        Event a2 = new Event(41, "a", 2.0);
        Event a3 = new Event(42, "a", 3.0);
        Event c1 = new Event(43, "c", 4.0);
        Event c2 = new Event(44, "c", 5.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(c1, 4));
        inputEvents.add(new StreamRecord<>(c2, 10));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Time.milliseconds(5));

        NFA<Event> nfa = compile(pattern, false);

        NFATestHarness harness =
                NFATestHarness.forNFA(nfa)
                        .withAfterMatchSkipStrategy(AfterMatchSkipStrategy.skipPastLastEvent())
                        .build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Collections.singletonList(Lists.newArrayList(a1, a2, a3, c1)));
    }
}
