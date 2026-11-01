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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava33.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link Pattern#notFollowedBy(String)} and {@link Pattern#notNext(String)}. */
@SuppressWarnings("unchecked")
class NotPatternITCase {

    @Test
    void testNotNext() throws Exception {
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
    void testNotNextNoMatches() throws Exception {
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

        assertThat(matches).isEmpty();
    }

    @Test
    void testNotNextNoMatchesAtTheEnd() throws Exception {
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

        assertThat(matches).isEmpty();
    }

    @Test
    void testNotFollowedBy() throws Exception {
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
    void testNotFollowedByBeforeOptional() throws Exception {
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
    void testTimesWithNotFollowedBy() throws Exception {
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
    void testIgnoreStateOfTimesWithNotFollowedBy() throws Exception {
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
    void testTimesWithNotFollowedByAfter() throws Exception {
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
    void testNotFollowedByBeforeOptionalAtTheEnd() throws Exception {
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
    void testNotFollowedByBeforeOptionalTimes() throws Exception {
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
    void testNotFollowedByWithBranchingAtStart() throws Exception {
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
    void testNotNextAfterOneOrMoreSkipTillNext() throws Exception {
        final List<List<Event>> matches = testNotNextAfterOneOrMore(false);
        assertThat(matches).isEmpty();
    }

    @Test
    void testNotNextAfterOneOrMoreSkipTillAny() throws Exception {
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
    void testNotFollowedByNextAfterOneOrMoreEager() throws Exception {
        final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(true, false);
        assertThat(matches).isEmpty();
    }

    @Test
    void testNotFollowedByAnyAfterOneOrMoreEager() throws Exception {
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
    void testNotFollowedByNextAfterOneOrMoreCombinations() throws Exception {
        final List<List<Event>> matches = testNotFollowedByAfterOneOrMore(false, false);
        assertThat(matches).isEmpty();
    }

    @Test
    void testNotFollowedByAnyAfterOneOrMoreCombinations() throws Exception {
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
    void testNotFollowedByAnyBeforeOneOrMoreEager() throws Exception {
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
    void testNotFollowedByAnyBeforeOneOrMoreCombinations() throws Exception {
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
    void testNotFollowedByBeforeOneOrMoreEager() throws Exception {
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
    void testNotFollowedByBeforeOneOrMoreCombinations() throws Exception {
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
    void testNotFollowedByBeforeZeroOrMoreEagerSkipTillNext() throws Exception {
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
    void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillNext() throws Exception {
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
    void testNotFollowedByBeforeZeroOrMoreEagerSkipTillAny() throws Exception {
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
    void testNotFollowedByBeforeZeroOrMoreCombinationsSkipTillAny() throws Exception {
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
    void testNotFollowedByWithinFirstAndLastAtEnd() throws Exception {
        testNotFollowedByWithinAtEnd(WithinType.FIRST_AND_LAST);
    }

    @Test
    void testNotFollowedByWithinPreviousAndCurrentAtEnd() throws Exception {
        testNotFollowedByWithinAtEnd(WithinType.PREVIOUS_AND_CURRENT);
    }

    void testNotFollowedByWithinAtEnd(WithinType withinType) throws Exception {
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
                        .within(Duration.ofMillis(3), withinType);

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> matches = feedNFA(inputEvents, nfa);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a2)));
    }

    @Test
    void testNotFollowByBeforeTimesWithin() throws Exception {
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
                        .within(Duration.ofMillis(3));

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
    void testNotFollowedByWithinAtEndAfterMatch() throws Exception {
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
                        .within(Duration.ofMillis(5));

        NFA<Event> nfa = compile(pattern, false);

        NFATestHarness harness =
                NFATestHarness.forNFA(nfa)
                        .withAfterMatchSkipStrategy(AfterMatchSkipStrategy.skipPastLastEvent())
                        .build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Collections.singletonList(Lists.newArrayList(a1, a2, a3, c1)));
    }

    // FLINK-31040
    @Test
    void testLoopingPatternBeforeNotFollowedByIsReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event a4 = new Event(4, "a", 4.0);
        Event c1 = new Event(5, "c", 5.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(a4, 4));
        inputEvents.add(new StreamRecord<>(c1, 10));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("A", AfterMatchSkipStrategy.noSkip())
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .consecutive()
                        .notFollowedBy("B")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(3));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        // Every run inside the window is a match, the last element of each start included.
        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1),
                        Lists.newArrayList(a1, a2),
                        Lists.newArrayList(a1, a2, a3),
                        Lists.newArrayList(a2),
                        Lists.newArrayList(a2, a3),
                        Lists.newArrayList(a2, a3, a4),
                        Lists.newArrayList(a3),
                        Lists.newArrayList(a3, a4),
                        Lists.newArrayList(a4)));
    }

    @Test
    void testNonGreedyLoopingPatternBeforeNotFollowedByIsReleasedOnTimeout() throws Exception {
        testLoopingPatternWithSkipPastLastEventBeforeNotFollowedBy(false);
    }

    // FLINK-31044
    @Test
    void testGreedyLoopingPatternBeforeNotFollowedByIsReleasedOnTimeout() throws Exception {
        testLoopingPatternWithSkipPastLastEventBeforeNotFollowedBy(true);
    }

    /**
     * A consecutive loop before a trailing NOT pattern, skipping past the last event of a match:
     * the whole run [a1 a2 a3] is the match when the window ends, whether the loop is greedy or
     * not.
     */
    private void testLoopingPatternWithSkipPastLastEventBeforeNotFollowedBy(boolean greedy)
            throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event clock = new Event(4, "x", 4.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(clock, 50));

        Pattern<Event, Event> loop =
                Pattern.<Event>begin("A", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .consecutive();
        if (greedy) {
            loop = loop.greedy();
        }
        Pattern<Event, ?> pattern =
                loop.notFollowedBy("B")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, a2, a3)));
    }

    @Test
    void testLoopClosedByTimeKeepsEveryElement() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event s1 = new Event(1, "search", 1.0);
        Event s2 = new Event(2, "search", 2.0);
        Event s3 = new Event(3, "search", 3.0);
        Event other = new Event(4, "other-user-traffic", 4.0);

        inputEvents.add(new StreamRecord<>(s1, 1));
        inputEvents.add(new StreamRecord<>(s2, 11));
        inputEvents.add(new StreamRecord<>(s3, 21));
        inputEvents.add(new StreamRecord<>(other, 300));

        NFATestHarness harness = NFATestHarness.forPattern(searchStruggle()).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(s1, s2, s3)));
    }

    @Test
    void testForbiddenEventCancelsFromTheLoopsFirstElementOn() throws Exception {
        Event s1 = new Event(1, "search", 1.0);
        Event click = new Event(2, "click", 2.0);
        Event s2 = new Event(3, "search", 3.0);
        Event s3 = new Event(4, "search", 4.0);
        Event clock = new Event(5, "x", 5.0);

        // A click inside the loop cancels.
        List<StreamRecord<Event>> inside = new ArrayList<>();
        inside.add(new StreamRecord<>(s1, 1));
        inside.add(new StreamRecord<>(s2, 11));
        inside.add(new StreamRecord<>(click, 16));
        inside.add(new StreamRecord<>(s3, 21));
        inside.add(new StreamRecord<>(clock, 300));

        NFATestHarness insideHarness = NFATestHarness.forPattern(searchStruggle()).build();
        comparePatterns(insideHarness.feedRecords(inside), Lists.<List<Event>>newArrayList());

        // A click before the loop's first element does not.
        List<StreamRecord<Event>> before = new ArrayList<>();
        before.add(new StreamRecord<>(s1, 1));
        before.add(new StreamRecord<>(click, 6));
        before.add(new StreamRecord<>(s2, 11));
        before.add(new StreamRecord<>(s3, 21));
        before.add(new StreamRecord<>(clock, 300));

        NFATestHarness beforeHarness = NFATestHarness.forPattern(searchStruggle()).build();
        comparePatterns(
                beforeHarness.feedRecords(before),
                Lists.<List<Event>>newArrayList(Lists.newArrayList(s1, s2, s3)));
    }

    /**
     * A forbidden event that arrives while an attempt waits at an optional repeat of a relaxed
     * {@code times(2, 4)} cancels the attempt, as it does in a relaxed {@code oneOrMore()} loop.
     *
     * <p>x4 matches nothing; skipping it moves [a1 a2 a3] and [a2 a3] into the ignore states of the
     * optional repeats they wait at, where b5 arrives. The ignore state of a {@code oneOrMore()}
     * loop over a single pattern has a stop edge on the NOT condition, so in the loop b5 cancels
     * every attempt that started before it, and only [a6 c7] is a match. The ignore states of the
     * optional repeats are held to the same rule; without the stop edge there, [a1 a2 a3] and [a2
     * a3] skip b5 as well, take a6 and complete with c7.
     */
    @Test
    void testForbiddenEventInsideAnOptionalRepeatCancelsTheMatch() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event x4 = new Event(4, "x", 4.0);
        Event b5 = new Event(5, "b", 5.0);
        Event a6 = new Event(6, "a", 6.0);
        Event c7 = new Event(7, "c", 7.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(x4, 4));
        inputEvents.add(new StreamRecord<>(b5, 5));
        inputEvents.add(new StreamRecord<>(a6, 6));
        inputEvents.add(new StreamRecord<>(c7, 7));

        Pattern<Event, ?> loop =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")));

        comparePatterns(
                NFATestHarness.forPattern(loop).build().feedRecords(inputEvents),
                Lists.<List<Event>>newArrayList(Lists.newArrayList(a6, c7)));

        Pattern<Event, ?> repeats =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 4)
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")));

        comparePatterns(
                NFATestHarness.forPattern(repeats).build().feedRecords(inputEvents),
                Lists.<List<Event>>newArrayList());
    }

    /**
     * The pattern of {@link #testForbiddenEventInsideAnOptionalRepeatCancelsTheMatch} ending in the
     * NOT pattern: nothing is released when the window closes.
     *
     * <p>b5 arrives while [a1 a2 a3] and [a2 a3] wait in the ignore states of optional repeats, and
     * the stop edge there cancels both. Without it, [a1 a2 a3] takes a6, which fills the range and
     * moves it into the Pending state, released as a match when the window closes; [a2 a3] takes a6
     * and waits at an optional repeat, which time closes into the Pending state as well.
     */
    @Test
    void testForbiddenEventInsideAnOptionalRepeatCancelsBeforeTheTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event x4 = new Event(4, "x", 4.0);
        Event b5 = new Event(5, "b", 5.0);
        Event a6 = new Event(6, "a", 6.0);
        Event clock = new Event(7, "x", 7.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(x4, 4));
        inputEvents.add(new StreamRecord<>(b5, 5));
        inputEvents.add(new StreamRecord<>(a6, 6));
        inputEvents.add(new StreamRecord<>(clock, 50));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 4)
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList());
    }

    /**
     * Time closes the optional repeats of {@code times(2, 4)} before a trailing NOT pattern as it
     * closes a loop: an attempt that has taken two or more events, and waits for another one when
     * its window ends, is a match if the forbidden event did not arrive.
     *
     * <p>[a1 a2] moves into the Pending state with a3, which it skips there. [a1 a2 a3] and [a2 a3]
     * still wait at an optional repeat when the clock passes the end of their windows, and time
     * closes the repeat. [a3] has one element only and times out.
     */
    @Test
    void testTimesRangeBeforeNotFollowedByIsReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event clock = new Event(4, "x", 4.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(clock, 50));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 4)
                        .notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, a2),
                        Lists.newArrayList(a1, a2, a3),
                        Lists.newArrayList(a2, a3)));
    }

    /**
     * Time closes the optional repetition of a group pattern before a trailing NOT pattern as it
     * closes the optional repeats of a single pattern: [a3 d4] still waits for a second repetition
     * of the group when the clock passes the end of its window, and is a match.
     *
     * <p>[a1 d2] moves into the Pending state with a3, which it skips there, and [a1 d2 a3 d4]
     * takes both repetitions.
     */
    @Test
    void testGroupPatternTimesRangeBeforeNotFollowedByIsReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event d2 = new Event(2, "d", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event d4 = new Event(4, "d", 4.0);
        Event clock = new Event(5, "x", 5.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(d2, 2));
        inputEvents.add(new StreamRecord<>(a3, 3));
        inputEvents.add(new StreamRecord<>(d4, 4));
        inputEvents.add(new StreamRecord<>(clock, 50));

        Pattern<Event, ?> pattern =
                Pattern.begin(
                                Pattern.<Event>begin("a")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("d")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("d"))))
                        .times(1, 2)
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, d2, a3, d4),
                        Lists.newArrayList(a1, d2),
                        Lists.newArrayList(a3, d4)));
    }

    @Test
    void testTimesRangeClosedByItsOwnWindowIsReleasedOnTimeout() throws Exception {
        testTimesRangeClosedByItsOwnWindow(false);
    }

    @Test
    void testGreedyTimesRangeClosedByItsOwnWindowIsReleasedOnTimeout() throws Exception {
        testTimesRangeClosedByItsOwnWindow(true);
    }

    /**
     * A {@code times(2, 4)} with its own window between events is closed when that window runs out,
     * and what it took is a match if the forbidden event does not arrive before the pattern's
     * window ends, whether the repeats are greedy or not.
     *
     * <p>a3 arrives 6 ms after a2, outside the 3 ms window, so [a1 a2] can take no more events and
     * waits for the end of the 20 ms window, in which no b arrives. A greedy {@code times(2, 4)}
     * takes every event it can, and a3 is not one it can take any more, so greediness changes
     * nothing here. [a2] and [a3] have one element only and time out.
     */
    private void testTimesRangeClosedByItsOwnWindow(boolean greedy) throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a3 = new Event(3, "a", 3.0);
        Event clock = new Event(4, "x", 4.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a3, 8));
        inputEvents.add(new StreamRecord<>(clock, 100));

        Pattern<Event, Event> repeats =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 4, Duration.ofMillis(3));
        if (greedy) {
            repeats = repeats.greedy();
        }
        Pattern<Event, ?> pattern =
                repeats.notFollowedBy("not")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(20));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, a2)));
    }

    /**
     * An optional greedy loop with its own window between events, at the end of a group pattern
     * before a trailing NOT pattern, that time closes before it takes an event: what the group took
     * is a match if the forbidden event does not arrive.
     *
     * <p>[x1] waits for an a, which has to arrive within 3 ms of x1. a6 arrives 5 ms after x1, so
     * time closes the loop for [x1] without an element. a6 is an a, but the loop can no longer take
     * it for [x1], so a6 does not end [x1], which is released when the 20 ms window ends.
     */
    @Test
    void testOptionalGreedyLoopAtTheEndOfAGroupIsReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event x1 = new Event(1, "x", 1.0);
        Event a6 = new Event(6, "a", 6.0);
        Event clock = new Event(7, "y", 7.0);

        inputEvents.add(new StreamRecord<>(x1, 1));
        inputEvents.add(new StreamRecord<>(a6, 6));
        inputEvents.add(new StreamRecord<>(clock, 100));

        Pattern<Event, Event> group =
                Pattern.<Event>begin("x")
                        .where(SimpleCondition.of(value -> value.getName().equals("x")))
                        .followedBy("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore(Duration.ofMillis(3))
                        .optional()
                        .greedy();
        Pattern<Event, ?> pattern =
                Pattern.begin(group)
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(20));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(x1)));
    }

    /**
     * A greedy {@code times(2, 4)} with its own window between events, before a NOT pattern with a
     * window of its own: when time closes the repeats, what they took is a match if the forbidden
     * event does not arrive within that window after the last of them.
     *
     * <p>a8 arrives 6 ms after a2, outside the 3 ms window, so the repeats can take no more events
     * for [a1 a2], which then waits for the 10 ms after a2 in which no b may arrive, and none does.
     * [a8] has one element only and times out.
     */
    @Test
    void testGreedyTimesRangeBeforeWindowedNotFollowedByIsReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event a2 = new Event(2, "a", 2.0);
        Event a8 = new Event(8, "a", 8.0);
        Event clock = new Event(9, "y", 9.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(a2, 2));
        inputEvents.add(new StreamRecord<>(a8, 8));
        inputEvents.add(new StreamRecord<>(clock, 100));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 4, Duration.ofMillis(3))
                        .greedy()
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10), WithinType.PREVIOUS_AND_CURRENT);

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList(Lists.newArrayList(a1, a2)));
    }

    /**
     * Time closes the optional stages that follow a NOT pattern in the middle of the pattern, also
     * for a partial match that has taken one of them: [a1 x2] and [a1] are matches.
     *
     * <p>Once [a1] takes x2, an n can no longer end it, so [a1 x2] waits in a copy of the group's
     * state without the edge to the NOT pattern n. Time closes the optional group from that copy as
     * it does from the group's own state, and no b arrives within the window.
     */
    @Test
    void testOptionalStagesAfterAMiddleNotFollowedByAreReleasedOnTimeout() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 1.0);
        Event x2 = new Event(2, "x", 2.0);
        Event clock = new Event(3, "y", 3.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(x2, 2));
        inputEvents.add(new StreamRecord<>(clock, 50));

        Pattern<Event, Event> optionalGroup =
                Pattern.<Event>begin("w")
                        .where(SimpleCondition.of(value -> value.getName().equals("w")))
                        .optional();
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .notFollowedBy("n")
                        .where(SimpleCondition.of(value -> value.getName().equals("n")))
                        .followedBy("x")
                        .where(SimpleCondition.of(value -> value.getName().equals("x")))
                        .optional()
                        .followedBy(optionalGroup)
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(10));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(
                matches,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, x2), Lists.newArrayList(a1)));
    }

    /**
     * Time closes a stage past an optional greedy loop that has taken nothing, but emits no match
     * that the loop would have extended.
     *
     * <p>[c1 c2] waits for a third c, which has to arrive within 3 ms of c2. None does, so at 5
     * time closes the repeats of c, and [c1 c2], past the optional loop of d, waits for the end of
     * the window. Because the loop of d is greedy, a partial match waiting there is dropped by the
     * first event the loop could take, d5 here, as it is when an event moves it there: [c1 c2] is
     * not emitted, and the result is the one of the repeats of c simply timing out before d5
     * arrives. [c1 c2 d5] is not found either: an event that arrives before the end of the window
     * of c moves [c1 c2] into the loop of d, where d5 extends it, but time alone does not.
     */
    @Test
    void testStageClosedByTimeBeforeAGreedyLoopIsDroppedByAnEventOfTheLoop() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c1 = new Event(1, "c", 1.0);
        Event c2 = new Event(2, "c", 2.0);
        Event d5 = new Event(5, "d", 5.0);
        Event clock = new Event(6, "y", 6.0);

        inputEvents.add(new StreamRecord<>(c1, 1));
        inputEvents.add(new StreamRecord<>(c2, 2));
        inputEvents.add(new StreamRecord<>(d5, 5));
        inputEvents.add(new StreamRecord<>(clock, 100));

        Pattern<Event, Event> group =
                Pattern.<Event>begin("c")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .times(2, 4, Duration.ofMillis(3))
                        .followedBy("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .oneOrMore()
                        .optional()
                        .greedy();
        Pattern<Event, ?> pattern =
                Pattern.begin(group)
                        .notFollowedBy("b")
                        .where(SimpleCondition.of(value -> value.getName().equals("b")))
                        .within(Duration.ofMillis(20));

        NFATestHarness harness = NFATestHarness.forPattern(pattern).build();
        final List<List<Event>> matches = harness.feedRecords(inputEvents);

        comparePatterns(matches, Lists.<List<Event>>newArrayList());
    }

    /** "A search, two or more searches, and then no click within 120": the shopper leaves. */
    private static Pattern<Event, ?> searchStruggle() {
        return Pattern.<Event>begin("first", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(SimpleCondition.of(value -> value.getName().equals("search")))
                .followedBy("more")
                .where(SimpleCondition.of(value -> value.getName().equals("search")))
                .timesOrMore(2)
                .notFollowedBy("click")
                .where(SimpleCondition.of(value -> value.getName().equals("click")))
                .within(Duration.ofMillis(120));
    }
}
