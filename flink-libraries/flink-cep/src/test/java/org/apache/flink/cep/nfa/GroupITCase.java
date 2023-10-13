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
import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;
import static org.junit.Assert.assertEquals;

/** IT tests covering {@link GroupPattern}. */
@SuppressWarnings("unchecked")
public class GroupITCase extends TestLogger {

    @Test
    public void testGroupFollowedByTimes() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event d = new Event(45, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c (a b){2} d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .times(2)
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, b1, a2, b2, d)));
    }

    @Test
    public void testGroupFollowedByOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event b = new Event(43, "b", 3.0);
        Event d = new Event(44, "d", 4.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(b, 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c (a b)? d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, d), Lists.newArrayList(c, a1, b, d)));
    }

    @Test
    public void testFollowedByGroupTimesOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a = new Event(41, "a", 2.0);
        Event d = new Event(45, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a, 2));
        inputEvents.add(new StreamRecord<>(d, 3));

        // c (a b){2}? d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .times(2)
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(c, d)));
    }

    @Test
    public void testGroupFollowedByOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event d = new Event(45, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c (a b)+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .oneOrMore()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, b1, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d)));
    }

    @Test
    public void testGroupFollowedByZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event d = new Event(45, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c (a b)* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .oneOrMore()
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(c, a1, b1, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d)));
    }

    @Test
    public void testGroupFollowedByAnyTimesCombinations() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event a3 = new Event(45, "a", 4.0);
        Event b3 = new Event(46, "b", 5.0);
        Event d = new Event(47, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(a3, 6));
        inputEvents.add(new StreamRecord<>(b3, 7));
        inputEvents.add(new StreamRecord<>(d, 8));

        // c any (a b){2} d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .times(2)
                        .allowCombinations()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, b1, a2, b2, d),
                        Lists.newArrayList(c, a1, b1, a3, b3, d),
                        Lists.newArrayList(c, a2, b2, a3, b3, d)));
    }

    @Test
    public void testGroupFollowedByAnyTimesOptional() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event a3 = new Event(45, "a", 4.0);
        Event b3 = new Event(46, "b", 5.0);
        Event d = new Event(47, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(a3, 6));
        inputEvents.add(new StreamRecord<>(b3, 7));
        inputEvents.add(new StreamRecord<>(d, 8));

        // c any (a b){2}? d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .times(2)
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d),
                        Lists.newArrayList(c, a2, b2, a3, b3, d)));
    }

    @Test
    public void testGroupFollowedByAnyOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event a3 = new Event(45, "a", 4.0);
        Event b3 = new Event(46, "b", 5.0);
        Event d = new Event(47, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(a3, 6));
        inputEvents.add(new StreamRecord<>(b3, 7));
        inputEvents.add(new StreamRecord<>(d, 8));

        // c any (a b){1,} d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .oneOrMore()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, b1, d),
                        Lists.newArrayList(c, a2, b2, d),
                        Lists.newArrayList(c, a3, b3, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d),
                        Lists.newArrayList(c, a2, b2, a3, b3, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, a3, b3, d)));
    }

    @Test
    public void testGroupNextZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 3.0);
        Event b2 = new Event(44, "b", 3.0);
        Event a3 = new Event(45, "a", 4.0);
        Event b3 = new Event(46, "b", 3.0);
        Event d = new Event(47, "d", 1.0);

        inputEvents.add(new StreamRecord<>(c, 1L));
        inputEvents.add(new StreamRecord<>(a1, 3L));
        inputEvents.add(new StreamRecord<>(b1, 4L));
        inputEvents.add(new StreamRecord<>(a2, 5L));
        inputEvents.add(new StreamRecord<>(b2, 6L));
        inputEvents.add(new StreamRecord<>(a3, 7L));
        inputEvents.add(new StreamRecord<>(b3, 8L));
        inputEvents.add(new StreamRecord<>(d, 9L));

        // c next (a b)* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .next(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .oneOrMore()
                        .optional()
                        .consecutive()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, d),
                        Lists.newArrayList(c, a1, b1, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, a3, b3, d)));
    }

    @Test
    public void testGroupNotFollowedBy() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event d = new Event(43, "d", 3.0);
        Event a2 = new Event(44, "a", 4.0);
        Event b2 = new Event(45, "b", 5.0);
        Event e = new Event(46, "e", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(d, 4));
        inputEvents.add(new StreamRecord<>(a2, 5));
        inputEvents.add(new StreamRecord<>(b2, 6));
        inputEvents.add(new StreamRecord<>(e, 7));

        // c (a b) ^d e
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .notFollowedBy("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a2, b2, e)));
    }

    @Test
    public void testGroupNotNext() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event d = new Event(43, "d", 3.0);
        Event a2 = new Event(44, "a", 4.0);
        Event b2 = new Event(45, "b", 5.0);
        Event e = new Event(46, "e", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(d, 4));
        inputEvents.add(new StreamRecord<>(a2, 5));
        inputEvents.add(new StreamRecord<>(b2, 6));
        inputEvents.add(new StreamRecord<>(e, 7));

        // c (a b) next ^d e
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .notNext("notPattern")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a2, b2, e)));
    }

    @Test
    public void testGroupNest() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event d = new Event(40, "d", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event c1 = new Event(43, "c", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event c2 = new Event(45, "c", 4.0);
        Event e = new Event(46, "e", 6.0);

        inputEvents.add(new StreamRecord<>(d, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(c1, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(c2, 6));
        inputEvents.add(new StreamRecord<>(e, 7));

        // d (a (b c)*)? e
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy(
                                                Pattern.<Event>begin("middle2")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "b")))
                                                        .followedBy("middle3")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "c"))))
                                        .oneOrMore()
                                        .optional())
                        .optional()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(d, e),
                        Lists.newArrayList(d, a1, e),
                        Lists.newArrayList(d, a1, b1, c1, e),
                        Lists.newArrayList(d, a1, b1, c1, b2, c2, e)));
    }

    @Test
    public void testGroupNestTimes() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event d = new Event(40, "d", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event c1 = new Event(43, "c", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event c2 = new Event(45, "c", 4.0);
        Event b3 = new Event(46, "b", 5.0);
        Event c3 = new Event(47, "c", 4.0);
        Event a2 = new Event(48, "a", 2.0);
        Event b4 = new Event(49, "b", 3.0);
        Event c4 = new Event(50, "c", 4.0);
        Event b5 = new Event(51, "b", 5.0);
        Event c5 = new Event(52, "c", 4.0);
        Event b6 = new Event(53, "b", 5.0);
        Event c6 = new Event(54, "c", 4.0);
        Event e = new Event(55, "e", 6.0);

        inputEvents.add(new StreamRecord<>(d, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(c1, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(c2, 6));
        inputEvents.add(new StreamRecord<>(b3, 7));
        inputEvents.add(new StreamRecord<>(c3, 8));
        inputEvents.add(new StreamRecord<>(a2, 9));
        inputEvents.add(new StreamRecord<>(b4, 10));
        inputEvents.add(new StreamRecord<>(c4, 11));
        inputEvents.add(new StreamRecord<>(b5, 12));
        inputEvents.add(new StreamRecord<>(c5, 13));
        inputEvents.add(new StreamRecord<>(b6, 14));
        inputEvents.add(new StreamRecord<>(c6, 15));
        inputEvents.add(new StreamRecord<>(e, 16));

        // d any (a (b c){3}){0, 2} e
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy(
                                                Pattern.<Event>begin("middle2")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "b")))
                                                        .followedBy("middle3")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "c"))))
                                        .times(3))
                        .times(0, 2)
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(d, e),
                        Lists.newArrayList(d, a1, b1, c1, b2, c2, b3, c3, e),
                        Lists.newArrayList(d, a2, b4, c4, b5, c5, b6, c6, e),
                        Lists.newArrayList(
                                d, a1, b1, c1, b2, c2, b3, c3, a2, b4, c4, b5, c5, b6, c6, e)));
    }

    @Test
    public void testGroupNestTimesConsecutive() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event d = new Event(40, "d", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event c1 = new Event(43, "c", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event c2 = new Event(45, "c", 4.0);
        Event b3 = new Event(46, "b", 5.0);
        Event c3 = new Event(47, "c", 4.0);
        Event a2 = new Event(48, "a", 2.0);
        Event b4 = new Event(49, "b", 3.0);
        Event c4 = new Event(50, "c", 4.0);
        Event b5 = new Event(51, "b", 5.0);
        Event c5 = new Event(52, "c", 4.0);
        Event b6 = new Event(53, "b", 5.0);
        Event c6 = new Event(54, "c", 4.0);
        Event e = new Event(55, "e", 6.0);

        inputEvents.add(new StreamRecord<>(d, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(c1, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(c2, 6));
        inputEvents.add(new StreamRecord<>(b3, 7));
        inputEvents.add(new StreamRecord<>(c3, 8));
        inputEvents.add(new StreamRecord<>(a2, 9));
        inputEvents.add(new StreamRecord<>(b4, 10));
        inputEvents.add(new StreamRecord<>(c4, 11));
        inputEvents.add(new StreamRecord<>(new Event(0, "breaking", 99.0), 12));
        inputEvents.add(new StreamRecord<>(b5, 13));
        inputEvents.add(new StreamRecord<>(c5, 14));
        inputEvents.add(new StreamRecord<>(b6, 15));
        inputEvents.add(new StreamRecord<>(c6, 16));
        inputEvents.add(new StreamRecord<>(e, 17));

        // d any (a (b c){3}){0, 2} e
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedByAny(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy(
                                                Pattern.<Event>begin("middle2")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "b")))
                                                        .followedBy("middle3")
                                                        .where(
                                                                SimpleCondition.of(
                                                                        value ->
                                                                                value.getName()
                                                                                        .equals(
                                                                                                "c"))))
                                        .times(3)
                                        .consecutive())
                        .times(0, 2)
                        .consecutive()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("e")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(d, e),
                        Lists.newArrayList(d, a1, b1, c1, b2, c2, b3, c3, e)));
    }

    @Test
    public void testGroupBegin() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event d = new Event(45, "d", 6.0);

        inputEvents.add(new StreamRecord<>(a1, 1));
        inputEvents.add(new StreamRecord<>(b1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(b2, 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // (a b){1, 2} d
        Pattern<Event, ?> pattern =
                Pattern.begin(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .times(1, 2)
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(a1, b1, d),
                        Lists.newArrayList(a2, b2, d),
                        Lists.newArrayList(a1, b1, a2, b2, d)));
    }

    @Test
    public void testGroupFollowedByOneOrMoreWithUntilCondition() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event b1 = new Event(42, "b", 3.0);
        Event a2 = new Event(43, "a", 4.0);
        Event b2 = new Event(44, "b", 5.0);
        Event d = new Event(45, "d", 6.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(b1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(b2, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c (a b)+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("a")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b"))))
                        .oneOrMore()
                        .until(SimpleCondition.of(value -> value.getName().equals("d")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();

        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();
        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, b1, d),
                        Lists.newArrayList(c, a1, b1, a2, b2, d)));

        assertEquals(1, nfaState.getPartialMatches().size());
        assertEquals("start", nfaState.getPartialMatches().peek().getCurrentStateName());
    }

    @Test
    public void testGroupStartsWithOptionalPattern() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a = new Event(40, "a", 1.0);
        Event c = new Event(41, "c", 2.0);
        Event d = new Event(42, "d", 3.0);

        inputEvents.add(new StreamRecord<>(a, 1));
        inputEvents.add(new StreamRecord<>(c, 2));
        inputEvents.add(new StreamRecord<>(d, 3));

        // a (b? c) d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .next(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b")))
                                        .optional()
                                        .next("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("c"))))
                        .next("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();

        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();
        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(a, c, d)));
    }

    @Test
    public void testFollowedByOptionalGroupPattern() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event a = new Event(40, "a", 1.0);
        Event d1 = new Event(41, "d", 2.0);
        Event d2 = new Event(42, "d", 3.0);

        inputEvents.add(new StreamRecord<>(a, 1));
        inputEvents.add(new StreamRecord<>(d1, 2));
        inputEvents.add(new StreamRecord<>(d2, 3));

        // a -> (b c)? d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("b")))
                                        .next("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("c"))))
                        .optional()
                        .next("d")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        NFAState nfaState = nfa.createInitialNFAState();

        NFATestHarness nfaTestHarness = NFATestHarness.forNFA(nfa).withNFAState(nfaState).build();
        final List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(inputEvents);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(a, d1)));
    }
}
