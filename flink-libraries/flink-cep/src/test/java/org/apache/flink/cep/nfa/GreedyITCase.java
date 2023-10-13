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

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.apache.flink.cep.utils.NFATestUtilities.feedNFA;
import static org.apache.flink.cep.utils.NFAUtils.compile;

/** IT tests covering {@link Pattern#greedy()}. */
public class GreedyITCase extends TestLogger {

    @Test
    public void testGreedyZeroOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyZeroOrMoreInBetween() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 2));
        inputEvents.add(new StreamRecord<>(a1, 3));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 4));
        inputEvents.add(new StreamRecord<>(a2, 5));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 6));
        inputEvents.add(new StreamRecord<>(a3, 7));
        inputEvents.add(new StreamRecord<>(d, 8));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyZeroOrMoreWithDummyEventsAfterQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(new Event(43, "dummy", 2.0), 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, d)));
    }

    @Test
    public void testGreedyZeroOrMoreWithDummyEventsBeforeQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(43, "dummy", 2.0), 2));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(c, d)));
    }

    @Test
    public void testGreedyUntilZeroOrMoreWithDummyEventsAfterQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event a3 = new Event(43, "a", 3.0);
        Event d = new Event(45, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(new Event(44, "a", 4.0), 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .until(SimpleCondition.of(value -> value.getPrice() > 3.0))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyUntilWithDummyEventsBeforeQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event a3 = new Event(43, "a", 3.0);
        Event d = new Event(45, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(44, "a", 4.0), 2));
        inputEvents.add(new StreamRecord<>(a1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(a3, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c a* d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .until(SimpleCondition.of(value -> value.getPrice() > 3.0))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(c, d)));
    }

    @Test
    public void testGreedyOneOrMore() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyOneOrMoreInBetween() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 2));
        inputEvents.add(new StreamRecord<>(a1, 3));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 4));
        inputEvents.add(new StreamRecord<>(a2, 5));
        inputEvents.add(new StreamRecord<>(new Event(1, "dummy", 1111), 6));
        inputEvents.add(new StreamRecord<>(a3, 7));
        inputEvents.add(new StreamRecord<>(d, 8));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyOneOrMoreWithDummyEventsAfterQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(new Event(43, "dummy", 2.0), 4));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, d)));
    }

    @Test
    public void testGreedyOneOrMoreWithDummyEventsBeforeQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event d = new Event(44, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(43, "dummy", 2.0), 2));
        inputEvents.add(new StreamRecord<>(d, 5));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList());
    }

    @Test
    public void testGreedyUntilOneOrMoreWithDummyEventsAfterQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event a3 = new Event(43, "a", 3.0);
        Event d = new Event(45, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(new Event(44, "a", 4.0), 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .until(SimpleCondition.of(value -> value.getPrice() > 3.0))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, d)));
    }

    @Test
    public void testGreedyUntilOneOrMoreWithDummyEventsBeforeQuantifier() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 3.0);
        Event a3 = new Event(43, "a", 3.0);
        Event d = new Event(45, "d", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(new Event(44, "a", 4.0), 2));
        inputEvents.add(new StreamRecord<>(a1, 3));
        inputEvents.add(new StreamRecord<>(a2, 4));
        inputEvents.add(new StreamRecord<>(a3, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c a+ d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .greedy()
                        .until(SimpleCondition.of(value -> value.getPrice() > 3.0))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(resultingPatterns, Lists.<List<Event>>newArrayList());
    }

    @Test
    public void testGreedyZeroOrMoreBeforeGroupPattern() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(40, "a", 1.0);
        Event a2 = new Event(40, "a", 1.0);
        Event a3 = new Event(40, "a", 1.0);
        Event d1 = new Event(40, "d", 1.0);
        Event e1 = new Event(40, "e", 1.0);
        Event d2 = new Event(40, "d", 1.0);
        Event e2 = new Event(40, "e", 1.0);
        Event f = new Event(44, "f", 3.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(new Event(43, "dummy", 2.0), 4));
        inputEvents.add(new StreamRecord<>(a3, 5));
        inputEvents.add(new StreamRecord<>(d1, 6));
        inputEvents.add(new StreamRecord<>(e1, 7));
        inputEvents.add(new StreamRecord<>(d2, 8));
        inputEvents.add(new StreamRecord<>(e2, 9));
        inputEvents.add(new StreamRecord<>(f, 10));

        // c a* (d e){2} f
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy()
                        .followedBy(
                                Pattern.<Event>begin("middle1")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("d")))
                                        .followedBy("middle2")
                                        .where(
                                                SimpleCondition.of(
                                                        value -> value.getName().equals("e"))))
                        .times(2)
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("f")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, a2, a3, d1, e1, d2, e2, f)));
    }

    @Test
    public void testEndWithZeroOrMoreGreedy() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(new Event(44, "dummy", 2.0), 4));
        inputEvents.add(new StreamRecord<>(a3, 5));

        // c a*
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .greedy();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c),
                        Lists.newArrayList(c, a1),
                        Lists.newArrayList(c, a1, a2),
                        Lists.newArrayList(c, a1, a2, a3)));
    }

    @Test
    public void testEndWithZeroOrMoreConsecutiveGreedy() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(new Event(44, "dummy", 2.0), 4));
        inputEvents.add(new StreamRecord<>(a3, 5));

        // c a*
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .oneOrMore()
                        .optional()
                        .consecutive()
                        .greedy();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c),
                        Lists.newArrayList(c, a1),
                        Lists.newArrayList(c, a1, a2)));
    }

    @Test
    public void testEndWithGreedyTimesRange() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event a4 = new Event(44, "a", 2.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(a4, 5));
        inputEvents.add(new StreamRecord<>(new Event(44, "dummy", 2.0), 6));

        // c a{2, 5}
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 5)
                        .greedy();

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(c, a1, a2),
                        Lists.newArrayList(c, a1, a2, a3),
                        Lists.newArrayList(c, a1, a2, a3, a4)));
    }

    @Test
    public void testGreedyTimesRange() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        Event c = new Event(40, "c", 1.0);
        Event a1 = new Event(41, "a", 2.0);
        Event a2 = new Event(42, "a", 2.0);
        Event a3 = new Event(43, "a", 2.0);
        Event a4 = new Event(44, "a", 2.0);
        Event d = new Event(45, "d", 2.0);

        inputEvents.add(new StreamRecord<>(c, 1));
        inputEvents.add(new StreamRecord<>(a1, 2));
        inputEvents.add(new StreamRecord<>(a2, 3));
        inputEvents.add(new StreamRecord<>(a3, 4));
        inputEvents.add(new StreamRecord<>(a4, 5));
        inputEvents.add(new StreamRecord<>(d, 6));

        // c a{2, 5} d
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("c")))
                        .followedBy("middle")
                        .where(SimpleCondition.of(value -> value.getName().equals("a")))
                        .times(2, 5)
                        .greedy()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("d")));

        NFA<Event> nfa = compile(pattern, false);

        final List<List<Event>> resultingPatterns = feedNFA(inputEvents, nfa);

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(Lists.newArrayList(c, a1, a2, a3, a4, d)));
    }
}
