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
import org.apache.flink.cep.nfa.aftermatch.SkipPastLastStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.utils.NFATestHarness;
import org.apache.flink.cep.utils.TestSharedBuffer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.cep.utils.NFATestUtilities.comparePatterns;
import static org.junit.Assert.assertThat;

/** IT tests covering {@link AfterMatchSkipStrategy}. */
public class AfterMatchSkipITCase extends TestLogger {

    @Test
    public void testNoSkip() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 0.0);
        Event a2 = new Event(2, "a", 0.0);
        Event a3 = new Event(3, "a", 0.0);
        Event a4 = new Event(4, "a", 0.0);
        Event a5 = new Event(5, "a", 0.0);
        Event a6 = new Event(6, "a", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));
        streamEvents.add(new StreamRecord<Event>(a2));
        streamEvents.add(new StreamRecord<Event>(a3));
        streamEvents.add(new StreamRecord<Event>(a4));
        streamEvents.add(new StreamRecord<Event>(a5));
        streamEvents.add(new StreamRecord<Event>(a6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.noSkip())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .times(3);

        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a1, a2, a3),
                        Lists.newArrayList(a2, a3, a4),
                        Lists.newArrayList(a3, a4, a5),
                        Lists.newArrayList(a4, a5, a6)));
    }

    @Test
    public void testNoSkipWithFollowedByAny() throws Exception {
        List<List<Event>> resultingPatterns =
                TwoVariablesFollowedByAny.compute(AfterMatchSkipStrategy.noSkip());

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(
                                TwoVariablesFollowedByAny.a1, TwoVariablesFollowedByAny.b1),
                        Lists.newArrayList(
                                TwoVariablesFollowedByAny.a1, TwoVariablesFollowedByAny.b2),
                        Lists.newArrayList(
                                TwoVariablesFollowedByAny.a2, TwoVariablesFollowedByAny.b2)));
    }

    @Test
    public void testSkipToNextWithFollowedByAny() throws Exception {
        List<List<Event>> resultingPatterns =
                TwoVariablesFollowedByAny.compute(AfterMatchSkipStrategy.skipToNext());

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(
                                TwoVariablesFollowedByAny.a1, TwoVariablesFollowedByAny.b1),
                        Lists.newArrayList(
                                TwoVariablesFollowedByAny.a2, TwoVariablesFollowedByAny.b2)));
    }

    static class TwoVariablesFollowedByAny {

        static Event a1 = new Event(1, "a", 0.0);
        static Event b1 = new Event(2, "b", 0.0);
        static Event a2 = new Event(4, "a", 0.0);
        static Event b2 = new Event(5, "b", 0.0);

        private static List<List<Event>> compute(AfterMatchSkipStrategy skipStrategy)
                throws Exception {
            List<StreamRecord<Event>> streamEvents = new ArrayList<>();

            streamEvents.add(new StreamRecord<>(a1));
            streamEvents.add(new StreamRecord<>(b1));
            streamEvents.add(new StreamRecord<>(a2));
            streamEvents.add(new StreamRecord<>(b2));

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("start")
                            .where(
                                    new SimpleCondition<Event>() {

                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().equals("a");
                                        }
                                    })
                            .followedByAny("end")
                            .where(
                                    new SimpleCondition<Event>() {

                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().equals("b");
                                        }
                                    });

            NFATestHarness nfaTestHarness =
                    NFATestHarness.forPattern(pattern)
                            .withAfterMatchSkipStrategy(skipStrategy)
                            .build();

            return nfaTestHarness.feedRecords(streamEvents);
        }
    }

    @Test
    public void testNoSkipWithQuantifierAtTheEnd() throws Exception {
        List<List<Event>> resultingPatterns =
                QuantifierAtEndOfPattern.compute(AfterMatchSkipStrategy.noSkip());

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(
                                QuantifierAtEndOfPattern.a1,
                                QuantifierAtEndOfPattern.b1,
                                QuantifierAtEndOfPattern.b2,
                                QuantifierAtEndOfPattern.b3),
                        Lists.newArrayList(
                                QuantifierAtEndOfPattern.a1,
                                QuantifierAtEndOfPattern.b1,
                                QuantifierAtEndOfPattern.b2),
                        Lists.newArrayList(
                                QuantifierAtEndOfPattern.a1, QuantifierAtEndOfPattern.b1)));
    }

    @Test
    public void testSkipToNextWithQuantifierAtTheEnd() throws Exception {
        List<List<Event>> resultingPatterns =
                QuantifierAtEndOfPattern.compute(AfterMatchSkipStrategy.skipToNext());

        comparePatterns(
                resultingPatterns,
                Lists.<List<Event>>newArrayList(
                        Lists.newArrayList(
                                QuantifierAtEndOfPattern.a1, QuantifierAtEndOfPattern.b1)));
    }

    static class QuantifierAtEndOfPattern {

        static Event a1 = new Event(1, "a", 0.0);
        static Event b1 = new Event(2, "b", 0.0);
        static Event b2 = new Event(4, "b", 0.0);
        static Event b3 = new Event(5, "b", 0.0);

        private static List<List<Event>> compute(AfterMatchSkipStrategy skipStrategy)
                throws Exception {
            List<StreamRecord<Event>> streamEvents = new ArrayList<>();

            streamEvents.add(new StreamRecord<>(a1));
            streamEvents.add(new StreamRecord<>(b1));
            streamEvents.add(new StreamRecord<>(b2));
            streamEvents.add(new StreamRecord<>(b3));

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("start")
                            .where(
                                    new SimpleCondition<Event>() {

                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().equals("a");
                                        }
                                    })
                            .next("end")
                            .where(
                                    new SimpleCondition<Event>() {

                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().equals("b");
                                        }
                                    })
                            .oneOrMore();

            NFATestHarness nfaTestHarness =
                    NFATestHarness.forPattern(pattern)
                            .withAfterMatchSkipStrategy(skipStrategy)
                            .build();

            return nfaTestHarness.feedRecords(streamEvents);
        }
    }

    @Test
    public void testSkipPastLast() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a", 0.0);
        Event a2 = new Event(2, "a", 0.0);
        Event a3 = new Event(3, "a", 0.0);
        Event a4 = new Event(4, "a", 0.0);
        Event a5 = new Event(5, "a", 0.0);
        Event a6 = new Event(6, "a", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));
        streamEvents.add(new StreamRecord<Event>(a2));
        streamEvents.add(new StreamRecord<Event>(a3));
        streamEvents.add(new StreamRecord<Event>(a4));
        streamEvents.add(new StreamRecord<Event>(a5));
        streamEvents.add(new StreamRecord<Event>(a6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().equals("a");
                                    }
                                })
                        .times(3);

        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(Lists.newArrayList(a1, a2, a3), Lists.newArrayList(a4, a5, a6)));
    }

    @Test
    public void testSkipToFirst() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event ab1 = new Event(1, "ab", 0.0);
        Event ab2 = new Event(2, "ab", 0.0);
        Event ab3 = new Event(3, "ab", 0.0);
        Event ab4 = new Event(4, "ab", 0.0);
        Event ab5 = new Event(5, "ab", 0.0);
        Event ab6 = new Event(6, "ab", 0.0);

        streamEvents.add(new StreamRecord<Event>(ab1));
        streamEvents.add(new StreamRecord<Event>(ab2));
        streamEvents.add(new StreamRecord<Event>(ab3));
        streamEvents.add(new StreamRecord<Event>(ab4));
        streamEvents.add(new StreamRecord<Event>(ab5));
        streamEvents.add(new StreamRecord<Event>(ab6));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipToFirst("end"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .times(2)
                        .next("end")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .times(2);

        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(ab1, ab2, ab3, ab4),
                        Lists.newArrayList(ab3, ab4, ab5, ab6)));
    }

    @Test
    public void testSkipToLast() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event ab1 = new Event(1, "ab", 0.0);
        Event ab2 = new Event(2, "ab", 0.0);
        Event ab3 = new Event(3, "ab", 0.0);
        Event ab4 = new Event(4, "ab", 0.0);
        Event ab5 = new Event(5, "ab", 0.0);
        Event ab6 = new Event(6, "ab", 0.0);
        Event ab7 = new Event(7, "ab", 0.0);

        streamEvents.add(new StreamRecord<Event>(ab1));
        streamEvents.add(new StreamRecord<Event>(ab2));
        streamEvents.add(new StreamRecord<Event>(ab3));
        streamEvents.add(new StreamRecord<Event>(ab4));
        streamEvents.add(new StreamRecord<Event>(ab5));
        streamEvents.add(new StreamRecord<Event>(ab6));
        streamEvents.add(new StreamRecord<Event>(ab7));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", AfterMatchSkipStrategy.skipToLast("end"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .times(2)
                        .next("end")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .times(2);
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(ab1, ab2, ab3, ab4),
                        Lists.newArrayList(ab4, ab5, ab6, ab7)));
    }

    @Test
    public void testSkipPastLast2() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event a2 = new Event(2, "a2", 0.0);
        Event b1 = new Event(3, "b1", 0.0);
        Event b2 = new Event(4, "b2", 0.0);
        Event c1 = new Event(5, "c1", 0.0);
        Event c2 = new Event(6, "c2", 0.0);
        Event d1 = new Event(7, "d1", 0.0);
        Event d2 = new Event(7, "d2", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(b1));
        streamEvents.add(new StreamRecord<>(b2));
        streamEvents.add(new StreamRecord<>(c1));
        streamEvents.add(new StreamRecord<>(c2));
        streamEvents.add(new StreamRecord<>(d1));
        streamEvents.add(new StreamRecord<>(d2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .followedByAny("b")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .followedByAny("c")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("c");
                                    }
                                })
                        .followedBy("d")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("d");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns, Collections.singletonList(Lists.newArrayList(a1, b1, c1, d1)));
    }

    @Test
    public void testSkipPastLast3() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event c = new Event(2, "c", 0.0);
        Event a2 = new Event(3, "a2", 0.0);
        Event b2 = new Event(4, "b2", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));
        streamEvents.add(new StreamRecord<Event>(c));
        streamEvents.add(new StreamRecord<Event>(a2));
        streamEvents.add(new StreamRecord<Event>(b2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns, Lists.<List<Event>>newArrayList(Lists.newArrayList(a2, b2)));
    }

    @Test
    public void testSkipToFirstWithOptionalMatch() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event ab1 = new Event(1, "ab1", 0.0);
        Event c1 = new Event(2, "c1", 0.0);
        Event ab2 = new Event(3, "ab2", 0.0);
        Event c2 = new Event(4, "c2", 0.0);

        streamEvents.add(new StreamRecord<Event>(ab1));
        streamEvents.add(new StreamRecord<Event>(c1));
        streamEvents.add(new StreamRecord<Event>(ab2));
        streamEvents.add(new StreamRecord<Event>(c2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("x", AfterMatchSkipStrategy.skipToFirst("b"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("x");
                                    }
                                })
                        .oneOrMore()
                        .optional()
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .next("c")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("c");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(Lists.newArrayList(ab1, c1), Lists.newArrayList(ab2, c2)));
    }

    @Test
    public void testSkipToFirstAtStartPosition() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event ab1 = new Event(1, "ab1", 0.0);
        Event c1 = new Event(2, "c1", 0.0);
        Event ab2 = new Event(3, "ab2", 0.0);
        Event c2 = new Event(4, "c2", 0.0);

        streamEvents.add(new StreamRecord<Event>(ab1));
        streamEvents.add(new StreamRecord<Event>(c1));
        streamEvents.add(new StreamRecord<Event>(ab2));
        streamEvents.add(new StreamRecord<Event>(c2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("b", AfterMatchSkipStrategy.skipToFirst("b"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .next("c")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("c");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(Lists.newArrayList(ab1, c1), Lists.newArrayList(ab2, c2)));
    }

    @Test
    public void testSkipToFirstWithOneOrMore() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event b1 = new Event(2, "b1", 0.0);
        Event a2 = new Event(3, "a2", 0.0);
        Event b2 = new Event(4, "b2", 0.0);
        Event b3 = new Event(5, "b3", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b4 = new Event(4, "b4", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));
        streamEvents.add(new StreamRecord<Event>(b1));
        streamEvents.add(new StreamRecord<Event>(a2));
        streamEvents.add(new StreamRecord<Event>(b2));
        streamEvents.add(new StreamRecord<Event>(b3));
        streamEvents.add(new StreamRecord<Event>(a3));
        streamEvents.add(new StreamRecord<Event>(b4));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToFirst("b"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .oneOrMore()
                        .consecutive();
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a1, b1),
                        Lists.newArrayList(a2, b2),
                        Lists.newArrayList(a3, b4)));
    }

    @Test(expected = FlinkRuntimeException.class)
    public void testSkipToFirstElementOfMatch() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin(
                                "a", AfterMatchSkipStrategy.skipToFirst("a").throwExceptionOnMiss())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        // skip to first element of a match should throw exception if they are enabled,
        // this mode is used in MATCH RECOGNIZE which assumes that skipping to first element
        // would result in infinite loop. In CEP by default(with exceptions disabled), we use no
        // skip
        // strategy in this case.
    }

    @Test(expected = FlinkRuntimeException.class)
    public void testSkipToFirstNonExistentPosition() throws Exception {
        MissedSkipTo.compute(AfterMatchSkipStrategy.skipToFirst("b").throwExceptionOnMiss());

        // exception should be thrown
    }

    @Test
    public void testSkipToFirstNonExistentPositionWithoutException() throws Exception {
        List<List<Event>> resultingPatterns =
                MissedSkipTo.compute(AfterMatchSkipStrategy.skipToFirst("b"));

        comparePatterns(
                resultingPatterns,
                Collections.singletonList(Lists.newArrayList(MissedSkipTo.a, MissedSkipTo.c)));
    }

    @Test(expected = FlinkRuntimeException.class)
    public void testSkipToLastNonExistentPosition() throws Exception {
        MissedSkipTo.compute(AfterMatchSkipStrategy.skipToLast("b").throwExceptionOnMiss());

        // exception should be thrown
    }

    @Test
    public void testSkipToLastNonExistentPositionWithoutException() throws Exception {
        List<List<Event>> resultingPatterns =
                MissedSkipTo.compute(AfterMatchSkipStrategy.skipToFirst("b"));

        comparePatterns(
                resultingPatterns,
                Collections.singletonList(Lists.newArrayList(MissedSkipTo.a, MissedSkipTo.c)));
    }

    static class MissedSkipTo {
        static Event a = new Event(1, "a", 0.0);
        static Event c = new Event(4, "c", 0.0);

        static List<List<Event>> compute(AfterMatchSkipStrategy skipStrategy) throws Exception {
            List<StreamRecord<Event>> streamEvents = new ArrayList<>();

            streamEvents.add(new StreamRecord<>(a));
            streamEvents.add(new StreamRecord<>(c));

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("a")
                            .where(
                                    new SimpleCondition<Event>() {

                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().contains("a");
                                        }
                                    })
                            .next("b")
                            .where(
                                    new SimpleCondition<Event>() {
                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().contains("b");
                                        }
                                    })
                            .oneOrMore()
                            .optional()
                            .consecutive()
                            .next("c")
                            .where(
                                    new SimpleCondition<Event>() {
                                        @Override
                                        public boolean filter(Event value) throws Exception {
                                            return value.getName().contains("c");
                                        }
                                    });
            NFATestHarness nfaTestHarness =
                    NFATestHarness.forPattern(pattern)
                            .withAfterMatchSkipStrategy(skipStrategy)
                            .build();

            return nfaTestHarness.feedRecords(streamEvents);
        }
    }

    @Test
    public void testSkipToLastWithOneOrMore() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event b1 = new Event(2, "b1", 0.0);
        Event a2 = new Event(3, "a2", 0.0);
        Event b2 = new Event(4, "b2", 0.0);
        Event b3 = new Event(5, "b3", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b4 = new Event(4, "b4", 0.0);

        streamEvents.add(new StreamRecord<Event>(a1));
        streamEvents.add(new StreamRecord<Event>(b1));
        streamEvents.add(new StreamRecord<Event>(a2));
        streamEvents.add(new StreamRecord<Event>(b2));
        streamEvents.add(new StreamRecord<Event>(b3));
        streamEvents.add(new StreamRecord<Event>(a3));
        streamEvents.add(new StreamRecord<Event>(b4));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToLast("b"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                })
                        .oneOrMore()
                        .consecutive();
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a1, b1),
                        Lists.newArrayList(a2, b2),
                        Lists.newArrayList(a3, b4)));
    }

    /** Example from docs. */
    @Test
    public void testSkipPastLastWithOneOrMoreAtBeginning() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event a2 = new Event(2, "a2", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b1 = new Event(4, "b1", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(a3));
        streamEvents.add(new StreamRecord<>(b1));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .oneOrMore()
                        .consecutive()
                        .greedy()
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns, Collections.singletonList(Lists.newArrayList(a1, a2, a3, b1)));
    }

    /** Example from docs. */
    @Test
    public void testSkipToLastWithOneOrMoreAtBeginning() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event a2 = new Event(2, "a2", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b1 = new Event(4, "b1", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(a3));
        streamEvents.add(new StreamRecord<>(b1));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToLast("a"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .oneOrMore()
                        .consecutive()
                        .greedy()
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(Lists.newArrayList(a1, a2, a3, b1), Lists.newArrayList(a3, b1)));
    }

    /** Example from docs. */
    @Test
    public void testSkipToFirstWithOneOrMoreAtBeginning() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event a2 = new Event(2, "a2", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b1 = new Event(4, "b1", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(a3));
        streamEvents.add(new StreamRecord<>(b1));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToFirst("a"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .oneOrMore()
                        .consecutive()
                        .greedy()
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a1, a2, a3, b1),
                        Lists.newArrayList(a2, a3, b1),
                        Lists.newArrayList(a3, b1)));
    }

    /** Example from docs. */
    @Test
    public void testNoSkipWithOneOrMoreAtBeginning() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event a2 = new Event(2, "a2", 0.0);
        Event a3 = new Event(3, "a3", 0.0);
        Event b1 = new Event(4, "b1", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(a3));
        streamEvents.add(new StreamRecord<>(b1));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.noSkip())
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .oneOrMore()
                        .consecutive()
                        .greedy()
                        .next("b")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a1, a2, a3, b1),
                        Lists.newArrayList(a2, a3, b1),
                        Lists.newArrayList(a3, b1)));
    }

    /** Example from docs. */
    @Test
    public void testSkipToFirstDiscarding() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a = new Event(1, "a", 0.0);
        Event b = new Event(2, "b", 0.0);
        Event c1 = new Event(3, "c1", 0.0);
        Event c2 = new Event(4, "c2", 0.0);
        Event c3 = new Event(5, "c3", 0.0);
        Event d = new Event(6, "d", 0.0);

        streamEvents.add(new StreamRecord<>(a));
        streamEvents.add(new StreamRecord<>(b));
        streamEvents.add(new StreamRecord<>(c1));
        streamEvents.add(new StreamRecord<>(c2));
        streamEvents.add(new StreamRecord<>(c3));
        streamEvents.add(new StreamRecord<>(d));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a or c", AfterMatchSkipStrategy.skipToFirst("c*"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a")
                                                || value.getName().contains("c");
                                    }
                                })
                        .followedBy("b or c")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("b")
                                                || value.getName().contains("c");
                                    }
                                })
                        .followedBy("c*")
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("c");
                                    }
                                })
                        .oneOrMore()
                        .greedy()
                        .followedBy("d")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("d");
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(
                        Lists.newArrayList(a, b, c1, c2, c3, d),
                        Lists.newArrayList(c1, c2, c3, d)));
    }

    @Test
    public void testSkipBeforeOtherAlreadyCompleted() throws Exception {
        List<StreamRecord<Event>> streamEvents = new ArrayList<>();

        Event a1 = new Event(1, "a1", 0.0);
        Event c1 = new Event(2, "c1", 0.0);
        Event a2 = new Event(3, "a2", 1.0);
        Event c2 = new Event(4, "c2", 0.0);
        Event b1 = new Event(5, "b1", 1.0);
        Event b2 = new Event(6, "b2", 0.0);

        streamEvents.add(new StreamRecord<>(a1));
        streamEvents.add(new StreamRecord<>(c1));
        streamEvents.add(new StreamRecord<>(a2));
        streamEvents.add(new StreamRecord<>(c2));
        streamEvents.add(new StreamRecord<>(b1));
        streamEvents.add(new StreamRecord<>(b2));

        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("a", AfterMatchSkipStrategy.skipToFirst("c"))
                        .where(
                                new SimpleCondition<Event>() {

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("a");
                                    }
                                })
                        .followedBy("c")
                        .where(
                                new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return value.getName().contains("c");
                                    }
                                })
                        .followedBy("b")
                        .where(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        return value.getName().contains("b")
                                                && ctx.getEventsForPattern("a")
                                                                .iterator()
                                                                .next()
                                                                .getPrice()
                                                        == value.getPrice();
                                    }
                                });
        NFATestHarness nfaTestHarness = NFATestHarness.forPattern(pattern).build();

        List<List<Event>> resultingPatterns = nfaTestHarness.feedRecords(streamEvents);

        comparePatterns(
                resultingPatterns,
                Lists.newArrayList(Lists.newArrayList(a1, c1, b2), Lists.newArrayList(a2, c2, b1)));
    }

    @Test
    public void testSharedBufferIsProperlyCleared() throws Exception {
        List<StreamRecord<Event>> inputEvents = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            inputEvents.add(new StreamRecord<>(new Event(1, "a", 1.0), i));
        }

        SkipPastLastStrategy matchSkipStrategy = AfterMatchSkipStrategy.skipPastLastEvent();
        Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start", matchSkipStrategy)
                        .where(
                                new SimpleCondition<Event>() {
                                    private static final long serialVersionUID =
                                            5726188262756267490L;

                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return true;
                                    }
                                })
                        .times(2);

        SharedBuffer<Event> sharedBuffer =
                TestSharedBuffer.createTestBuffer(Event.createTypeSerializer());
        NFATestHarness nfaTestHarness =
                NFATestHarness.forPattern(pattern).withSharedBuffer(sharedBuffer).build();

        nfaTestHarness.feedRecords(inputEvents);

        assertThat(sharedBuffer.isEmpty(), Matchers.is(true));
    }
}
