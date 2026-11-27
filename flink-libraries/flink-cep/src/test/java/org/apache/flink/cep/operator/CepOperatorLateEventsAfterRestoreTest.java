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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Late events after a restore (FLINK-41553). */
class CepOperatorLateEventsAfterRestoreTest {

    private static final OutputTag<Event> LATE = new OutputTag<Event>("late") {};

    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testLateEventsAfterRestoreGoToSideOutput(boolean rocksDb) throws Exception {
        OperatorSubtaskState snapshot = snapshotAfterStartAndMiddle(rocksDb, LATE);

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, LATE)) {
            harness.initializeState(snapshot);
            harness.open();
            // older than the watermark of 5000 before the restore and the newest event in the NFA
            harness.processElement(new Event("start", 3), 1000L);
            harness.processElement(new Event("middle", 4), 2000L);
            // older than the watermark before the restore, which the operator doesn't snapshot
            // (FLINK-5601), but newer than every event in the NFA, so it is processed
            harness.processElement(new Event("middle", 5), 4000L);
            harness.processElement(new Event("end", 9), 6000L);
            harness.processWatermark(7000L);

            assertThat(harness.extractOutputValues())
                    .containsExactly("{V=[start#1], M=[middle#2, middle#5], L=[end#9]}");
            assertThat(lateEvents(harness)).containsExactly("start#3@1000", "middle#4@2000");
        }
    }

    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testLateEventsAfterRestoreAreCountedWithoutSideOutput(boolean rocksDb) throws Exception {
        OperatorSubtaskState snapshot = snapshotAfterStartAndMiddle(rocksDb, null);

        CepOperator<Event, String, String> operator = operator(true, null);
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(operator, rocksDb)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("middle", 3), 2000L);
            harness.processElement(new Event("end", 9), 6000L);
            harness.processWatermark(7000L);

            assertThat(harness.extractOutputValues())
                    .containsExactly("{V=[start#1], M=[middle#2], L=[end#9]}");
            assertThat(operator.getLateRecordsNumber()).isOne();
        }
    }

    /**
     * A job that restored once, buffered late events and failed before they were processed must
     * also restore from its latest snapshot.
     */
    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testLateEventsBufferedAtSnapshotAreLateAfterRestore(boolean rocksDb) throws Exception {
        OperatorSubtaskState snapshot = snapshotAfterStartAndMiddle(rocksDb, LATE);

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, LATE)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("start", 3), 1000L);
            harness.processElement(new Event("middle", 4), 2000L);
            snapshot = harness.snapshot(2L, 2L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, LATE)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("end", 9), 6000L);
            harness.processWatermark(7000L);

            assertThat(harness.extractOutputValues())
                    .containsExactly("{V=[start#1], M=[middle#2], L=[end#9]}");
            assertThat(lateEvents(harness)).containsExactly("start#3@1000", "middle#4@2000");
        }
    }

    /**
     * With a skip strategy, a completed match waits for partial matches that started earlier. Its
     * events are still referenced, so events before its last event are late as well.
     */
    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testLateEventsBeforeAWaitingCompletedMatchGoToSideOutput(boolean rocksDb)
            throws Exception {
        // B only matches the end event whose id is the id of A plus 10
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("A", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(kind("A", "start", false))
                        .followedBy("B")
                        .where(
                                new IterativeCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value, Context<Event> ctx)
                                            throws Exception {
                                        Event a = ctx.getEventsForPattern("A").iterator().next();
                                        return value.kind.equals("end") && value.id == a.id + 10;
                                    }
                                })
                        .within(Duration.ofMinutes(30));

        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(operator(pattern, LATE), rocksDb)) {
            harness.open();
            harness.processElement(new Event("start", 1), 1000L);
            harness.processElement(new Event("start", 2), 2000L);
            harness.processElement(new Event("end", 12), 3000L);
            harness.processWatermark(5000L);
            // the match of start#2 waits for the partial match of start#1
            assertThat(harness.extractOutputValues()).isEmpty();
            snapshot = harness.snapshot(1L, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(operator(pattern, LATE), rocksDb)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("end", 11), 2500L);
            harness.processWatermark(Duration.ofHours(1).toMillis());

            assertThat(harness.extractOutputValues()).containsExactly("{A=[start#2], B=[end#12]}");
            assertThat(lateEvents(harness)).containsExactly("end#11@2500");
        }
    }

    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testElementsBufferedAtSnapshotAreProcessedAfterRestore(boolean rocksDb) throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, LATE)) {
            harness.open();
            harness.processElement(new Event("start", 1), 1000L);
            harness.processWatermark(1500L);
            // still buffered when the snapshot is taken
            harness.processElement(new Event("middle", 2), 2000L);
            snapshot = harness.snapshot(1L, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, LATE)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("end", 9), 3000L);
            harness.processWatermark(4000L);

            assertThat(harness.extractOutputValues())
                    .containsExactly("{V=[start#1], M=[middle#2], L=[end#9]}");
            assertThat(lateEvents(harness)).isEmpty();
        }
    }

    /**
     * Stop-with-savepoint with --drain advances the NFA to {@link Long#MAX_VALUE}, while partial
     * matches without {@code within()} stay.
     */
    @ParameterizedTest(name = "rocksdb = {0}")
    @ValueSource(booleans = {false, true})
    void testRestoreFromDrainedSavepointWithoutWithin(boolean rocksDb) throws Exception {
        OperatorSubtaskState snapshot;
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, false, LATE)) {
            harness.open();
            harness.processElement(new Event("start", 1), 1000L);
            harness.processElement(new Event("middle", 2), 2000L);
            harness.processWatermark(1500L);
            harness.processWatermark(Long.MAX_VALUE);
            snapshot = harness.snapshot(1L, 1L);
        }

        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, false, LATE)) {
            harness.initializeState(snapshot);
            harness.open();
            harness.processElement(new Event("middle", 3), 2000L);
            harness.processElement(new Event("middle", 4), 2500L);
            harness.processElement(new Event("end", 9), 3000L);
            harness.processWatermark(4000L);

            assertThat(harness.extractOutputValues())
                    .containsExactly("{V=[start#1], M=[middle#2, middle#4], L=[end#9]}");
            assertThat(lateEvents(harness)).containsExactly("middle#3@2000");
        }
    }

    private static OperatorSubtaskState snapshotAfterStartAndMiddle(
            boolean rocksDb, OutputTag<Event> lateTag) throws Exception {
        try (KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                harness(rocksDb, true, lateTag)) {
            harness.open();
            harness.processElement(new Event("start", 1), 1000L);
            harness.processElement(new Event("middle", 2), 2000L);
            harness.processWatermark(5000L);
            return harness.snapshot(1L, 1L);
        }
    }

    private static List<String> lateEvents(
            KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness) {
        if (harness.getSideOutput(LATE) == null) {
            return Collections.emptyList();
        }
        return harness.getSideOutput(LATE).stream()
                .map((StreamRecord<Event> r) -> r.getValue() + "@" + r.getTimestamp())
                .collect(Collectors.toList());
    }

    /** Like SQL {@code DEFINE x AS x.kind = '...'}, reading the events of its own variable. */
    private static IterativeCondition<Event> kind(String variable, String kind, boolean negate) {
        return new IterativeCondition<Event>() {
            @Override
            public boolean filter(Event value, Context<Event> ctx) throws Exception {
                ctx.getEventsForPattern(variable).forEach(e -> {});
                return negate != kind.equals(value.kind);
            }
        };
    }

    /** {@code PATTERN (V M* L) [WITHIN INTERVAL '30' MINUTE] AFTER MATCH SKIP PAST LAST ROW}. */
    private static CepOperator<Event, String, String> operator(
            boolean within, OutputTag<Event> lateTag) {
        Pattern<Event, Event> pattern =
                Pattern.<Event>begin("V", AfterMatchSkipStrategy.skipPastLastEvent())
                        .where(kind("V", "start", false))
                        .next("M")
                        .where(kind("M", "end", true))
                        .oneOrMore()
                        .optional()
                        .consecutive()
                        .greedy()
                        .next("L")
                        .where(kind("L", "end", false));
        if (within) {
            pattern = pattern.within(Duration.ofMinutes(30));
        }
        return operator(pattern, lateTag);
    }

    private static CepOperator<Event, String, String> operator(
            Pattern<Event, Event> pattern, OutputTag<Event> lateTag) {
        return new CepOperator<>(
                TypeInformation.of(Event.class).createSerializer(new SerializerConfigImpl()),
                false,
                NFACompiler.compileFactory(pattern, false),
                null,
                pattern.getAfterMatchSkipStrategy(),
                new PatternProcessFunction<Event, String>() {
                    @Override
                    public void processMatch(
                            Map<String, List<Event>> match, Context ctx, Collector<String> out) {
                        out.collect(match.toString());
                    }
                },
                lateTag);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness(
            boolean rocksDb, boolean within, OutputTag<Event> lateTag) throws Exception {
        return harness(operator(within, lateTag), rocksDb);
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness(
            CepOperator<Event, String, String> operator, boolean rocksDb) throws Exception {
        KeyedOneInputStreamOperatorTestHarness<String, Event, String> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator,
                        (KeySelector<Event, String>) e -> e.key,
                        BasicTypeInfo.STRING_TYPE_INFO);
        harness.setStateBackend(
                rocksDb ? new EmbeddedRocksDBStateBackend() : new HashMapStateBackend());
        return harness;
    }

    /** Input row. */
    public static class Event {
        public String key;
        public String kind;
        public int id;

        public Event() {}

        Event(String kind, int id) {
            this.key = "k";
            this.kind = kind;
            this.id = id;
        }

        @Override
        public String toString() {
            return kind + "#" + id;
        }
    }
}
