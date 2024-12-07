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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.SubEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.utils.CepOperatorTestUtilities;
import org.apache.flink.mock.Whitebox;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.apache.flink.cep.utils.CepOperatorBuilder.createOperatorForNFA;
import static org.apache.flink.cep.utils.EventBuilder.event;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.validateMockitoUsage;
import static org.mockito.Mockito.verify;

/** Tests for {@link CepOperator}. */
public class CEPOperatorTest extends TestLogger {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @After
    public void validate() {
        validateMockitoUsage();
    }

    @Test
    public void testKeyedCEPOperatorWatermarkForwarding() throws Exception {

        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                getCepTestHarness(false);

        try {
            harness.open();

            Watermark expectedWatermark = new Watermark(42L);

            harness.processWatermark(expectedWatermark);

            verifyWatermark(harness.getOutput().poll(), 42L);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testProcessingTimestampisPassedToNFA() throws Exception {

        final NFA<Event> nfa =
                NFACompiler.compileFactory(Pattern.<Event>begin("begin"), true).createNFA();
        final NFA<Event> spyNFA = spy(nfa);

        try (OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(createOperatorForNFA(spyNFA).build())) {

            long timestamp = 5;
            harness.open();
            harness.setProcessingTime(timestamp);
            StreamRecord<Event> event = event().withTimestamp(3).asStreamRecord();
            harness.processElement(event);
            verify(spyNFA)
                    .process(
                            any(SharedBufferAccessor.class),
                            any(NFAState.class),
                            eq(event.getValue()),
                            eq(timestamp),
                            any(AfterMatchSkipStrategy.class),
                            any(TimerService.class));
        }
    }

    @Test
    public void testKeyedCEPOperatorCheckpointing() throws Exception {

        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                getCepTestHarness(false);

        try {
            harness.open();

            Event startEvent = new Event(42, "start", 1.0);
            SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
            Event endEvent = new Event(42, "end", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

            // simulate snapshot/restore with some elements in internal sorting queue
            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            harness = getCepTestHarness(false);

            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processWatermark(new Watermark(Long.MIN_VALUE));

            harness.processElement(
                    new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

            // if element timestamps are not correctly checkpointed/restored this will lead to
            // a pruning time underflow exception in NFA
            harness.processWatermark(new Watermark(2L));

            harness.processElement(new StreamRecord<Event>(middleEvent, 3L));
            harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4L));
            harness.processElement(new StreamRecord<>(endEvent, 5L));

            // simulate snapshot/restore with empty element queue but NFA state
            OperatorSubtaskState snapshot2 = harness.snapshot(1L, 1L);
            harness.close();

            harness = getCepTestHarness(false);

            harness.setup();
            harness.initializeState(snapshot2);
            harness.open();

            harness.processWatermark(new Watermark(Long.MAX_VALUE));

            // get and verify the output

            Queue<Object> result = harness.getOutput();

            assertEquals(2, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
            verifyWatermark(result.poll(), Long.MAX_VALUE);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testKeyedCEPOperatorCheckpointingWithRocksDB() throws Exception {

        String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
        EmbeddedRocksDBStateBackend rocksDBStateBackend =
                new EmbeddedRocksDBStateBackend(TernaryBoolean.FALSE);
        rocksDBStateBackend.setDbStoragePath(rocksDbPath);

        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                getCepTestHarness(false);

        try {
            harness.setStateBackend(rocksDBStateBackend);
            harness.setCheckpointStorage(new JobManagerCheckpointStorage());

            harness.open();

            Event startEvent = new Event(42, "start", 1.0);
            SubEvent middleEvent = new SubEvent(42, "foo", 1.0, 10.0);
            Event endEvent = new Event(42, "end", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

            // simulate snapshot/restore with some elements in internal sorting queue
            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            harness = getCepTestHarness(false);

            rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
            rocksDBStateBackend.setDbStoragePath(rocksDbPath);
            harness.setStateBackend(rocksDBStateBackend);

            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processWatermark(new Watermark(Long.MIN_VALUE));

            harness.processElement(
                    new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

            // if element timestamps are not correctly checkpointed/restored this will lead to
            // a pruning time underflow exception in NFA
            harness.processWatermark(new Watermark(2L));

            // simulate snapshot/restore with empty element queue but NFA state
            OperatorSubtaskState snapshot2 = harness.snapshot(1L, 1L);
            harness.close();

            harness = getCepTestHarness(false);

            rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
            rocksDBStateBackend.setDbStoragePath(rocksDbPath);
            harness.setStateBackend(rocksDBStateBackend);
            harness.setup();
            harness.initializeState(snapshot2);
            harness.open();

            harness.processElement(new StreamRecord<Event>(middleEvent, 3L));
            harness.processElement(new StreamRecord<>(new Event(42, "start", 1.0), 4L));
            harness.processElement(new StreamRecord<>(endEvent, 5L));

            harness.processWatermark(new Watermark(Long.MAX_VALUE));

            // get and verify the output

            Queue<Object> result = harness.getOutput();

            assertEquals(2, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
            verifyWatermark(result.poll(), Long.MAX_VALUE);
        } finally {
            harness.close();
        }
    }

    /**
     * Tests that the internal time of a CEP operator advances only given watermarks. See FLINK-5033
     */
    @Test
    public void testKeyedAdvancingTimeWithoutElements() throws Exception {
        final Event startEvent = new Event(42, "start", 1.0);
        final long watermarkTimestamp1 = 5L;
        final long watermarkTimestamp2 = 13L;

        final Map<String, List<Event>> expectedSequence = new HashMap<>(2);
        expectedSequence.put("start", Collections.<Event>singletonList(startEvent));

        final OutputTag<Tuple2<Map<String, List<Event>>, Long>> timedOut =
                new OutputTag<Tuple2<Map<String, List<Event>>, Long>>("timedOut") {};
        final KeyedOneInputStreamOperatorTestHarness<Integer, Event, Map<String, List<Event>>>
                harness =
                        new KeyedOneInputStreamOperatorTestHarness<>(
                                new CepOperator<>(
                                        Event.createTypeSerializer(),
                                        false,
                                        new NFAFactory(true),
                                        null,
                                        null,
                                        new TimedOutProcessFunction(timedOut),
                                        null),
                                new KeySelector<Event, Integer>() {
                                    private static final long serialVersionUID =
                                            7219185117566268366L;

                                    @Override
                                    public Integer getKey(Event value) throws Exception {
                                        return value.getId();
                                    }
                                },
                                BasicTypeInfo.INT_TYPE_INFO);

        try {
            String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
            EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
            rocksDBStateBackend.setDbStoragePath(rocksDbPath);

            harness.setStateBackend(rocksDBStateBackend);
            harness.setCheckpointStorage(new JobManagerCheckpointStorage());
            harness.setup(
                    new KryoSerializer<>(
                            (Class<Map<String, List<Event>>>) (Object) Map.class,
                            new SerializerConfigImpl()));
            harness.open();

            harness.processElement(new StreamRecord<>(startEvent, 3L));
            harness.processWatermark(new Watermark(watermarkTimestamp1));
            harness.processWatermark(new Watermark(watermarkTimestamp2));

            Queue<Object> result = harness.getOutput();
            Queue<StreamRecord<Tuple2<Map<String, List<Event>>, Long>>> sideOutput =
                    harness.getSideOutput(timedOut);

            assertEquals(2L, result.size());
            assertEquals(1L, sideOutput.size());

            Object watermark1 = result.poll();

            assertTrue(watermark1 instanceof Watermark);

            assertEquals(watermarkTimestamp1, ((Watermark) watermark1).getTimestamp());

            Tuple2<Map<String, List<Event>>, Long> leftResult = sideOutput.poll().getValue();

            assertEquals(watermarkTimestamp2, (long) leftResult.f1);
            assertEquals(expectedSequence, leftResult.f0);

            Object watermark2 = result.poll();

            assertTrue(watermark2 instanceof Watermark);

            assertEquals(watermarkTimestamp2, ((Watermark) watermark2).getTimestamp());
        } finally {
            harness.close();
        }
    }

    @Test
    public void testKeyedCEPOperatorNFAUpdate() throws Exception {

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            Event startEvent = new Event(42, "c", 1.0);
            SubEvent middleEvent = new SubEvent(42, "a", 1.0, 10.0);
            Event endEvent = new Event(42, "b", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));

            // simulate snapshot/restore with some elements in internal sorting queue
            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            operator = CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
            harness = CepOperatorTestUtilities.getCepTestHarness(operator);

            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processElement(new StreamRecord<>(new Event(42, "d", 1.0), 4L));
            OperatorSubtaskState snapshot2 = harness.snapshot(0L, 0L);
            harness.close();

            operator = CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
            harness = CepOperatorTestUtilities.getCepTestHarness(operator);

            harness.setup();
            harness.initializeState(snapshot2);
            harness.open();

            harness.processElement(new StreamRecord<Event>(middleEvent, 4L));
            harness.processElement(new StreamRecord<>(endEvent, 4L));

            // get and verify the output

            Queue<Object> result = harness.getOutput();

            assertEquals(1, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testKeyedCEPOperatorNFAUpdateWithRocksDB() throws Exception {

        String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
        EmbeddedRocksDBStateBackend rocksDBStateBackend =
                new EmbeddedRocksDBStateBackend(TernaryBoolean.FALSE);
        rocksDBStateBackend.setDbStoragePath(rocksDbPath);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.setStateBackend(rocksDBStateBackend);
            harness.setCheckpointStorage(new JobManagerCheckpointStorage());

            harness.open();

            Event startEvent = new Event(42, "c", 1.0);
            SubEvent middleEvent = new SubEvent(42, "a", 1.0, 10.0);
            Event endEvent = new Event(42, "b", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));

            // simulate snapshot/restore with some elements in internal sorting queue
            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            operator = CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
            harness = CepOperatorTestUtilities.getCepTestHarness(operator);

            rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
            rocksDBStateBackend.setDbStoragePath(rocksDbPath);
            harness.setStateBackend(rocksDBStateBackend);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processElement(new StreamRecord<>(new Event(42, "d", 1.0), 4L));
            OperatorSubtaskState snapshot2 = harness.snapshot(0L, 0L);
            harness.close();

            operator = CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
            harness = CepOperatorTestUtilities.getCepTestHarness(operator);

            rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
            rocksDBStateBackend.setDbStoragePath(rocksDbPath);
            harness.setStateBackend(rocksDBStateBackend);
            harness.setup();
            harness.initializeState(snapshot2);
            harness.open();

            harness.processElement(new StreamRecord<Event>(middleEvent, 4L));
            harness.processElement(new StreamRecord<>(endEvent, 4L));

            // get and verify the output

            Queue<Object> result = harness.getOutput();

            assertEquals(1, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testKeyedCEPOperatorNFAUpdateTimes() throws Exception {
        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            final ValueState nfaOperatorState =
                    Whitebox.getInternalState(operator, "computationStates");
            final ValueState nfaOperatorStateSpy = Mockito.spy(nfaOperatorState);
            Whitebox.setInternalState(operator, "computationStates", nfaOperatorStateSpy);

            Event startEvent = new Event(42, "c", 1.0);
            SubEvent middleEvent = new SubEvent(42, "a", 1.0, 10.0);
            Event endEvent = new Event(42, "b", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "d", 1.0), 4L));
            harness.processElement(new StreamRecord<Event>(middleEvent, 4L));
            harness.processElement(new StreamRecord<>(endEvent, 4L));

            // verify the number of invocations NFA is updated
            Mockito.verify(nfaOperatorStateSpy, Mockito.times(3)).update(Mockito.any());

            // get and verify the output
            Queue<Object> result = harness.getOutput();

            assertEquals(1, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testKeyedCEPOperatorNFAUpdateTimesWithRocksDB() throws Exception {

        String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        rocksDBStateBackend.setDbStoragePath(rocksDbPath);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(true, new SimpleNFAFactory());
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.setStateBackend(rocksDBStateBackend);
            harness.setCheckpointStorage(new JobManagerCheckpointStorage());

            harness.open();

            final ValueState nfaOperatorState =
                    Whitebox.getInternalState(operator, "computationStates");
            final ValueState nfaOperatorStateSpy = Mockito.spy(nfaOperatorState);
            Whitebox.setInternalState(operator, "computationStates", nfaOperatorStateSpy);

            Event startEvent = new Event(42, "c", 1.0);
            SubEvent middleEvent = new SubEvent(42, "a", 1.0, 10.0);
            Event endEvent = new Event(42, "b", 1.0);

            harness.processElement(new StreamRecord<>(startEvent, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "d", 1.0), 4L));
            harness.processElement(new StreamRecord<Event>(middleEvent, 4L));
            harness.processElement(new StreamRecord<>(endEvent, 4L));

            // verify the number of invocations NFA is updated
            Mockito.verify(nfaOperatorStateSpy, Mockito.times(3)).update(Mockito.any());

            // get and verify the output
            Queue<Object> result = harness.getOutput();

            assertEquals(1, result.size());

            verifyPattern(result.poll(), startEvent, middleEvent, endEvent);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorCleanupEventTime() throws Exception {

        Event startEvent1 = new Event(42, "start", 1.0);
        Event startEvent2 = new Event(42, "start", 2.0);
        SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
        SubEvent middleEvent3 = new SubEvent(42, "foo3", 1.0, 10.0);
        Event endEvent1 = new Event(42, "end", 1.0);
        Event endEvent2 = new Event(42, "end", 2.0);

        Event startEventK2 = new Event(43, "start", 1.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator = getKeyedCepOperator(false);
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            harness.processWatermark(new Watermark(Long.MIN_VALUE));

            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
            harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
            harness.processElement(
                    new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));
            harness.processElement(new StreamRecord<>(startEvent1, 1L));
            harness.processElement(new StreamRecord<>(startEventK2, 1L));

            // there must be 2 keys 42, 43 registered for the watermark callback
            // all the seen elements must be in the priority queues but no NFA yet.

            assertEquals(4L, harness.numEventTimeTimers());
            assertEquals(4L, operator.getPQSize(42));
            assertEquals(1L, operator.getPQSize(43));
            assertTrue(!operator.hasNonEmptySharedBuffer(42));
            assertTrue(!operator.hasNonEmptySharedBuffer(43));

            harness.processWatermark(new Watermark(2L));

            verifyWatermark(harness.getOutput().poll(), Long.MIN_VALUE);
            verifyWatermark(harness.getOutput().poll(), 2L);

            // still the 2 keys
            // one element in PQ for 42 (the barfoo) as it arrived early
            // for 43 the element entered the NFA and the PQ is empty

            assertEquals(4L, harness.numEventTimeTimers());
            assertTrue(operator.hasNonEmptySharedBuffer(42));
            assertEquals(1L, operator.getPQSize(42));
            assertTrue(operator.hasNonEmptySharedBuffer(43));
            assertTrue(!operator.hasNonEmptyPQ(43));

            harness.processElement(new StreamRecord<>(startEvent2, 4L));
            harness.processElement(new StreamRecord<Event>(middleEvent2, 5L));

            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            CepOperator<Event, Integer, Map<String, List<Event>>> operator2 =
                    getKeyedCepOperator(false);
            harness = CepOperatorTestUtilities.getCepTestHarness(operator2);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processElement(new StreamRecord<>(endEvent1, 6L));
            harness.processWatermark(11L);
            harness.processWatermark(12L);

            // now we have 1 key because the 43 expired and was removed.
            // 42 is still there due to startEvent2
            assertEquals(3L, harness.numEventTimeTimers());
            assertTrue(operator2.hasNonEmptySharedBuffer(42));
            assertTrue(!operator2.hasNonEmptyPQ(42));
            assertTrue(!operator2.hasNonEmptySharedBuffer(43));
            assertTrue(!operator2.hasNonEmptyPQ(43));

            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);
            verifyWatermark(harness.getOutput().poll(), 11L);
            verifyWatermark(harness.getOutput().poll(), 12L);

            // this is a late event, because timestamp(12) = last watermark(12)
            harness.processElement(new StreamRecord<Event>(middleEvent3, 12L));
            harness.processElement(new StreamRecord<>(endEvent2, 13L));
            harness.processWatermark(20L);
            harness.processWatermark(21L);

            assertTrue(!operator2.hasNonEmptySharedBuffer(42));
            assertTrue(!operator2.hasNonEmptyPQ(42));
            assertEquals(0L, harness.numEventTimeTimers());

            assertEquals(3, harness.getOutput().size());
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent2);

            verifyWatermark(harness.getOutput().poll(), 20L);
            verifyWatermark(harness.getOutput().poll(), 21L);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorCleanupEventTimeWithSameElements() throws Exception {

        Event startEvent = new Event(41, "c", 1.0);
        Event middle1Event1 = new Event(41, "a", 2.0);
        Event middle1Event2 = new Event(41, "a", 3.0);
        Event middle1Event3 = new Event(41, "a", 4.0);
        Event middle2Event1 = new Event(41, "b", 5.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(false, new ComplexNFAFactory());
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            harness.processWatermark(new Watermark(Long.MIN_VALUE));

            harness.processElement(new StreamRecord<>(middle2Event1, 6));
            harness.processElement(new StreamRecord<>(middle1Event3, 7));
            harness.processElement(new StreamRecord<>(startEvent, 1));
            harness.processElement(new StreamRecord<>(middle1Event1, 3));
            harness.processElement(new StreamRecord<>(middle1Event2, 3));
            harness.processElement(new StreamRecord<>(middle1Event1, 3));
            harness.processElement(new StreamRecord<>(new Event(41, "d", 6.0), 5));

            assertEquals(5L, harness.numEventTimeTimers());
            assertEquals(7L, operator.getPQSize(41));
            assertTrue(!operator.hasNonEmptySharedBuffer(41));

            harness.processWatermark(new Watermark(2L));

            verifyWatermark(harness.getOutput().poll(), Long.MIN_VALUE);
            verifyWatermark(harness.getOutput().poll(), 2L);

            assertEquals(5L, harness.numEventTimeTimers());
            assertEquals(6L, operator.getPQSize(41));
            assertTrue(operator.hasNonEmptySharedBuffer(41)); // processed the first element

            harness.processWatermark(new Watermark(8L));

            List<List<Event>> resultingPatterns = new ArrayList<>();
            while (!harness.getOutput().isEmpty()) {
                Object o = harness.getOutput().poll();
                if (!(o instanceof Watermark)) {
                    StreamRecord<Map<String, List<Event>>> el =
                            (StreamRecord<Map<String, List<Event>>>) o;
                    List<Event> res = new ArrayList<>();
                    for (List<Event> le : el.getValue().values()) {
                        res.addAll(le);
                    }
                    resultingPatterns.add(res);
                } else {
                    verifyWatermark(o, 8L);
                }
            }

            compareMaps(
                    resultingPatterns,
                    Lists.<List<Event>>newArrayList(
                            Lists.newArrayList(startEvent, middle1Event1),
                            Lists.newArrayList(startEvent, middle1Event1, middle1Event2),
                            Lists.newArrayList(startEvent, middle2Event1, middle1Event3),
                            Lists.newArrayList(
                                    startEvent, middle1Event1, middle1Event2, middle1Event1),
                            Lists.newArrayList(
                                    startEvent, middle1Event1, middle2Event1, middle1Event3),
                            Lists.newArrayList(
                                    startEvent,
                                    middle1Event1,
                                    middle1Event1,
                                    middle1Event2,
                                    middle1Event3),
                            Lists.newArrayList(
                                    startEvent,
                                    middle1Event1,
                                    middle1Event2,
                                    middle2Event1,
                                    middle1Event3),
                            Lists.newArrayList(
                                    startEvent,
                                    middle1Event1,
                                    middle1Event1,
                                    middle1Event2,
                                    middle2Event1,
                                    middle1Event3)));

            assertEquals(1L, harness.numEventTimeTimers());
            assertEquals(0L, operator.getPQSize(41));
            assertTrue(operator.hasNonEmptySharedBuffer(41));

            harness.processWatermark(new Watermark(17L));
            verifyWatermark(harness.getOutput().poll(), 17L);

            assertTrue(!operator.hasNonEmptySharedBuffer(41));
            assertTrue(!operator.hasNonEmptyPQ(41));
            assertEquals(0L, harness.numEventTimeTimers());
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorSideOutputLateElementsEventTime() throws Exception {

        Event startEvent = new Event(41, "c", 1.0);
        Event middle1Event1 = new Event(41, "a", 2.0);
        Event middle1Event2 = new Event(41, "a", 3.0);
        Event middle1Event3 = new Event(41, "a", 4.0);

        OutputTag<Event> lateDataTag =
                new OutputTag<Event>("late-data", TypeInformation.of(Event.class));

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(
                        false, new ComplexNFAFactory(), null, lateDataTag);
        try (OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator)) {

            harness.open();

            harness.processWatermark(new Watermark(Long.MIN_VALUE));
            harness.processElement(new StreamRecord<>(startEvent, 6));

            verifyWatermark(harness.getOutput().poll(), Long.MIN_VALUE);
            harness.processWatermark(new Watermark(6L));
            verifyWatermark(harness.getOutput().poll(), 6L);

            harness.processElement(new StreamRecord<>(middle1Event1, 4));
            harness.processElement(new StreamRecord<>(middle1Event2, 5));
            harness.processElement(new StreamRecord<>(middle1Event3, 7));

            List<Event> late = new ArrayList<>();

            while (!harness.getSideOutput(lateDataTag).isEmpty()) {
                StreamRecord<Event> eventStreamRecord = harness.getSideOutput(lateDataTag).poll();
                late.add(eventStreamRecord.getValue());
            }

            List<Event> expected = Lists.newArrayList(middle1Event1, middle1Event2);
            Assert.assertArrayEquals(expected.toArray(), late.toArray());
        }
    }

    @Test
    public void testCEPOperatorLateRecordsMetric() throws Exception {
        Event startEvent = new Event(41, "c", 1.0);
        Event middle1Event1 = new Event(41, "a", 2.0);
        Event middle1Event2 = new Event(41, "a", 3.0);
        Event middle1Event3 = new Event(41, "a", 4.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator = getKeyedCepOperator(false);
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);
        try {
            harness.open();
            harness.processWatermark(0);
            harness.processElement(startEvent, 1L);
            harness.processWatermark(2L);
            harness.processElement(middle1Event1, 1L);
            harness.processElement(middle1Event2, 3L);
            harness.processWatermark(4L);
            harness.processElement(middle1Event3, 3L);

            assertEquals(2L, operator.getLateRecordsNumber());
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorCleanupProcessingTime() throws Exception {

        Event startEvent1 = new Event(42, "start", 1.0);
        Event startEvent2 = new Event(42, "start", 2.0);
        SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 1.0, 10.0);
        SubEvent middleEvent3 = new SubEvent(42, "foo3", 1.0, 10.0);
        Event endEvent1 = new Event(42, "end", 1.0);
        Event endEvent2 = new Event(42, "end", 2.0);

        Event startEventK2 = new Event(43, "start", 1.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator = getKeyedCepOperator(true);
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            harness.setProcessingTime(0L);

            harness.processElement(new StreamRecord<>(startEvent1, 1L));
            harness.processElement(new StreamRecord<>(startEventK2, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
            harness.processElement(new StreamRecord<Event>(middleEvent1, 2L));
            harness.processElement(
                    new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

            assertTrue(!operator.hasNonEmptyPQ(42));
            assertTrue(!operator.hasNonEmptyPQ(43));
            assertTrue(operator.hasNonEmptySharedBuffer(42));
            assertTrue(operator.hasNonEmptySharedBuffer(43));

            harness.setProcessingTime(3L);

            harness.processElement(new StreamRecord<>(startEvent2, 3L));
            harness.processElement(new StreamRecord<Event>(middleEvent2, 4L));

            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            CepOperator<Event, Integer, Map<String, List<Event>>> operator2 =
                    getKeyedCepOperator(true);
            harness = CepOperatorTestUtilities.getCepTestHarness(operator2);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.setProcessingTime(3L);
            harness.processElement(new StreamRecord<>(endEvent1, 5L));

            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);

            harness.setProcessingTime(11L);

            harness.processElement(new StreamRecord<Event>(middleEvent3, 11L));
            harness.processElement(new StreamRecord<>(endEvent2, 12L));

            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent2);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent3, endEvent2);

            harness.setProcessingTime(21L);

            assertTrue(!operator2.hasNonEmptySharedBuffer(42));

            harness.processElement(new StreamRecord<>(startEvent1, 21L));
            assertTrue(operator2.hasNonEmptySharedBuffer(42));

            harness.setProcessingTime(49L);

            // TODO: 3/13/17 we have to have another event in order to clean up
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));

            // the pattern expired
            assertTrue(!operator2.hasNonEmptySharedBuffer(42));

            assertEquals(0L, harness.numEventTimeTimers());
            assertTrue(!operator2.hasNonEmptyPQ(42));
            assertTrue(!operator2.hasNonEmptyPQ(43));
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorSerializationWRocksDB() throws Exception {
        String rocksDbPath = tempFolder.newFolder().getAbsolutePath();
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();
        rocksDBStateBackend.setDbStoragePath(rocksDbPath);

        final Event startEvent1 = new Event(40, "start", 1.0);
        final Event startEvent2 = new Event(40, "start", 2.0);
        final SubEvent middleEvent1 = new SubEvent(40, "foo1", 1.0, 10);
        final SubEvent middleEvent2 = new SubEvent(40, "foo2", 2.0, 10);
        final SubEvent middleEvent3 = new SubEvent(40, "foo3", 3.0, 10);
        final SubEvent middleEvent4 = new SubEvent(40, "foo4", 1.0, 10);
        final Event nextOne = new Event(40, "next-one", 1.0);
        final Event endEvent = new Event(40, "end", 1.0);

        final Pattern<Event, ?> pattern =
                Pattern.<Event>begin("start")
                        .where(SimpleCondition.of(value -> value.getName().equals("start")))
                        .followedBy("middle")
                        .subtype(SubEvent.class)
                        .where(
                                new IterativeCondition<SubEvent>() {

                                    private static final long serialVersionUID =
                                            6215754202506583964L;

                                    @Override
                                    public boolean filter(SubEvent value, Context<SubEvent> ctx)
                                            throws Exception {
                                        if (!value.getName().startsWith("foo")) {
                                            return false;
                                        }

                                        double sum = 0.0;
                                        for (Event event : ctx.getEventsForPattern("middle")) {
                                            sum += event.getPrice();
                                        }
                                        sum += value.getPrice();
                                        return Double.compare(sum, 5.0) < 0;
                                    }
                                })
                        .oneOrMore()
                        .allowCombinations()
                        .followedBy("end")
                        .where(SimpleCondition.of(value -> value.getName().equals("end")));

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                CepOperatorTestUtilities.getKeyedCepOperator(
                        false,
                        new NFACompiler.NFAFactory<Event>() {
                            private static final long serialVersionUID = 477082663248051994L;

                            @Override
                            public NFA<Event> createNFA() {
                                return NFACompiler.compileFactory(pattern, false).createNFA();
                            }
                        });

        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.setStateBackend(rocksDBStateBackend);
            harness.setCheckpointStorage(new JobManagerCheckpointStorage());
            harness.open();

            harness.processWatermark(0L);
            harness.processElement(new StreamRecord<>(startEvent1, 1));
            harness.processElement(new StreamRecord<Event>(middleEvent1, 2));
            harness.processWatermark(2L);
            harness.processElement(new StreamRecord<Event>(middleEvent3, 5));
            harness.processElement(new StreamRecord<Event>(middleEvent2, 3));
            harness.processElement(new StreamRecord<>(startEvent2, 4));
            harness.processWatermark(5L);
            harness.processElement(new StreamRecord<>(nextOne, 7));
            harness.processElement(new StreamRecord<>(endEvent, 8));
            harness.processElement(new StreamRecord<Event>(middleEvent4, 6));
            harness.processWatermark(100L);

            List<List<Event>> resultingPatterns = new ArrayList<>();
            while (!harness.getOutput().isEmpty()) {
                Object o = harness.getOutput().poll();
                if (!(o instanceof Watermark)) {
                    StreamRecord<Map<String, List<Event>>> el =
                            (StreamRecord<Map<String, List<Event>>>) o;
                    List<Event> res = new ArrayList<>();
                    for (List<Event> le : el.getValue().values()) {
                        res.addAll(le);
                    }
                    resultingPatterns.add(res);
                }
            }

            compareMaps(
                    resultingPatterns,
                    Lists.<List<Event>>newArrayList(
                            Lists.newArrayList(
                                    startEvent1,
                                    endEvent,
                                    middleEvent1,
                                    middleEvent2,
                                    middleEvent4),
                            Lists.newArrayList(startEvent1, endEvent, middleEvent2, middleEvent1),
                            Lists.newArrayList(startEvent1, endEvent, middleEvent3, middleEvent1),
                            Lists.newArrayList(startEvent2, endEvent, middleEvent3, middleEvent4),
                            Lists.newArrayList(startEvent1, endEvent, middleEvent4, middleEvent1),
                            Lists.newArrayList(startEvent1, endEvent, middleEvent1),
                            Lists.newArrayList(startEvent2, endEvent, middleEvent3)));
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorComparatorProcessTime() throws Exception {
        Event startEvent1 = new Event(42, "start", 1.0);
        Event startEvent2 = new Event(42, "start", 2.0);
        SubEvent middleEvent1 = new SubEvent(42, "foo1", 3.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 4.0, 10.0);
        Event endEvent1 = new Event(42, "end", 1.0);

        Event startEventK2 = new Event(43, "start", 1.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                getKeyedCepOperatorWithComparator(true);
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            harness.setProcessingTime(0L);

            harness.processElement(new StreamRecord<>(startEvent1, 0L));
            harness.processElement(new StreamRecord<>(startEventK2, 0L));
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 0L));
            harness.processElement(new StreamRecord<>(new SubEvent(42, "barfoo", 1.0, 5.0), 0L));

            assertTrue(!operator.hasNonEmptySharedBuffer(42));
            assertTrue(!operator.hasNonEmptySharedBuffer(43));

            harness.setProcessingTime(3L);
            assertTrue(operator.hasNonEmptySharedBuffer(42));
            assertTrue(operator.hasNonEmptySharedBuffer(43));

            harness.processElement(new StreamRecord<>(middleEvent2, 3L));
            harness.processElement(new StreamRecord<>(middleEvent1, 3L));
            harness.processElement(new StreamRecord<>(startEvent2, 3L));

            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            CepOperator<Event, Integer, Map<String, List<Event>>> operator2 =
                    getKeyedCepOperatorWithComparator(true);
            harness = CepOperatorTestUtilities.getCepTestHarness(operator2);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.setProcessingTime(4L);
            harness.processElement(new StreamRecord<>(endEvent1, 5L));
            harness.setProcessingTime(5L);

            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent1, endEvent1);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent1);
        } finally {
            harness.close();
        }
    }

    @Test
    public void testCEPOperatorComparatorEventTime() throws Exception {
        Event startEvent1 = new Event(42, "start", 1.0);
        Event startEvent2 = new Event(42, "start", 2.0);
        SubEvent middleEvent1 = new SubEvent(42, "foo1", 1.0, 10.0);
        SubEvent middleEvent2 = new SubEvent(42, "foo2", 2.0, 10.0);
        Event endEvent = new Event(42, "end", 1.0);

        Event startEventK2 = new Event(43, "start", 1.0);

        CepOperator<Event, Integer, Map<String, List<Event>>> operator =
                getKeyedCepOperatorWithComparator(false);
        OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> harness =
                CepOperatorTestUtilities.getCepTestHarness(operator);

        try {
            harness.open();

            harness.processWatermark(0L);

            harness.processElement(new StreamRecord<>(startEvent1, 1L));
            harness.processElement(new StreamRecord<>(startEventK2, 1L));
            harness.processElement(new StreamRecord<>(new Event(42, "foobar", 1.0), 2L));
            harness.processElement(
                    new StreamRecord<Event>(new SubEvent(42, "barfoo", 1.0, 5.0), 3L));

            assertTrue(operator.hasNonEmptyPQ(42));
            assertTrue(operator.hasNonEmptyPQ(43));
            assertFalse(operator.hasNonEmptySharedBuffer(42));
            assertFalse(operator.hasNonEmptySharedBuffer(43));

            harness.processWatermark(3L);
            assertFalse(operator.hasNonEmptyPQ(42));
            assertFalse(operator.hasNonEmptyPQ(43));
            assertTrue(operator.hasNonEmptySharedBuffer(42));
            assertTrue(operator.hasNonEmptySharedBuffer(43));

            harness.processElement(new StreamRecord<>(startEvent2, 4L));
            harness.processElement(new StreamRecord<Event>(middleEvent2, 5L));
            harness.processElement(new StreamRecord<Event>(middleEvent1, 5L));

            OperatorSubtaskState snapshot = harness.snapshot(0L, 0L);
            harness.close();

            CepOperator<Event, Integer, Map<String, List<Event>>> operator2 =
                    getKeyedCepOperatorWithComparator(false);
            harness = CepOperatorTestUtilities.getCepTestHarness(operator2);
            harness.setup();
            harness.initializeState(snapshot);
            harness.open();

            harness.processElement(new StreamRecord<>(endEvent, 6L));
            harness.processWatermark(6L);

            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent1, endEvent);
            verifyPattern(harness.getOutput().poll(), startEvent1, middleEvent2, endEvent);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent1, endEvent);
            verifyPattern(harness.getOutput().poll(), startEvent2, middleEvent2, endEvent);
            verifyWatermark(harness.getOutput().poll(), 6L);
        } finally {
            harness.close();
        }
    }

    private void verifyWatermark(Object outputObject, long timestamp) {
        assertTrue(outputObject instanceof Watermark);
        assertEquals(timestamp, ((Watermark) outputObject).getTimestamp());
    }

    private void verifyPattern(Object outputObject, Event start, SubEvent middle, Event end) {
        assertTrue(outputObject instanceof StreamRecord);

        StreamRecord<?> resultRecord = (StreamRecord<?>) outputObject;
        assertTrue(resultRecord.getValue() instanceof Map);

        @SuppressWarnings("unchecked")
        Map<String, List<Event>> patternMap = (Map<String, List<Event>>) resultRecord.getValue();
        assertEquals(start, patternMap.get("start").get(0));
        assertEquals(middle, patternMap.get("middle").get(0));
        assertEquals(end, patternMap.get("end").get(0));
    }

    private CepOperator<Event, Integer, Map<String, List<Event>>> getKeyedCepOperator(
            boolean isProcessingTime) {
        return CepOperatorTestUtilities.getKeyedCepOperator(isProcessingTime, new NFAFactory());
    }

    private CepOperator<Event, Integer, Map<String, List<Event>>> getKeyedCepOperatorWithComparator(
            boolean isProcessingTime) {

        return CepOperatorTestUtilities.getKeyedCepOperator(
                isProcessingTime,
                new NFAFactory(),
                new org.apache.flink.cep.EventComparator<Event>() {
                    @Override
                    public int compare(Event o1, Event o2) {
                        return Double.compare(o1.getPrice(), o2.getPrice());
                    }
                });
    }

    private void compareMaps(List<List<Event>> actual, List<List<Event>> expected) {
        Assert.assertEquals(expected.size(), actual.size());

        for (List<Event> p : actual) {
            Collections.sort(p, new EventComparator());
        }

        for (List<Event> p : expected) {
            Collections.sort(p, new EventComparator());
        }

        Collections.sort(actual, new ListEventComparator());
        Collections.sort(expected, new ListEventComparator());
        Assert.assertArrayEquals(expected.toArray(), actual.toArray());
    }

    private class ListEventComparator implements Comparator<List<Event>> {

        @Override
        public int compare(List<Event> o1, List<Event> o2) {
            int sizeComp = Integer.compare(o1.size(), o2.size());
            if (sizeComp == 0) {
                EventComparator comp = new EventComparator();
                for (int i = 0; i < o1.size(); i++) {
                    int eventComp = comp.compare(o1.get(i), o2.get(i));
                    if (eventComp != 0) {
                        return eventComp;
                    }
                }
                return 0;
            } else {
                return sizeComp;
            }
        }
    }

    private class EventComparator implements Comparator<Event> {

        @Override
        public int compare(Event o1, Event o2) {
            int nameComp = o1.getName().compareTo(o2.getName());
            int priceComp = Double.compare(o1.getPrice(), o2.getPrice());
            int idComp = Integer.compare(o1.getId(), o2.getId());
            if (nameComp == 0) {
                if (priceComp == 0) {
                    return idComp;
                } else {
                    return priceComp;
                }
            } else {
                return nameComp;
            }
        }
    }

    private OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> getCepTestHarness(
            boolean isProcessingTime) throws Exception {
        return CepOperatorTestUtilities.getCepTestHarness(getKeyedCepOpearator(isProcessingTime));
    }

    private CepOperator<Event, Integer, Map<String, List<Event>>> getKeyedCepOpearator(
            boolean isProcessingTime) {
        return CepOperatorTestUtilities.getKeyedCepOperator(
                isProcessingTime, new CEPOperatorTest.NFAFactory());
    }

    private static class NFAFactory implements NFACompiler.NFAFactory<Event> {

        private static final long serialVersionUID = 1173020762472766713L;

        private final boolean handleTimeout;

        private NFAFactory() {
            this(false);
        }

        private NFAFactory(boolean handleTimeout) {
            this.handleTimeout = handleTimeout;
        }

        @Override
        public NFA<Event> createNFA() {

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("start")
                            .where(SimpleCondition.of(value -> value.getName().equals("start")))
                            .followedByAny("middle")
                            .subtype(SubEvent.class)
                            .where(SimpleCondition.of(value -> value.getVolume() > 5.0))
                            .followedByAny("end")
                            .where(SimpleCondition.of(value -> value.getName().equals("end")))
                            // add a window timeout to test whether timestamps of elements in the
                            // priority queue in CEP operator are correctly checkpointed/restored
                            .within(Duration.ofMillis(10L));

            return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
        }
    }

    private static class ComplexNFAFactory implements NFACompiler.NFAFactory<Event> {

        private static final long serialVersionUID = 1173020762472766713L;

        private final boolean handleTimeout;

        private ComplexNFAFactory() {
            this(false);
        }

        private ComplexNFAFactory(boolean handleTimeout) {
            this.handleTimeout = handleTimeout;
        }

        @Override
        public NFA<Event> createNFA() {

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("start")
                            .where(SimpleCondition.of(value -> value.getName().equals("c")))
                            .followedBy("middle1")
                            .where(SimpleCondition.of(value -> value.getName().equals("a")))
                            .oneOrMore()
                            .optional()
                            .followedBy("middle2")
                            .where(SimpleCondition.of(value -> value.getName().equals("b")))
                            .optional()
                            .followedBy("end")
                            .where(SimpleCondition.of(value -> value.getName().equals("a")))
                            .within(Duration.ofMillis(10L));

            return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
        }
    }

    private static class SimpleNFAFactory implements NFACompiler.NFAFactory<Event> {

        private static final long serialVersionUID = 1173020762472766713L;

        private final boolean handleTimeout;

        private SimpleNFAFactory() {
            this(false);
        }

        private SimpleNFAFactory(boolean handleTimeout) {
            this.handleTimeout = handleTimeout;
        }

        @Override
        public NFA<Event> createNFA() {

            Pattern<Event, ?> pattern =
                    Pattern.<Event>begin("start")
                            .where(SimpleCondition.of(value -> value.getName().equals("c")))
                            .followedBy("middle")
                            .where(SimpleCondition.of(value -> value.getName().equals("a")))
                            .followedBy("end")
                            .where(SimpleCondition.of(value -> value.getName().equals("b")))
                            .within(Duration.ofMillis(10L));

            return NFACompiler.compileFactory(pattern, handleTimeout).createNFA();
        }
    }

    private static class TimedOutProcessFunction
            extends PatternProcessFunction<Event, Map<String, List<Event>>>
            implements TimedOutPartialMatchHandler<Event> {

        private final OutputTag<Tuple2<Map<String, List<Event>>, Long>> timedOutTag;

        private TimedOutProcessFunction(
                OutputTag<Tuple2<Map<String, List<Event>>, Long>> timedOutTag) {
            this.timedOutTag = timedOutTag;
        }

        @Override
        public void processMatch(
                Map<String, List<Event>> match,
                PatternProcessFunction.Context ctx,
                Collector<Map<String, List<Event>>> out)
                throws Exception {
            out.collect(match);
        }

        @Override
        public void processTimedOutMatch(
                Map<String, List<Event>> match, PatternProcessFunction.Context ctx)
                throws Exception {
            ctx.output(timedOutTag, Tuple2.of(match, ctx.timestamp()));
        }
    }
}
