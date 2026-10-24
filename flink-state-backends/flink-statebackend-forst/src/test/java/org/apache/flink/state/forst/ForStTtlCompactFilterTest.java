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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapStateDescriptor;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.asyncprocessing.EpochManager;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateExecutionController;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.internal.InternalListState;
import org.apache.flink.runtime.state.v2.internal.InternalMapState;
import org.apache.flink.runtime.state.v2.internal.InternalValueState;
import org.apache.flink.streaming.runtime.tasks.StreamTaskActionExecutor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorImpl;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the TTL compaction filter is actually configured (and therefore removes expired
 * entries during compaction) for states created through the State V2 API on {@link
 * ForStKeyedStateBackend}.
 */
class ForStTtlCompactFilterTest {

    private static final Duration TTL = Duration.ofMillis(1000);

    private final AtomicLong currentTime = new AtomicLong(0L);

    private ForStKeyedStateBackend<String> keyedBackend;
    private StateExecutionController<String> aec;
    private RecordContext<String> context;
    private MockEnvironment env;

    @BeforeEach
    void setup(@TempDir File temporaryFolder) throws Exception {
        FileSystem.initialize(new Configuration(), null);
        Configuration configuration = new Configuration();
        configuration.set(ForStOptions.PRIMARY_DIRECTORY, temporaryFolder.toURI().toString());
        ForStStateBackend forStStateBackend =
                new ForStStateBackend().configure(configuration, null);

        env = ForStStateTestBase.getMockEnvironment(temporaryFolder);

        TtlTimeProvider timeProvider = currentTime::get;
        keyedBackend =
                ForStTestUtils.createKeyedStateBackend(
                        forStStateBackend,
                        env,
                        StringSerializer.INSTANCE,
                        Collections.emptyList(),
                        timeProvider);

        MailboxExecutor mailboxExecutor =
                new MailboxExecutorImpl(
                        new TaskMailboxImpl(), 0, StreamTaskActionExecutor.IMMEDIATE);
        aec =
                new StateExecutionController<>(
                        mailboxExecutor,
                        (a, b) -> {},
                        keyedBackend.createStateExecutor(),
                        new DeclarationManager(),
                        EpochManager.ParallelMode.SERIAL_BETWEEN_EPOCH,
                        1,
                        100,
                        0,
                        1,
                        null,
                        null);
        keyedBackend.setup(aec);
    }

    @AfterEach
    void tearDown() throws Exception {
        keyedBackend.close();
        env.close();
    }

    /**
     * ReturnExpiredIfNotCleanedUp: the read path does not hide expired entries, so an entry that is
     * gone after compaction proves that the compaction filter removed it physically.
     */
    private static StateTtlConfig ttlConfig() {
        return StateTtlConfig.newBuilder(TTL)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                .cleanupInRocksdbCompactFilter(1L)
                .build();
    }

    @Test
    void testExpiredEntriesAreRemovedByCompaction() throws Exception {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("ttl-value-state", StringSerializer.INSTANCE);
        descriptor.enableTimeToLive(ttlConfig());

        InternalValueState<String, VoidNamespace, String> state =
                keyedBackend.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        currentTime.set(0L);
        for (String key : new String[] {"k1", "k2", "k3"}) {
            setCurrentContext(key);
            state.update("v-" + key);
            drain();
        }

        // Move the clock past the TTL and write one fresh entry: the old ones are expired, the
        // new one is not.
        currentTime.set(TTL.toMillis() + 1);
        setCurrentContext("k4");
        state.update("v-k4");
        drain();

        // Without a configured compaction filter this is a plain rewrite and nothing is dropped.
        keyedBackend.compactState(descriptor);

        for (String key : new String[] {"k1", "k2", "k3"}) {
            setCurrentContext(key);
            assertThat(state.value()).as("expired entry %s should be removed", key).isNull();
            drain();
        }
        setCurrentContext("k4");
        assertThat(state.value()).isEqualTo("v-k4");
        drain();
    }

    /**
     * A V2 list state is registered with the element serializer itself (a TtlSerializer), not a
     * ListSerializer as in V1 — configuring the filter must not assume the V1 shape.
     * Variable-length elements take the element-filter path of the native filter.
     */
    @Test
    void testExpiredListElementsAreRemovedByCompaction() throws Exception {
        testExpiredListElementsAreRemovedByCompaction(
                "ttl-list-state-var", StringSerializer.INSTANCE, "a", "b", "c");
    }

    /** Fixed-length elements take the fixed-element-length path of the native filter. */
    @Test
    void testExpiredFixedLengthListElementsAreRemovedByCompaction() throws Exception {
        testExpiredListElementsAreRemovedByCompaction(
                "ttl-list-state-fixed", LongSerializer.INSTANCE, 1L, 2L, 3L);
    }

    /**
     * Map entries carry their own TTL timestamp behind the null flag of the serialized user value;
     * the filter is configured in map mode for that layout.
     */
    @Test
    void testExpiredMapEntriesAreRemovedByCompaction() throws Exception {
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>(
                        "ttl-map-state", StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        descriptor.enableTimeToLive(ttlConfig());

        InternalMapState<String, VoidNamespace, String, String> state =
                keyedBackend.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        currentTime.set(0L);
        setCurrentContext("k1");
        state.put("uk1", "v1");
        state.put("uk2", "v2");
        drain();

        currentTime.set(TTL.toMillis() + 1);
        setCurrentContext("k1");
        state.put("uk3", "v3");
        drain();

        keyedBackend.compactState(descriptor);

        setCurrentContext("k1");
        assertThat(state.get("uk1")).as("expired entry uk1 should be removed").isNull();
        assertThat(state.get("uk2")).as("expired entry uk2 should be removed").isNull();
        assertThat(state.get("uk3")).isEqualTo("v3");
        drain();
    }

    private <E> void testExpiredListElementsAreRemovedByCompaction(
            String stateName, TypeSerializer<E> elementSerializer, E e1, E e2, E fresh)
            throws Exception {
        ListStateDescriptor<E> descriptor = new ListStateDescriptor<>(stateName, elementSerializer);
        descriptor.enableTimeToLive(ttlConfig());

        InternalListState<String, VoidNamespace, E> state =
                keyedBackend.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        // k1: two elements that will expire, plus one fresh element added after the TTL.
        // k2: only expired elements.
        currentTime.set(0L);
        setCurrentContext("k1");
        state.update(Arrays.asList(e1, e2));
        drain();
        setCurrentContext("k2");
        state.update(Arrays.asList(e1, e2));
        drain();

        currentTime.set(TTL.toMillis() + 1);
        setCurrentContext("k1");
        state.add(fresh);
        drain();

        keyedBackend.compactState(descriptor);

        setCurrentContext("k1");
        assertThat(state.get())
                .as("expired elements of k1 should be removed, the fresh one kept")
                .containsExactly(fresh);
        drain();
        setCurrentContext("k2");
        assertThat(state.get()).as("k2 should have no elements left").isEmpty();
        drain();
    }

    private void setCurrentContext(String key) {
        context = aec.buildContext(key, key);
        context.retain();
        aec.setCurrentContext(context);
    }

    private void drain() {
        context.release();
        aec.drainInflightRecords(0);
    }
}
