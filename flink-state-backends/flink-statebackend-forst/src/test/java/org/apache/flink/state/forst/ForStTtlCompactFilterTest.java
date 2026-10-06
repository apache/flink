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
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
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

    @Test
    void testExpiredEntriesAreRemovedByCompaction() throws Exception {
        // ReturnExpiredIfNotCleanedUp: the read path does not hide expired entries, so a null
        // value after compaction proves that the compaction filter removed the entry physically.
        StateTtlConfig ttlConfig =
                StateTtlConfig.newBuilder(TTL)
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(
                                StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                        .cleanupInRocksdbCompactFilter(1L)
                        .build();
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("ttl-value-state", StringSerializer.INSTANCE);
        descriptor.enableTimeToLive(ttlConfig);

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
