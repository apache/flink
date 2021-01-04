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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Integration tests for {@link OperatorStateBackend}. */
public class StateBackendITCase extends AbstractTestBase {

    /** Verify that the user-specified state backend is used even if checkpointing is disabled. */
    @Test
    public void testStateBackendWithoutCheckpointing() throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);

        see.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        see.setStateBackend(new FailingStateBackend());

        see.fromElements(new Tuple2<>("Hello", 1))
                .keyBy(0)
                .map(
                        new RichMapFunction<Tuple2<String, Integer>, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                super.open(parameters);
                                getRuntimeContext()
                                        .getState(
                                                new ValueStateDescriptor<>("Test", Integer.class));
                            }

                            @Override
                            public String map(Tuple2<String, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .print();

        try {
            see.execute();
            fail();
        } catch (JobExecutionException e) {
            assertTrue(ExceptionUtils.findThrowable(e, SuccessException.class).isPresent());
        }
    }

    private static class FailingStateBackend implements StateBackend {
        private static final long serialVersionUID = 1L;

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return new MemoryBackendCheckpointStorageAccess(jobId, null, null, 1_000_000);
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws IOException {
            throw new SuccessException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {

            throw new SuccessException();
        }
    }

    static final class SuccessException extends IOException {
        private static final long serialVersionUID = -9218191172606739598L;
    }
}
