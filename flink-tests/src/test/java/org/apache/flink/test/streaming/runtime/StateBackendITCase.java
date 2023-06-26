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
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBaseJUnit4;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Integration tests for {@link OperatorStateBackend}. */
public class StateBackendITCase extends AbstractTestBaseJUnit4 {

    /** Verify that the user-specified state backend is used even if checkpointing is disabled. */
    @Test
    public void testStateBackendWithoutCheckpointing() throws Exception {

        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        see.setParallelism(1);

        see.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        see.setStateBackend(new FailingStateBackend());

        see.fromData(new Tuple2<>("Hello", 1))
                .keyBy(0)
                .map(
                        new RichMapFunction<Tuple2<String, Integer>, String>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void open(OpenContext openContext) throws Exception {
                                super.open(openContext);
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

    private static class FailingStateBackend implements StateBackend, CheckpointStorage {
        private static final long serialVersionUID = 1L;

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String pointer)
                throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return new MemoryBackendCheckpointStorageAccess(jobId, null, null, true, 1_000_000);
        }

        @Override
        public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws IOException {
            throw new SuccessException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            throw new SuccessException();
        }
    }

    static final class SuccessException extends IOException {
        private static final long serialVersionUID = -9218191172606739598L;
    }
}
