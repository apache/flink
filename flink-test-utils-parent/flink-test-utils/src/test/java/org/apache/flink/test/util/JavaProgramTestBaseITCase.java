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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.util.TestStreamEnvironment;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests that {@link JavaProgramTestBase} runs the test program with and without object reuse. */
class JavaProgramTestBaseITCase extends JavaProgramTestBase {

    private static final List<Boolean> OBJECT_REUSE_IN_TASKS = new CopyOnWriteArrayList<>();

    @BeforeEach
    void clearObservedObjectReuse() {
        OBJECT_REUSE_IN_TASKS.clear();
    }

    @Override
    @Test
    public void testJobWithObjectReuse() throws Exception {
        super.testJobWithObjectReuse();
        assertThat(OBJECT_REUSE_IN_TASKS).isNotEmpty().containsOnly(true);
    }

    @Override
    @Test
    public void testJobWithoutObjectReuse() throws Exception {
        super.testJobWithoutObjectReuse();
        assertThat(OBJECT_REUSE_IN_TASKS).isNotEmpty().containsOnly(false);
    }

    @Test
    void testExplicitObjectReuseConfigTakesPrecedence() {
        final TestStreamEnvironment testEnv = MINI_CLUSTER_EXTENSION.getTestStreamEnvironment();
        testEnv.getConfig().enableObjectReuse();
        testEnv.setAsContext();

        final Configuration conf = new Configuration();
        conf.set(PipelineOptions.OBJECT_REUSE, false);

        assertThat(
                        StreamExecutionEnvironment.getExecutionEnvironment(conf)
                                .getConfig()
                                .isObjectReuseEnabled())
                .isFalse();
    }

    @Override
    protected JobExecutionResult testProgram() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromData(1, 2, 3).map(new ObjectReuseRecorder()).sinkTo(new DiscardingSink<>());
        return env.execute();
    }

    private static class ObjectReuseRecorder extends RichMapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) {
            OBJECT_REUSE_IN_TASKS.add(getRuntimeContext().isObjectReuseEnabled());
            return value;
        }
    }
}
