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

package org.apache.flink.test.streaming.api.environment;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LocalStreamEnvironment}. */
@ExtendWith(TestLoggerExtension.class)
class LocalStreamEnvironmentITCase {

    /**
     * Test test verifies that the execution environment can be used to execute a single job with
     * multiple slots.
     */
    @Test
    void testRunIsolatedJob() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();
        assertThat(env.getParallelism()).isOne();

        addSmallBoundedJob(env, 3);
        env.execute();
    }

    /**
     * Test test verifies that the execution environment can be used to execute multiple bounded
     * streaming jobs after one another.
     */
    @Test
    void testMultipleJobsAfterAnother() throws Exception {
        LocalStreamEnvironment env = new LocalStreamEnvironment();

        addSmallBoundedJob(env, 3);
        env.execute();

        addSmallBoundedJob(env, 5);
        env.execute();
    }

    // ------------------------------------------------------------------------

    private static void addSmallBoundedJob(StreamExecutionEnvironment env, int parallelism) {
        DataStream<Long> stream = env.fromSequence(1, 100).setParallelism(parallelism);

        stream.filter(ignored -> false)
                .setParallelism(parallelism)
                .startNewChain()
                .print()
                .setParallelism(parallelism);
    }
}
