/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.testing.CollectingSink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests {@link CollectingSink}. */
class CollectingSinkTest {

    private CollectingSink<Integer> sink;

    @BeforeEach
    public void setup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromData(0, 1, 2).setParallelism(1);

        sink = new CollectingSink<>();
        stream.sinkTo(sink);
        env.execute();
    }

    @Test
    void testGetRemainingOutputGivesBackData() {
        List<Integer> result = sink.getRemainingOutput();
        assertThat(result).containsExactlyInAnyOrderElementsOf(Arrays.asList(0, 1, 2));
        closeAndAssertEmpty();
    }

    @Test
    void testPollGivesBackData() throws TimeoutException {
        for (int i = 0; i < 3; i++) {
            assertThat(sink.poll()).isEqualTo(i);
        }
        closeAndAssertEmpty();
    }

    private void closeAndAssertEmpty() {
        sink.close();
        List<Integer> result = sink.getRemainingOutput();
        assertThat(result).isEmpty();
    }
}
