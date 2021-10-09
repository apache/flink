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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.committableRecord;
import static org.apache.flink.streaming.runtime.operators.sink.SinkTestUtil.committableRecords;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

/** Tests for {@link GlobalBatchCommitterHandler}. */
public class GlobalBatchCommitterHandlerTest extends TestLogger {

    @Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutCommitter() throws Exception {
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(null);

        testHarness.initializeEmptyState();
    }

    @Test
    public void supportRetry() throws Exception {
        final TestSink.RetryOnceGlobalCommitter globalCommitter =
                new TestSink.RetryOnceGlobalCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(globalCommitter);

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElement(committableRecord("hotel"));
        testHarness.processElement(committableRecord("motel"));
        testHarness.endInput();
        testHarness.close();
        assertThat(globalCommitter.getCommittedData(), Matchers.contains("hotel|motel"));
    }

    @Test
    public void endOfInput() throws Exception {
        final TestSink.DefaultGlobalCommitter globalCommitter =
                new TestSink.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(globalCommitter);
        final List<String> inputs = Arrays.asList("compete", "swear", "shallow");

        testHarness.initializeEmptyState();
        testHarness.open();

        testHarness.processElements(committableRecords(inputs));

        testHarness.endInput();

        final List<String> expectedCommittedData =
                Arrays.asList(globalCommitter.combine(inputs), "end of input");

        assertThat(
                globalCommitter.getCommittedData(),
                containsInAnyOrder(expectedCommittedData.toArray()));

        testHarness.close();
    }

    @Test
    public void close() throws Exception {
        final TestSink.DefaultGlobalCommitter globalCommitter =
                new TestSink.DefaultGlobalCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(globalCommitter);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();

        assertThat(globalCommitter.isClosed(), is(true));
    }

    private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
            GlobalCommitter<String, String> globalCommitter) throws Exception {

        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(
                        TestSink.newBuilder()
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .setGlobalCommitter(globalCommitter)
                                .setGlobalCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build(),
                        true));
    }
}
