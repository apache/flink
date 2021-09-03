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

import org.apache.flink.api.connector.sink.Committer;
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

/** Tests for {@link BatchCommitterHandler}. */
public class BatchCommitterHandlerTest extends TestLogger {

    @Test(expected = IllegalStateException.class)
    public void throwExceptionWithoutCommitter() throws Exception {
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(null);
        testHarness.initializeEmptyState();
    }

    @Test
    public void supportRetry() throws Exception {
        final TestSink.RetryOnceCommitter committer = new TestSink.RetryOnceCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(committer);

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElement(committableRecord("those"));
        testHarness.processElement(committableRecord("these"));
        testHarness.endInput();
        testHarness.close();
        assertThat(committer.getCommittedData(), Matchers.contains("those", "these"));
    }

    @Test
    public void commit() throws Exception {

        final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(committer);

        final List<String> expectedCommittedData = Arrays.asList("youth", "laugh", "nothing");

        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.processElements(committableRecords(expectedCommittedData));
        testHarness.endInput();
        testHarness.close();

        assertThat(
                committer.getCommittedData(), containsInAnyOrder(expectedCommittedData.toArray()));
    }

    @Test
    public void close() throws Exception {
        final TestSink.DefaultCommitter committer = new TestSink.DefaultCommitter();
        final OneInputStreamOperatorTestHarness<byte[], byte[]> testHarness =
                createTestHarness(committer);
        testHarness.initializeEmptyState();
        testHarness.open();
        testHarness.close();

        assertThat(committer.isClosed(), is(true));
    }

    private OneInputStreamOperatorTestHarness<byte[], byte[]> createTestHarness(
            Committer<String> committer) throws Exception {
        return new OneInputStreamOperatorTestHarness<>(
                new CommitterOperatorFactory<>(
                        TestSink.newBuilder()
                                .setCommitter(committer)
                                .setCommittableSerializer(
                                        TestSink.StringCommittableSerializer.INSTANCE)
                                .build(),
                        true));
    }
}
