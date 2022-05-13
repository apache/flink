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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.collect.utils.AbstractTestCoordinationRequestHandler;
import org.apache.flink.streaming.api.operators.collect.utils.TestCheckpointedCoordinationRequestHandler;
import org.apache.flink.streaming.api.operators.collect.utils.TestJobClient;
import org.apache.flink.streaming.api.operators.collect.utils.TestUncheckpointedCoordinationRequestHandler;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/** Tests for {@link CollectResultIterator}. */
public class CollectResultIteratorTest extends TestLogger {

    private final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;

    private static final OperatorID TEST_OPERATOR_ID = new OperatorID();
    private static final JobID TEST_JOB_ID = new JobID();
    private static final String ACCUMULATOR_NAME = "accumulatorName";

    @Test
    public void testUncheckpointedIterator() throws Exception {
        Random random = new Random();

        // run this random test multiple times
        for (int testCount = 200; testCount > 0; testCount--) {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                expected.add(i);
            }

            CollectResultIterator<Integer> iterator =
                    createIteratorAndJobClient(
                                    new UncheckpointedCollectResultBuffer<>(serializer, true),
                                    new TestUncheckpointedCoordinationRequestHandler<>(
                                            random.nextInt(3),
                                            expected,
                                            serializer,
                                            ACCUMULATOR_NAME))
                            .f0;

            List<Integer> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }

            // this is an at least once iterator, so we expect each value to at least appear
            Set<Integer> actualSet = new HashSet<>(actual);
            for (int expectedValue : expected) {
                Assert.assertTrue(actualSet.contains(expectedValue));
            }

            iterator.close();
        }
    }

    @Test
    public void testCheckpointedIterator() throws Exception {
        // run this random test multiple times
        for (int testCount = 200; testCount > 0; testCount--) {
            List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                expected.add(i);
            }

            CollectResultIterator<Integer> iterator =
                    createIteratorAndJobClient(
                                    new CheckpointedCollectResultBuffer<>(serializer),
                                    new TestCheckpointedCoordinationRequestHandler<>(
                                            expected, serializer, ACCUMULATOR_NAME))
                            .f0;

            List<Integer> actual = new ArrayList<>();
            while (iterator.hasNext()) {
                actual.add(iterator.next());
            }
            Assert.assertEquals(expected.size(), actual.size());

            Collections.sort(expected);
            Collections.sort(actual);
            Assert.assertArrayEquals(
                    expected.toArray(new Integer[0]), actual.toArray(new Integer[0]));

            iterator.close();
        }
    }

    @Test
    public void testEarlyClose() throws Exception {
        List<Integer> expected = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            expected.add(i);
        }

        Tuple2<CollectResultIterator<Integer>, JobClient> tuple2 =
                createIteratorAndJobClient(
                        new CheckpointedCollectResultBuffer<>(serializer),
                        new TestCheckpointedCoordinationRequestHandler<>(
                                expected, serializer, ACCUMULATOR_NAME));
        CollectResultIterator<Integer> iterator = tuple2.f0;
        JobClient jobClient = tuple2.f1;

        for (int i = 0; i < 100; i++) {
            Assert.assertTrue(iterator.hasNext());
            Assert.assertNotNull(iterator.next());
        }
        Assert.assertTrue(iterator.hasNext());
        iterator.close();

        Assert.assertEquals(JobStatus.CANCELED, jobClient.getJobStatus().get());
    }

    private Tuple2<CollectResultIterator<Integer>, JobClient> createIteratorAndJobClient(
            AbstractCollectResultBuffer<Integer> buffer,
            AbstractTestCoordinationRequestHandler<Integer> handler) {
        CollectResultIterator<Integer> iterator =
                new CollectResultIterator<>(
                        buffer,
                        CompletableFuture.completedFuture(TEST_OPERATOR_ID),
                        ACCUMULATOR_NAME,
                        0);

        TestJobClient.JobInfoProvider infoProvider =
                new TestJobClient.JobInfoProvider() {

                    @Override
                    public boolean isJobFinished() {
                        return handler.isClosed();
                    }

                    @Override
                    public Map<String, OptionalFailure<Object>> getAccumulatorResults() {
                        return handler.getAccumulatorResults();
                    }
                };

        TestJobClient jobClient =
                new TestJobClient(TEST_JOB_ID, TEST_OPERATOR_ID, handler, infoProvider);
        iterator.setJobClient(jobClient);

        return Tuple2.of(iterator, jobClient);
    }
}
