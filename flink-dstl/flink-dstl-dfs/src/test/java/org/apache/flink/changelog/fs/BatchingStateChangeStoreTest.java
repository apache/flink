/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.changelog.fs;

import org.apache.flink.changelog.fs.StateChangeStore.StoreTask;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;

/** {@link BatchingStateChangeStore} test. */
public class BatchingStateChangeStoreTest {

    private final Random random = new Random();

    @Test
    public void testNoDelayAndThreshold() throws Exception {
        withStore(
                0,
                0,
                (store, probe) -> {
                    List<StateChangeSet> changes1 = getChanges(4);
                    store.save(new StoreTask(changes1));
                    assertSaved(probe, changes1);
                    List<StateChangeSet> changes2 = getChanges(4);
                    store.save(new StoreTask(changes2));
                    assertSaved(probe, changes1, changes2);
                });
    }

    @Test
    public void testSizeThreshold() throws Exception {
        int numChanges = 7;
        int changeSize = 11;
        int threshold = changeSize * numChanges;
        withStore(
                Integer.MAX_VALUE,
                threshold,
                (store, probe) -> {
                    List<StateChangeSet> expected = new ArrayList<>();
                    int runningSize = 0;
                    for (int i = 0; i < numChanges; i++) {
                        List<StateChangeSet> changes = getChanges(changeSize);
                        runningSize += changes.stream().mapToLong(StateChangeSet::getSize).sum();
                        store.save(new StoreTask(changes));
                        expected.addAll(changes);
                        if (runningSize >= threshold) {
                            assertSaved(probe, expected);
                        } else {
                            assertTrue(probe.getSaved().isEmpty());
                        }
                    }
                });
    }

    @Test
    public void testDelay() throws Exception {
        int delayMs = 50;
        withStore(
                delayMs,
                Integer.MAX_VALUE,
                (store, probe) -> {
                    List<StateChangeSet> changeSets = getChanges(4);
                    store.save(new StoreTask(changeSets));
                    assertTrue(probe.getSaved().isEmpty());
                    Thread.sleep(delayMs * 2);
                    Assert.assertEquals(changeSets, probe.getSaved());
                });
    }

    @Test(expected = RejectedExecutionException.class)
    public void testErrorHandling() throws Exception {
        TestingStateChangeStore probe = new TestingStateChangeStore();
        DirectScheduledExecutorService scheduler = new DirectScheduledExecutorService();
        try (BatchingStateChangeStore store =
                new BatchingStateChangeStore(
                        Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor())) {
            scheduler.shutdown();
            List<StateChangeSet> changes = getChanges(4);
            store.save(new StoreTask(changes));
        }
    }

    @Test
    public void testClose() throws Exception {
        TestingStateChangeStore probe = new TestingStateChangeStore();
        DirectScheduledExecutorService scheduler = new DirectScheduledExecutorService();
        DirectScheduledExecutorService retryScheduler = new DirectScheduledExecutorService();
        new BatchingStateChangeStore(
                        0,
                        0,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(retryScheduler))
                .close();
        assertTrue(probe.isClosed());
        assertTrue(scheduler.isShutdown());
        assertTrue(retryScheduler.isShutdown());
    }

    private List<StateChangeSet> getChanges(int size) {
        byte[] change = new byte[size];
        random.nextBytes(change);
        return singletonList(
                new StateChangeSet(
                        UUID.randomUUID(),
                        SequenceNumber.of(0),
                        singletonList(new StateChange(0, change))));
    }

    private static void withStore(
            int delayMs,
            int sizeThreshold,
            BiConsumerWithException<BatchingStateChangeStore, TestingStateChangeStore, Exception>
                    test)
            throws Exception {
        TestingStateChangeStore probe = new TestingStateChangeStore();

        try (BatchingStateChangeStore store =
                new BatchingStateChangeStore(
                        delayMs,
                        sizeThreshold,
                        RetryPolicy.NONE,
                        probe,
                        new DirectScheduledExecutorService(),
                        new RetryingExecutor(new DirectScheduledExecutorService()))) {
            test.accept(store, probe);
        }
    }

    @SafeVarargs
    private final void assertSaved(
            TestingStateChangeStore probe, List<StateChangeSet>... expected) {
        Assert.assertEquals(
                Arrays.stream(expected).flatMap(Collection::stream).collect(toList()),
                new ArrayList<>(probe.getSaved()));
    }
}
