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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.SerializedShuffleDescriptorAndIndicesID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultShuffleDescriptorsCache}. */
class DefaultShuffleDescriptorsCacheTest {
    private final Duration idleTimeout = Duration.ofMillis(100);

    @Test
    void testGetEntry() {
        DefaultShuffleDescriptorsCache cache =
                new DefaultShuffleDescriptorsCache(idleTimeout, Integer.MAX_VALUE);

        JobID jobId = new JobID();
        SerializedShuffleDescriptorAndIndicesID shuffleDescriptorsID =
                new SerializedShuffleDescriptorAndIndicesID();

        assertThat(cache.get(shuffleDescriptorsID)).isNull();

        ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(new ResultPartitionID());
        TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices =
                new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] {
                    new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex(
                            shuffleDescriptor, 0)
                };
        cache.put(jobId, shuffleDescriptorsID, shuffleDescriptorAndIndices);
        assertThat(cache.get(shuffleDescriptorsID).getShuffleDescriptorAndIndices())
                .isEqualTo(shuffleDescriptorAndIndices);
    }

    @Test
    void testEntryWillExpiredAfterIdleTimeout() {
        ManualClock clock = new ManualClock();
        ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
                new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());
        DefaultShuffleDescriptorsCache cache =
                new DefaultShuffleDescriptorsCache(idleTimeout, Integer.MAX_VALUE, clock);
        cache.start(mainThreadExecutor);

        JobID jobId = new JobID();
        SerializedShuffleDescriptorAndIndicesID shuffleDescriptorsID =
                new SerializedShuffleDescriptorAndIndicesID();

        ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(new ResultPartitionID());
        TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices =
                new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] {
                    new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex(
                            shuffleDescriptor, 0)
                };
        cache.put(jobId, shuffleDescriptorsID, shuffleDescriptorAndIndices);
        clock.advanceTime(idleTimeout.toMillis() - 1, TimeUnit.MILLISECONDS);
        assertThat(cache.get(shuffleDescriptorsID).getShuffleDescriptorAndIndices())
                .isEqualTo(shuffleDescriptorAndIndices);

        // get will update the idle since, so cache will not expire after (idle timeout - 1 + 2)
        clock.advanceTime(2, TimeUnit.MILLISECONDS);
        mainThreadExecutor.triggerScheduledTasks();
        assertThat(cache.get(shuffleDescriptorsID).getShuffleDescriptorAndIndices())
                .isEqualTo(shuffleDescriptorAndIndices);

        // cache expired after (idle timeout + 1)
        clock.advanceTime(idleTimeout.toMillis() + 1, TimeUnit.MILLISECONDS);
        mainThreadExecutor.triggerScheduledTasks();
        assertThat(cache.get(shuffleDescriptorsID)).isNull();
    }

    @Test
    void testClearCacheOfJob() {
        DefaultShuffleDescriptorsCache cache =
                new DefaultShuffleDescriptorsCache(idleTimeout, Integer.MAX_VALUE);

        JobID jobId = new JobID();
        SerializedShuffleDescriptorAndIndicesID shuffleDescriptorsID =
                new SerializedShuffleDescriptorAndIndicesID();

        assertThat(cache.get(shuffleDescriptorsID)).isNull();

        ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(new ResultPartitionID());
        TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices =
                new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] {
                    new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex(
                            shuffleDescriptor, 0)
                };
        cache.put(jobId, shuffleDescriptorsID, shuffleDescriptorAndIndices);
        assertThat(cache.get(shuffleDescriptorsID).getShuffleDescriptorAndIndices())
                .isEqualTo(shuffleDescriptorAndIndices);

        cache.clearCacheOfJob(jobId);
        assertThat(cache.get(shuffleDescriptorsID)).isNull();
    }

    @Test
    void testIdleCheckNotTriggeredAfterClose() {
        ManualClock clock = new ManualClock();
        ManuallyTriggeredComponentMainThreadExecutor mainThreadExecutor =
                new ManuallyTriggeredComponentMainThreadExecutor(Thread.currentThread());
        DefaultShuffleDescriptorsCache cache =
                new DefaultShuffleDescriptorsCache(idleTimeout, Integer.MAX_VALUE, clock);
        cache.start(mainThreadExecutor);
        assertThat(mainThreadExecutor.getAllScheduledTasks()).hasSize(1);

        // idle check will be rescheduled after trigger
        mainThreadExecutor.triggerScheduledTasks();
        assertThat(mainThreadExecutor.getAllScheduledTasks()).hasSize(1);

        // idle check will not be scheduled when cache manager stop
        cache.stop();
        mainThreadExecutor.triggerScheduledTasks();
        assertThat(mainThreadExecutor.getAllScheduledTasks()).isEmpty();
    }

    @Test
    void testPutWhenOverLimit() {
        DefaultShuffleDescriptorsCache cache = new DefaultShuffleDescriptorsCache(idleTimeout, 1);

        JobID jobId = new JobID();
        SerializedShuffleDescriptorAndIndicesID shuffleDescriptorsID =
                new SerializedShuffleDescriptorAndIndicesID();

        ShuffleDescriptor shuffleDescriptor = new UnknownShuffleDescriptor(new ResultPartitionID());
        TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] shuffleDescriptorAndIndices =
                new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] {
                    new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex(
                            shuffleDescriptor, 0)
                };
        cache.put(jobId, shuffleDescriptorsID, shuffleDescriptorAndIndices);
        assertThat(cache.get(shuffleDescriptorsID).getShuffleDescriptorAndIndices())
                .isEqualTo(shuffleDescriptorAndIndices);

        SerializedShuffleDescriptorAndIndicesID otherId =
                new SerializedShuffleDescriptorAndIndicesID();
        TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[]
                otherShuffleDescriptorAndIndices =
                        new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex[] {
                            new TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex(
                                    new UnknownShuffleDescriptor(new ResultPartitionID()), 0)
                        };
        cache.put(jobId, otherId, otherShuffleDescriptorAndIndices);
        assertThat(cache.get(shuffleDescriptorsID)).isNull();
        assertThat(cache.get(otherId).getShuffleDescriptorAndIndices())
                .isEqualTo(otherShuffleDescriptorAndIndices);
    }
}
