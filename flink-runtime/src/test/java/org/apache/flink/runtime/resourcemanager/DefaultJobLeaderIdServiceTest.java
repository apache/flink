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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the {@link DefaultJobLeaderIdService}. */
class DefaultJobLeaderIdServiceTest {

    /** Tests adding a job and finding out its leader id. */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    void testAddingJob() throws Exception {
        final JobID jobId = new JobID();
        final String address = "foobar";
        final JobMasterId leaderId = JobMasterId.generate();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);

        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);

        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);

        JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(highAvailabilityServices, scheduledExecutor, timeout);

        jobLeaderIdService.start(jobLeaderIdActions);

        jobLeaderIdService.addJob(jobId);

        CompletableFuture<JobMasterId> leaderIdFuture = jobLeaderIdService.getLeaderId(jobId);

        // notify the leader id service about the new leader
        leaderRetrievalService.notifyListener(address, leaderId.toUUID());

        assertThat(leaderIdFuture).isCompletedWithValue(leaderId);

        assertThat(jobLeaderIdService.containsJob(jobId)).isTrue();
    }

    /** Tests that removing a job completes the job leader id future exceptionally. */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    void testRemovingJob() throws Exception {
        final JobID jobId = new JobID();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);

        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);

        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);

        JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(highAvailabilityServices, scheduledExecutor, timeout);

        jobLeaderIdService.start(jobLeaderIdActions);

        jobLeaderIdService.addJob(jobId);

        CompletableFuture<JobMasterId> leaderIdFuture = jobLeaderIdService.getLeaderId(jobId);

        // remove the job before we could find a leader
        jobLeaderIdService.removeJob(jobId);

        assertThat(jobLeaderIdService.containsJob(jobId)).isFalse();

        assertThatFuture(leaderIdFuture)
                .withFailMessage("The leader id future should be completed exceptionally.")
                .eventuallyFails()
                .withThrowableOfType(ExecutionException.class);
    }

    /**
     * Tests that the initial job registration registers a timeout which will call {@link
     * JobLeaderIdActions#notifyJobTimeout(JobID, UUID)} when executed.
     */
    @Test
    void testInitialJobTimeout() throws Exception {
        final JobID jobId = new JobID();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);

        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);

        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);

        JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(highAvailabilityServices, scheduledExecutor, timeout);

        jobLeaderIdService.start(jobLeaderIdActions);

        jobLeaderIdService.addJob(jobId);

        assertThat(jobLeaderIdService.containsJob(jobId)).isTrue();

        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutor)
                .schedule(runnableArgumentCaptor.capture(), anyLong(), any(TimeUnit.class));

        Runnable timeoutRunnable = runnableArgumentCaptor.getValue();
        timeoutRunnable.run();

        ArgumentCaptor<UUID> timeoutIdArgumentCaptor = ArgumentCaptor.forClass(UUID.class);

        verify(jobLeaderIdActions, times(1))
                .notifyJobTimeout(eq(jobId), timeoutIdArgumentCaptor.capture());

        assertThat(jobLeaderIdService.isValidTimeout(jobId, timeoutIdArgumentCaptor.getValue()))
                .isTrue();
    }

    /**
     * Tests that a timeout get cancelled once a job leader has been found. Furthermore, it tests
     * that a new timeout is registered after the jobmanager has lost leadership.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    void jobTimeoutAfterLostLeadership() throws Exception {
        final JobID jobId = new JobID();
        final String address = "foobar";
        final JobMasterId leaderId = JobMasterId.generate();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);

        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);

        ScheduledFuture<?> timeout1 = mock(ScheduledFuture.class);
        ScheduledFuture<?> timeout2 = mock(ScheduledFuture.class);
        final Queue<ScheduledFuture<?>> timeoutQueue =
                new ArrayDeque<>(Arrays.asList(timeout1, timeout2));
        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);

        final AtomicReference<Runnable> lastRunnable = new AtomicReference<>();
        doAnswer(
                        invocation -> {
                            lastRunnable.set((Runnable) invocation.getArguments()[0]);
                            return timeoutQueue.poll();
                        })
                .when(scheduledExecutor)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);

        final AtomicReference<UUID> lastTimeoutId = new AtomicReference<>();

        doAnswer(
                        invocation -> {
                            lastTimeoutId.set((UUID) invocation.getArguments()[1]);
                            return null;
                        })
                .when(jobLeaderIdActions)
                .notifyJobTimeout(eq(jobId), any(UUID.class));

        JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(highAvailabilityServices, scheduledExecutor, timeout);

        jobLeaderIdService.start(jobLeaderIdActions);

        jobLeaderIdService.addJob(jobId);

        CompletableFuture<JobMasterId> leaderIdFuture = jobLeaderIdService.getLeaderId(jobId);

        // notify the leader id service about the new leader
        leaderRetrievalService.notifyListener(address, leaderId.toUUID());

        assertThat(leaderIdFuture).isCompletedWithValue(leaderId);

        assertThat(jobLeaderIdService.containsJob(jobId)).isTrue();

        // check that the first timeout got cancelled
        verify(timeout1, times(1)).cancel(anyBoolean());

        verify(scheduledExecutor, times(1))
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // initial timeout runnable which should no longer have an effect
        Runnable runnable = lastRunnable.get();

        assertThat(runnable).isNotNull();

        runnable.run();

        verify(jobLeaderIdActions, times(1)).notifyJobTimeout(eq(jobId), any(UUID.class));

        // the timeout should no longer be valid
        assertThat(jobLeaderIdService.isValidTimeout(jobId, lastTimeoutId.get())).isFalse();

        // lose leadership
        leaderRetrievalService.notifyListener("", null);

        verify(scheduledExecutor, times(2))
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // the second runnable should be the new timeout
        runnable = lastRunnable.get();

        assertThat(runnable).isNotNull();

        runnable.run();

        verify(jobLeaderIdActions, times(2)).notifyJobTimeout(eq(jobId), any(UUID.class));

        // the new timeout should be valid
        assertThat(jobLeaderIdService.isValidTimeout(jobId, lastTimeoutId.get())).isTrue();
    }

    /**
     * Tests that the leaderId future is only completed once the service is notified about an actual
     * leader being elected. Specifically, it tests that the future is not completed if the
     * leadership was revoked without a new leader having been elected.
     */
    @Test
    @Timeout(value = 10000, unit = TimeUnit.MILLISECONDS)
    void testLeaderFutureWaitsForValidLeader() throws Exception {
        final JobID jobId = new JobID();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);

        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);

        JobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(
                        highAvailabilityServices,
                        new ManuallyTriggeredScheduledExecutor(),
                        Time.milliseconds(5000L));

        jobLeaderIdService.start(new NoOpJobLeaderIdActions());

        jobLeaderIdService.addJob(jobId);

        // elect some leader
        leaderRetrievalService.notifyListener("foo", UUID.randomUUID());

        // notify about leadership loss
        leaderRetrievalService.notifyListener(null, null);

        final CompletableFuture<JobMasterId> leaderIdFuture = jobLeaderIdService.getLeaderId(jobId);
        // there is currently no leader, so this should not be completed
        assertThat(leaderIdFuture).isNotDone();

        // elect a new leader
        final UUID newLeaderId = UUID.randomUUID();
        leaderRetrievalService.notifyListener("foo", newLeaderId);
        assertThatFuture(leaderIdFuture)
                .eventuallySucceeds()
                .isEqualTo(JobMasterId.fromUuidOrNull(newLeaderId));
    }

    /** Tests that whether the service has been started. */
    @Test
    void testIsStarted() throws Exception {
        final JobID jobId = new JobID();
        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        SettableLeaderRetrievalService leaderRetrievalService =
                new SettableLeaderRetrievalService(null, null);
        highAvailabilityServices.setJobMasterLeaderRetriever(jobId, leaderRetrievalService);
        ScheduledExecutor scheduledExecutor = mock(ScheduledExecutor.class);
        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);
        DefaultJobLeaderIdService jobLeaderIdService =
                new DefaultJobLeaderIdService(highAvailabilityServices, scheduledExecutor, timeout);

        assertThat(jobLeaderIdService.isStarted()).isFalse();

        jobLeaderIdService.start(jobLeaderIdActions);

        assertThat(jobLeaderIdService.isStarted()).isTrue();

        jobLeaderIdService.stop();

        assertThat(jobLeaderIdService.isStarted()).isFalse();
    }

    private static class NoOpJobLeaderIdActions implements JobLeaderIdActions {

        @Override
        public void jobLeaderLostLeadership(JobID jobId, JobMasterId oldJobMasterId) {}

        @Override
        public void notifyJobTimeout(JobID jobId, UUID timeoutId) {}

        @Override
        public void handleError(Throwable error) {}
    }
}
