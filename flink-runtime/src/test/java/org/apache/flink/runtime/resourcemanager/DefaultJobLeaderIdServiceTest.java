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
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Tests for the {@link DefaultJobLeaderIdService}. */
public class DefaultJobLeaderIdServiceTest extends TestLogger {

    /** Tests adding a job and finding out its leader id. */
    @Test(timeout = 10000)
    public void testAddingJob() throws Exception {
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

        assertEquals(leaderId, leaderIdFuture.get());

        assertTrue(jobLeaderIdService.containsJob(jobId));
    }

    /** Tests that removing a job completes the job leader id future exceptionally. */
    @Test(timeout = 10000)
    public void testRemovingJob() throws Exception {
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

        assertFalse(jobLeaderIdService.containsJob(jobId));

        try {
            leaderIdFuture.get();

            fail("The leader id future should be completed exceptionally.");
        } catch (ExecutionException ignored) {
            // expected exception
        }
    }

    /**
     * Tests that the initial job registration registers a timeout which will call {@link
     * JobLeaderIdActions#notifyJobTimeout(JobID, UUID)} when executed.
     */
    @Test
    public void testInitialJobTimeout() throws Exception {
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

        assertTrue(jobLeaderIdService.containsJob(jobId));

        ArgumentCaptor<Runnable> runnableArgumentCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(scheduledExecutor)
                .schedule(runnableArgumentCaptor.capture(), anyLong(), any(TimeUnit.class));

        Runnable timeoutRunnable = runnableArgumentCaptor.getValue();
        timeoutRunnable.run();

        ArgumentCaptor<UUID> timeoutIdArgumentCaptor = ArgumentCaptor.forClass(UUID.class);

        verify(jobLeaderIdActions, times(1))
                .notifyJobTimeout(eq(jobId), timeoutIdArgumentCaptor.capture());

        assertTrue(jobLeaderIdService.isValidTimeout(jobId, timeoutIdArgumentCaptor.getValue()));
    }

    /**
     * Tests that a timeout get cancelled once a job leader has been found. Furthermore, it tests
     * that a new timeout is registered after the jobmanager has lost leadership.
     */
    @Test(timeout = 10000)
    public void jobTimeoutAfterLostLeadership() throws Exception {
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
                        new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Throwable {
                                lastRunnable.set((Runnable) invocation.getArguments()[0]);

                                return timeoutQueue.poll();
                            }
                        })
                .when(scheduledExecutor)
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        Time timeout = Time.milliseconds(5000L);
        JobLeaderIdActions jobLeaderIdActions = mock(JobLeaderIdActions.class);

        final AtomicReference<UUID> lastTimeoutId = new AtomicReference<>();

        doAnswer(
                        new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Throwable {
                                lastTimeoutId.set((UUID) invocation.getArguments()[1]);
                                return null;
                            }
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

        assertEquals(leaderId, leaderIdFuture.get());

        assertTrue(jobLeaderIdService.containsJob(jobId));

        // check that the first timeout got cancelled
        verify(timeout1, times(1)).cancel(anyBoolean());

        verify(scheduledExecutor, times(1))
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // initial timeout runnable which should no longer have an effect
        Runnable runnable = lastRunnable.get();

        assertNotNull(runnable);

        runnable.run();

        verify(jobLeaderIdActions, times(1)).notifyJobTimeout(eq(jobId), any(UUID.class));

        // the timeout should no longer be valid
        assertFalse(jobLeaderIdService.isValidTimeout(jobId, lastTimeoutId.get()));

        // lose leadership
        leaderRetrievalService.notifyListener("", null);

        verify(scheduledExecutor, times(2))
                .schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

        // the second runnable should be the new timeout
        runnable = lastRunnable.get();

        assertNotNull(runnable);

        runnable.run();

        verify(jobLeaderIdActions, times(2)).notifyJobTimeout(eq(jobId), any(UUID.class));

        // the new timeout should be valid
        assertTrue(jobLeaderIdService.isValidTimeout(jobId, lastTimeoutId.get()));
    }

    /**
     * Tests that the leaderId future is only completed once the service is notified about an actual
     * leader being elected. Specifically, it tests that the future is not completed if the
     * leadership was revoked without a new leader having been elected.
     */
    @Test(timeout = 10000)
    public void testLeaderFutureWaitsForValidLeader() throws Exception {
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
        assertThat(leaderIdFuture.isDone(), is(false));

        // elect a new leader
        final UUID newLeaderId = UUID.randomUUID();
        leaderRetrievalService.notifyListener("foo", newLeaderId);
        assertThat(leaderIdFuture.get(), is(JobMasterId.fromUuidOrNull(newLeaderId)));
    }

    /** Tests that whether the service has been started. */
    @Test
    public void testIsStarted() throws Exception {
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

        assertFalse(jobLeaderIdService.isStarted());

        jobLeaderIdService.start(jobLeaderIdActions);

        assertTrue(jobLeaderIdService.isStarted());

        jobLeaderIdService.stop();

        assertFalse(jobLeaderIdService.isStarted());
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
