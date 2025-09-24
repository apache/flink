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

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link AbstractApplication}. */
public class AbstractApplicationTest {

    @Test
    void testInitialization() {
        ApplicationID applicationID = new ApplicationID();
        long start = System.currentTimeMillis();
        AbstractApplication application = new MockApplication(applicationID);
        long end = System.currentTimeMillis();

        assertEquals(applicationID, application.getApplicationId());

        assertEquals(ApplicationState.CREATED, application.getApplicationStatus());

        long ts = application.getStatusTimestamp(ApplicationState.CREATED);
        assertTrue(start <= ts && ts <= end);
    }

    @Test
    void testAddJob() {
        AbstractApplication application = new MockApplication(new ApplicationID());
        JobID jobId = JobID.generate();

        boolean added = application.addJob(jobId);
        assertTrue(added);
        assertEquals(1, application.getJobs().size());
        assertTrue(application.getJobs().contains(jobId));

        added = application.addJob(jobId);
        assertFalse(added);
        assertEquals(1, application.getJobs().size());
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"RUNNING", "CANCELING"})
    void testTransitionFromCreated(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());

        long start = System.currentTimeMillis();
        application.transitionState(targetState);
        long end = System.currentTimeMillis();

        assertEquals(targetState, application.getApplicationStatus());

        long ts = application.getStatusTimestamp(targetState);
        assertTrue(start <= ts && ts <= end);
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"CREATED", "FINISHED", "FAILING", "CANCELED", "FAILED"})
    void testTransitionFromCreatedToUnsupportedStates(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"FINISHED", "FAILING", "CANCELING"})
    void testTransitionFromRunning(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();

        long start = System.currentTimeMillis();
        application.transitionState(targetState);
        long end = System.currentTimeMillis();

        assertEquals(targetState, application.getApplicationStatus());

        long ts = application.getStatusTimestamp(targetState);
        assertTrue(start <= ts && ts <= end);
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"CREATED", "RUNNING", "FAILED", "CANCELED"})
    void testTransitionFromRunningToUnsupportedStates(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @Test
    void testTransitionFromCanceling() {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToCanceling();

        long start = System.currentTimeMillis();
        application.transitionToCanceled();
        long end = System.currentTimeMillis();

        assertEquals(ApplicationState.CANCELED, application.getApplicationStatus());

        long ts = application.getStatusTimestamp(ApplicationState.CANCELED);
        assertTrue(start <= ts && ts <= end);
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"CREATED", "RUNNING", "CANCELING", "FAILING", "FINISHED", "FAILED"})
    void testTransitionFromCancelingToUnsupportedStates(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToCanceling();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @Test
    void testTransitionFromFailing() {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();
        application.transitionToFailing();

        long start = System.currentTimeMillis();
        application.transitionToFailed();
        long end = System.currentTimeMillis();

        assertEquals(ApplicationState.FAILED, application.getApplicationStatus());

        long ts = application.getStatusTimestamp(ApplicationState.FAILED);
        assertTrue(start <= ts && ts <= end);
    }

    @ParameterizedTest
    @EnumSource(
            value = ApplicationState.class,
            names = {"CREATED", "RUNNING", "CANCELING", "FAILING", "FINISHED", "CANCELED"})
    void testTransitionFromFailingToUnsupportedStates(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();
        application.transitionToFailing();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @ParameterizedTest
    @EnumSource(value = ApplicationState.class)
    void testTransitionFromFinished(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();
        application.transitionToFinished();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @ParameterizedTest
    @EnumSource(value = ApplicationState.class)
    void testTransitionFromCanceled(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToCanceling();
        application.transitionToCanceled();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    @ParameterizedTest
    @EnumSource(value = ApplicationState.class)
    void testTransitionFromFailed(ApplicationState targetState) {
        AbstractApplication application = new MockApplication(new ApplicationID());
        application.transitionToRunning();
        application.transitionToFailing();
        application.transitionToFailed();

        assertThrows(IllegalStateException.class, () -> application.transitionState(targetState));
    }

    private static class MockApplication extends AbstractApplication {
        public MockApplication(ApplicationID applicationId) {
            super(applicationId);
        }

        @Override
        public CompletableFuture<Acknowledge> execute(
                DispatcherGateway dispatcherGateway,
                ScheduledExecutor scheduledExecutor,
                Executor mainThreadExecutor,
                FatalErrorHandler errorHandler) {
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        @Override
        public void cancel() {}

        @Override
        public void dispose() {}

        @Override
        public String getName() {
            return "Mock Application";
        }
    }
}
