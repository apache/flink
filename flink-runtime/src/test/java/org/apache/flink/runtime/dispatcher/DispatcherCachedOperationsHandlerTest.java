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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.CheckpointType;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.async.CompletedOperationCache;
import org.apache.flink.runtime.rest.handler.async.OperationResult;
import org.apache.flink.runtime.rest.handler.job.AsynchronousJobOperationKey;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.core.testutils.FlinkMatchers.futureFailedWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the {@link DispatcherCachedOperationsHandler} component. */
public class DispatcherCachedOperationsHandlerTest extends TestLogger {

    private static final Time TIMEOUT = Time.minutes(10);

    private CompletedOperationCache<AsynchronousJobOperationKey, Long> checkpointTriggerCache;
    private CompletedOperationCache<AsynchronousJobOperationKey, String> savepointTriggerCache;
    private DispatcherCachedOperationsHandler handler;

    private TriggerCheckpointSpyFunction triggerCheckpointFunction;
    private TriggerSavepointSpyFunction triggerSavepointFunction;
    private TriggerSavepointSpyFunction stopWithSavepointFunction;

    private CompletableFuture<Long> checkpointIdFuture = new CompletableFuture<>();
    private CompletableFuture<String> savepointLocationFuture = new CompletableFuture<>();
    private final JobID jobID = new JobID();
    private final String targetDirectory = "dummyDirectory";
    private AsynchronousJobOperationKey operationKey;

    @BeforeEach
    public void setup() {

        checkpointIdFuture = new CompletableFuture<>();
        triggerCheckpointFunction =
                TriggerCheckpointSpyFunction.wrap(
                        new TriggerCheckpointSpyFunction() {
                            @Override
                            CompletableFuture<Long> applyWrappedFunction(
                                    JobID jobID, CheckpointType checkpointType, Time timeout) {
                                return checkpointIdFuture;
                            }
                        });

        savepointLocationFuture = new CompletableFuture<>();
        triggerSavepointFunction =
                TriggerSavepointSpyFunction.wrap(
                        (jobID, targetDirectory, formatType, savepointMode, timeout) ->
                                savepointLocationFuture);
        stopWithSavepointFunction =
                TriggerSavepointSpyFunction.wrap(
                        (jobID, targetDirectory, formatType, savepointMode, timeout) ->
                                savepointLocationFuture);

        checkpointTriggerCache =
                new CompletedOperationCache<>(
                        RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue());

        savepointTriggerCache =
                new CompletedOperationCache<>(
                        RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue());
        handler =
                new DispatcherCachedOperationsHandler(
                        triggerCheckpointFunction,
                        checkpointTriggerCache,
                        triggerSavepointFunction,
                        stopWithSavepointFunction,
                        savepointTriggerCache);
        operationKey = AsynchronousJobOperationKey.of(new TriggerId(), jobID);
    }

    @Test
    public void triggerSavepointRepeatedly() throws ExecutionException, InterruptedException {
        CompletableFuture<Acknowledge> firstAcknowledge =
                handler.triggerSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.SAVEPOINT,
                        TIMEOUT);
        CompletableFuture<Acknowledge> secondAcknowledge =
                handler.triggerSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.SAVEPOINT,
                        TIMEOUT);

        assertThat(triggerSavepointFunction.getNumberOfInvocations(), is(1));
        assertThat(
                triggerSavepointFunction.getInvocationParameters().get(0),
                is(
                        new Tuple4<>(
                                jobID,
                                targetDirectory,
                                SavepointFormatType.CANONICAL,
                                TriggerSavepointMode.SAVEPOINT)));

        assertThat(firstAcknowledge.get(), is(Acknowledge.get()));
        assertThat(secondAcknowledge.get(), is(Acknowledge.get()));
    }

    @Test
    public void stopWithSavepointRepeatedly() throws ExecutionException, InterruptedException {
        CompletableFuture<Acknowledge> firstAcknowledge =
                handler.stopWithSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.TERMINATE_WITH_SAVEPOINT,
                        TIMEOUT);
        CompletableFuture<Acknowledge> secondAcknowledge =
                handler.stopWithSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.TERMINATE_WITH_SAVEPOINT,
                        TIMEOUT);

        assertThat(stopWithSavepointFunction.getNumberOfInvocations(), is(1));
        assertThat(
                stopWithSavepointFunction.getInvocationParameters().get(0),
                is(
                        new Tuple4<>(
                                jobID,
                                targetDirectory,
                                SavepointFormatType.CANONICAL,
                                TriggerSavepointMode.TERMINATE_WITH_SAVEPOINT)));

        assertThat(firstAcknowledge.get(), is(Acknowledge.get()));
        assertThat(secondAcknowledge.get(), is(Acknowledge.get()));
    }

    @Test
    public void retryingCompletedOperationDoesNotMarkCacheEntryAsAccessed()
            throws ExecutionException, InterruptedException {
        handler.triggerSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.SAVEPOINT,
                        TIMEOUT)
                .get();
        savepointLocationFuture.complete("");

        handler.triggerSavepoint(
                        operationKey,
                        targetDirectory,
                        SavepointFormatType.CANONICAL,
                        TriggerSavepointMode.SAVEPOINT,
                        TIMEOUT)
                .get();

        // should not complete because we wait for the result to be accessed
        assertThat(
                savepointTriggerCache.closeAsync(),
                FlinkMatchers.willNotComplete(Duration.ofMillis(10)));
    }

    @Test
    public void throwsIfCacheIsShuttingDown() {
        savepointTriggerCache.closeAsync();
        assertThrows(
                IllegalStateException.class,
                () ->
                        handler.triggerSavepoint(
                                operationKey,
                                targetDirectory,
                                SavepointFormatType.CANONICAL,
                                TriggerSavepointMode.SAVEPOINT,
                                TIMEOUT));
    }

    @Test
    public void getStatus() throws ExecutionException, InterruptedException {
        handler.triggerSavepoint(
                operationKey,
                targetDirectory,
                SavepointFormatType.CANONICAL,
                TriggerSavepointMode.SAVEPOINT,
                TIMEOUT);

        String savepointLocation = "location";
        savepointLocationFuture.complete(savepointLocation);

        CompletableFuture<OperationResult<String>> statusFuture =
                handler.getSavepointStatus(operationKey);

        assertEquals(statusFuture.get(), OperationResult.success(savepointLocation));
    }

    @Test
    public void getStatusFailsIfKeyUnknown() throws InterruptedException {
        CompletableFuture<OperationResult<String>> statusFuture =
                handler.getSavepointStatus(operationKey);

        assertThat(statusFuture, futureFailedWith(UnknownOperationKeyException.class));
    }

    private abstract static class TriggerCheckpointSpyFunction
            implements TriggerCheckpointFunction {

        private final List<Tuple2<JobID, CheckpointType>> invocations = new ArrayList<>();

        @Override
        public CompletableFuture<Long> apply(
                JobID jobID, CheckpointType checkpointType, Time timeout) {
            invocations.add(new Tuple2<>(jobID, checkpointType));
            return applyWrappedFunction(jobID, checkpointType, timeout);
        }

        abstract CompletableFuture<Long> applyWrappedFunction(
                JobID jobID, CheckpointType checkpointType, Time timeout);

        public List<Tuple2<JobID, CheckpointType>> getInvocationParameters() {
            return invocations;
        }

        public int getNumberOfInvocations() {
            return invocations.size();
        }

        public static TriggerCheckpointSpyFunction wrap(
                TriggerCheckpointSpyFunction wrappedFunction) {
            return new TriggerCheckpointSpyFunction() {
                @Override
                CompletableFuture<Long> applyWrappedFunction(
                        JobID jobID, CheckpointType checkpointType, Time timeout) {
                    return wrappedFunction.apply(jobID, checkpointType, timeout);
                }
            };
        }
    }

    private abstract static class TriggerSavepointSpyFunction implements TriggerSavepointFunction {

        private final List<Tuple4<JobID, String, SavepointFormatType, TriggerSavepointMode>>
                invocations = new ArrayList<>();

        @Override
        public CompletableFuture<String> apply(
                JobID jobID,
                String targetDirectory,
                SavepointFormatType formatType,
                TriggerSavepointMode savepointMode,
                Time timeout) {
            invocations.add(new Tuple4<>(jobID, targetDirectory, formatType, savepointMode));
            return applyWrappedFunction(jobID, targetDirectory, formatType, savepointMode, timeout);
        }

        abstract CompletableFuture<String> applyWrappedFunction(
                JobID jobID,
                String targetDirectory,
                SavepointFormatType formatType,
                TriggerSavepointMode savepointMode,
                Time timeout);

        public List<Tuple4<JobID, String, SavepointFormatType, TriggerSavepointMode>>
                getInvocationParameters() {
            return invocations;
        }

        public int getNumberOfInvocations() {
            return invocations.size();
        }

        public static TriggerSavepointSpyFunction wrap(TriggerSavepointFunction wrappedFunction) {
            return new TriggerSavepointSpyFunction() {
                @Override
                CompletableFuture<String> applyWrappedFunction(
                        JobID jobID,
                        String targetDirectory,
                        SavepointFormatType formatType,
                        TriggerSavepointMode savepointMode,
                        Time timeout) {
                    return wrappedFunction.apply(
                            jobID, targetDirectory, formatType, savepointMode, timeout);
                }
            };
        }
    }
}
