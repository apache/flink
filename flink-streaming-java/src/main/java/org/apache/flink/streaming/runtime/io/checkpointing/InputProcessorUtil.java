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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.io.InputGateUtil;
import org.apache.flink.streaming.runtime.io.StreamOneInputProcessor;
import org.apache.flink.streaming.runtime.io.StreamTaskSourceInput;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler.Cancellable;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.streaming.runtime.tasks.TimerService;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.function.BiFunction;
import java.util.stream.Stream;

/**
 * Utility for creating {@link CheckpointedInputGate} based on checkpoint mode for {@link
 * StreamOneInputProcessor}.
 */
@Internal
public class InputProcessorUtil {

    public static CheckpointedInputGate[] createCheckpointedMultipleInputGate(
            MailboxExecutor mailboxExecutor,
            List<IndexedInputGate>[] inputGates,
            TaskIOMetricGroup taskIOMetricGroup,
            CheckpointBarrierHandler barrierHandler,
            StreamConfig config) {

        registerCheckpointMetrics(taskIOMetricGroup, barrierHandler);

        InputGate[] unionedInputGates =
                Arrays.stream(inputGates)
                        .map(InputGateUtil::createInputGate)
                        .toArray(InputGate[]::new);

        return Arrays.stream(unionedInputGates)
                .map(
                        unionedInputGate ->
                                new CheckpointedInputGate(
                                        unionedInputGate,
                                        barrierHandler,
                                        mailboxExecutor,
                                        config.isGraphContainingLoops()
                                                ? UpstreamRecoveryTracker.NO_OP
                                                : UpstreamRecoveryTracker.forInputGate(
                                                        unionedInputGate)))
                .toArray(CheckpointedInputGate[]::new);
    }

    public static CheckpointBarrierHandler createCheckpointBarrierHandler(
            CheckpointableTask toNotifyOnCheckpoint,
            StreamConfig config,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            List<IndexedInputGate>[] inputGates,
            List<StreamTaskSourceInput<?>> sourceInputs,
            MailboxExecutor mailboxExecutor,
            TimerService timerService) {

        CheckpointableInput[] inputs =
                Stream.<CheckpointableInput>concat(
                                Arrays.stream(inputGates).flatMap(Collection::stream),
                                sourceInputs.stream())
                        .sorted(Comparator.comparing(CheckpointableInput::getInputGateIndex))
                        .toArray(CheckpointableInput[]::new);

        Clock clock = SystemClock.getInstance();
        switch (config.getCheckpointMode()) {
            case EXACTLY_ONCE:
                int numberOfChannels =
                        (int)
                                Arrays.stream(inputs)
                                        .mapToLong(gate -> gate.getChannelInfos().size())
                                        .sum();
                return createBarrierHandler(
                        toNotifyOnCheckpoint,
                        config,
                        checkpointCoordinator,
                        taskName,
                        mailboxExecutor,
                        timerService,
                        inputs,
                        clock,
                        numberOfChannels);
            case AT_LEAST_ONCE:
                if (config.isUnalignedCheckpointsEnabled()) {
                    throw new IllegalStateException(
                            "Cannot use unaligned checkpoints with AT_LEAST_ONCE "
                                    + "checkpointing mode");
                }
                int numInputChannels =
                        Arrays.stream(inputs)
                                .mapToInt(CheckpointableInput::getNumberOfInputChannels)
                                .sum();
                return new CheckpointBarrierTracker(
                        numInputChannels,
                        toNotifyOnCheckpoint,
                        clock,
                        config.getConfiguration()
                                .get(
                                        ExecutionCheckpointingOptions
                                                .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH));
            default:
                throw new UnsupportedOperationException(
                        "Unrecognized Checkpointing Mode: " + config.getCheckpointMode());
        }
    }

    private static SingleCheckpointBarrierHandler createBarrierHandler(
            CheckpointableTask toNotifyOnCheckpoint,
            StreamConfig config,
            SubtaskCheckpointCoordinator checkpointCoordinator,
            String taskName,
            MailboxExecutor mailboxExecutor,
            TimerService timerService,
            CheckpointableInput[] inputs,
            Clock clock,
            int numberOfChannels) {
        boolean enableCheckpointAfterTasksFinished =
                config.getConfiguration()
                        .get(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH);
        if (config.isUnalignedCheckpointsEnabled()) {
            return SingleCheckpointBarrierHandler.alternating(
                    taskName,
                    toNotifyOnCheckpoint,
                    checkpointCoordinator,
                    clock,
                    numberOfChannels,
                    createRegisterTimerCallback(mailboxExecutor, timerService),
                    enableCheckpointAfterTasksFinished,
                    inputs);
        } else {
            return SingleCheckpointBarrierHandler.aligned(
                    taskName,
                    toNotifyOnCheckpoint,
                    clock,
                    numberOfChannels,
                    createRegisterTimerCallback(mailboxExecutor, timerService),
                    enableCheckpointAfterTasksFinished,
                    inputs);
        }
    }

    private static BiFunction<Callable<?>, Duration, Cancellable> createRegisterTimerCallback(
            MailboxExecutor mailboxExecutor, TimerService timerService) {
        return (callable, delay) -> {
            ScheduledFuture<?> scheduledFuture =
                    timerService.registerTimer(
                            timerService.getCurrentProcessingTime() + delay.toMillis(),
                            timestamp ->
                                    mailboxExecutor.submit(
                                            callable,
                                            "Execute checkpoint barrier handler delayed action"));
            return () -> scheduledFuture.cancel(false);
        };
    }

    private static void registerCheckpointMetrics(
            TaskIOMetricGroup taskIOMetricGroup, CheckpointBarrierHandler barrierHandler) {
        taskIOMetricGroup.gauge(
                MetricNames.CHECKPOINT_ALIGNMENT_TIME, barrierHandler::getAlignmentDurationNanos);
        taskIOMetricGroup.gauge(
                MetricNames.CHECKPOINT_START_DELAY_TIME,
                barrierHandler::getCheckpointStartDelayNanos);
    }
}
