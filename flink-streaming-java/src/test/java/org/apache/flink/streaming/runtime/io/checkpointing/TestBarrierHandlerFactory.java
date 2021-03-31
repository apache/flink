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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler.Cancellable;
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

/** A factory for creating instances of {@link SingleCheckpointBarrierHandler} for tests. */
public class TestBarrierHandlerFactory {
    private final AbstractInvokable target;
    private BiFunction<Callable<?>, Duration, Cancellable> actionRegistration =
            (callable, delay) -> () -> {};
    private Clock clock = SystemClock.getInstance();

    private TestBarrierHandlerFactory(AbstractInvokable target) {
        this.target = target;
    }

    public static TestBarrierHandlerFactory forTarget(AbstractInvokable target) {
        return new TestBarrierHandlerFactory(target);
    }

    public TestBarrierHandlerFactory withActionRegistration(
            BiFunction<Callable<?>, Duration, Cancellable> actionRegistration) {
        this.actionRegistration = actionRegistration;
        return this;
    }

    public TestBarrierHandlerFactory withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public SingleCheckpointBarrierHandler create(SingleInputGate inputGate) {
        return create(inputGate, new RecordingChannelStateWriter());
    }

    public SingleCheckpointBarrierHandler create(
            SingleInputGate inputGate, ChannelStateWriter stateWriter) {
        String taskName = "test";
        return SingleCheckpointBarrierHandler.alternating(
                taskName,
                target,
                new TestSubtaskCheckpointCoordinator(stateWriter),
                clock,
                inputGate.getNumberOfInputChannels(),
                actionRegistration,
                inputGate);
    }
}
