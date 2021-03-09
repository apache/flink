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
import org.apache.flink.streaming.runtime.tasks.TestSubtaskCheckpointCoordinator;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

/** A builder for creating instances of {@link SingleCheckpointBarrierHandler} for tests. */
public class TestBarrierHandlerBuilder {
    private final AbstractInvokable target;
    private Clock clock = SystemClock.getInstance();

    private TestBarrierHandlerBuilder(AbstractInvokable target) {
        this.target = target;
    }

    public static TestBarrierHandlerBuilder builder(AbstractInvokable target) {
        return new TestBarrierHandlerBuilder(target);
    }

    public TestBarrierHandlerBuilder withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public SingleCheckpointBarrierHandler build(SingleInputGate inputGate) {
        return build(inputGate, new RecordingChannelStateWriter());
    }

    public SingleCheckpointBarrierHandler build(
            SingleInputGate inputGate, ChannelStateWriter stateWriter) {
        String taskName = "test";
        return new SingleCheckpointBarrierHandler(
                taskName,
                target,
                clock,
                inputGate.getNumberOfInputChannels(),
                new AlternatingController(
                        new AlignedController(inputGate),
                        new UnalignedController(
                                new TestSubtaskCheckpointCoordinator(stateWriter), inputGate),
                        clock));
    }
}
