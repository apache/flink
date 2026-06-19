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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;

import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

public class ChannelStateWriteResultUtil {

    public static void assertHasSpecialCause(
            ChannelStateWriter.ChannelStateWriteResult result, Class<? extends Throwable> type) {
        assertThatThrownBy(() -> result.getInputChannelStateHandles().get())
                .hasCauseInstanceOf(type);
        assertThatThrownBy(() -> result.getResultSubpartitionStateHandles().get())
                .hasCauseInstanceOf(type);
    }

    public static void assertCheckpointFailureReason(
            ChannelStateWriter.ChannelStateWriteResult result,
            CheckpointFailureReason checkpointFailureReason) {
        assertThatThrownBy(() -> result.getInputChannelStateHandles().get())
                .cause()
                .asInstanceOf(type(CheckpointException.class))
                .satisfies(
                        checkpointException ->
                                assertThat(checkpointException.getCheckpointFailureReason())
                                        .isEqualTo(checkpointFailureReason));

        assertThatThrownBy(() -> result.getResultSubpartitionStateHandles().get())
                .cause()
                .asInstanceOf(type(CheckpointException.class))
                .satisfies(
                        checkpointException ->
                                assertThat(checkpointException.getCheckpointFailureReason())
                                        .isEqualTo(checkpointFailureReason));
    }

    public static void assertAllSubtaskDoneNormally(
            Collection<ChannelStateWriter.ChannelStateWriteResult> results) {
        assertThat(results)
                .allMatch(ChannelStateWriter.ChannelStateWriteResult::isDone)
                .allMatch(
                        result -> !result.getInputChannelStateHandles().isCompletedExceptionally())
                .allMatch(
                        result ->
                                !result.getResultSubpartitionStateHandles()
                                        .isCompletedExceptionally());
    }

    public static void assertAllSubtaskNotDone(
            Collection<ChannelStateWriter.ChannelStateWriteResult> results) {
        assertThat(results).allMatch(result -> !result.isDone());
    }
}
