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

package org.apache.flink.runtime.operators.lifecycle.validation;

import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointCompletedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.CheckpointStartedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.OperatorFinishedEvent;
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;
import org.apache.flink.runtime.operators.lifecycle.event.WatermarkReceivedEvent;
import org.apache.flink.streaming.api.operators.StreamOperator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Checks that {@link StreamOperator#finish()} was called and then only checkpoints were starting or
 * completing and at least one of them completed.
 */
public class FinishingValidator implements TestOperatorLifecycleValidator {

    @Override
    public void validateOperatorLifecycle(
            TestJobWithDescription job,
            String operatorId,
            int subtaskIndex,
            List<TestEvent> operatorEvents) {
        boolean opFinished = false;
        Set<Long> finalCheckpointCandidates = new HashSet<>();
        for (TestEvent ev : operatorEvents) {
            if (ev instanceof OperatorFinishedEvent) {
                opFinished = true;
            } else if (ev instanceof CheckpointStartedEvent) {
                if (opFinished) {
                    finalCheckpointCandidates.add(((CheckpointStartedEvent) ev).checkpointID);
                }
            } else if (ev instanceof CheckpointCompletedEvent) {
                if (finalCheckpointCandidates.contains(
                        ((CheckpointCompletedEvent) ev).checkpointID)) {
                    return;
                }
            } else if (opFinished) {
                fail(
                        format(
                                "Unexpected event after operator %s[%d] finished: %s",
                                operatorId, subtaskIndex, ev));
            }
        }
        assertTrue(
                format(
                        "Operator %s[%d] wasn't finished (events: %s)",
                        operatorId, subtaskIndex, operatorEvents),
                opFinished);
        fail(
                format(
                        "Operator %s[%d] was finished but didn't finish the checkpoint after that;"
                                + "checkpoints started after finish: %s (events (excluding watermarks): %s)",
                        operatorId,
                        subtaskIndex,
                        finalCheckpointCandidates,
                        operatorEvents.stream()
                                .filter(ev -> !(ev instanceof WatermarkReceivedEvent))
                                .collect(toList())));
    }
}
