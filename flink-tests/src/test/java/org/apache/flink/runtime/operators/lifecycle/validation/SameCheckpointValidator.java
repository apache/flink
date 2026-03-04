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
import org.apache.flink.runtime.operators.lifecycle.event.TestEvent;

import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

/** Checks that all subtasks have received the same last checkpoint. */
public class SameCheckpointValidator implements TestOperatorLifecycleValidator {

    private final long lastCheckpointID;

    public SameCheckpointValidator(long lastCheckpointID) {
        this.lastCheckpointID = lastCheckpointID;
    }

    @Override
    public void validateOperatorLifecycle(
            TestJobWithDescription job,
            String operatorId,
            int subtaskIndex,
            List<TestEvent> operatorEvents) {

        boolean started = false;
        boolean finished = false;
        for (TestEvent ev : operatorEvents) {
            if (ev instanceof CheckpointStartedEvent) {
                if (lastCheckpointID == ((CheckpointStartedEvent) ev).checkpointID) {
                    assertThat(started)
                            .as(
                                    format(
                                            "Operator %s[%d] started checkpoint %d twice",
                                            operatorId, subtaskIndex, lastCheckpointID))
                            .isFalse();
                    started = true;
                }
            } else if (ev instanceof CheckpointCompletedEvent) {
                if (lastCheckpointID == ((CheckpointCompletedEvent) ev).checkpointID) {
                    assertThat(started)
                            .as(
                                    format(
                                            "Operator %s[%d] finished checkpoint %d before starting",
                                            operatorId, subtaskIndex, lastCheckpointID))
                            .isTrue();
                    assertThat(finished)
                            .as(
                                    format(
                                            "Operator %s[%d] finished checkpoint %d twice",
                                            operatorId, subtaskIndex, lastCheckpointID))
                            .isFalse();
                    finished = true;
                }
            }
        }
        assertThat(finished)
                .as(
                        format(
                                "Operator %s[%d] didn't finish checkpoint %d (events: %s)",
                                operatorId, subtaskIndex, lastCheckpointID, operatorEvents))
                .isTrue();
    }
}
