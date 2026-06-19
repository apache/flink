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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** An {@link InternalFailuresListener} implementation for testing purpose. */
public class TestingInternalFailuresListener implements InternalFailuresListener {

    private final List<ExecutionAttemptID> failedTasks = new ArrayList<>();

    @Override
    public void notifyTaskFailure(
            ExecutionAttemptID attemptId,
            Throwable t,
            boolean cancelTask,
            boolean releasePartitions) {
        failedTasks.add(attemptId);
    }

    @Override
    public void notifyGlobalFailure(Throwable t) {}

    public List<ExecutionAttemptID> getFailedTasks() {
        return Collections.unmodifiableList(failedTasks);
    }
}
