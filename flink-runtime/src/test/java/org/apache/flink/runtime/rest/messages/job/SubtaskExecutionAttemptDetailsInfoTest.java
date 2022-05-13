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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;

import java.util.Random;

/** Tests (un)marshalling of the {@link SubtaskExecutionAttemptDetailsInfo}. */
public class SubtaskExecutionAttemptDetailsInfoTest
        extends RestResponseMarshallingTestBase<SubtaskExecutionAttemptDetailsInfo> {

    @Override
    protected Class<SubtaskExecutionAttemptDetailsInfo> getTestResponseClass() {
        return SubtaskExecutionAttemptDetailsInfo.class;
    }

    @Override
    protected SubtaskExecutionAttemptDetailsInfo getTestResponseInstance() throws Exception {
        final Random random = new Random();

        final IOMetricsInfo ioMetricsInfo =
                new IOMetricsInfo(
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean(),
                        Math.abs(random.nextLong()),
                        random.nextBoolean());

        return new SubtaskExecutionAttemptDetailsInfo(
                Math.abs(random.nextInt()),
                ExecutionState.values()[random.nextInt(ExecutionState.values().length)],
                Math.abs(random.nextInt()),
                "localhost:" + random.nextInt(65536),
                Math.abs(random.nextLong()),
                Math.abs(random.nextLong()),
                Math.abs(random.nextLong()),
                ioMetricsInfo,
                "taskmanagerId");
    }
}
