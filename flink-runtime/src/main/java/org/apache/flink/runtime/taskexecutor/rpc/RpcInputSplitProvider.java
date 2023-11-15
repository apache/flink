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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProviderException;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class RpcInputSplitProvider implements InputSplitProvider {
    private final JobMasterGateway jobMasterGateway;
    private final JobVertexID jobVertexID;
    private final ExecutionAttemptID executionAttemptID;
    private final Duration timeout;

    public RpcInputSplitProvider(
            JobMasterGateway jobMasterGateway,
            JobVertexID jobVertexID,
            ExecutionAttemptID executionAttemptID,
            Duration timeout) {
        this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
        this.jobVertexID = Preconditions.checkNotNull(jobVertexID);
        this.executionAttemptID = Preconditions.checkNotNull(executionAttemptID);
        this.timeout = Preconditions.checkNotNull(timeout);
    }

    @Override
    public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader)
            throws InputSplitProviderException {
        Preconditions.checkNotNull(userCodeClassLoader);

        CompletableFuture<SerializedInputSplit> futureInputSplit =
                jobMasterGateway.requestNextInputSplit(jobVertexID, executionAttemptID);

        try {
            SerializedInputSplit serializedInputSplit =
                    futureInputSplit.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (serializedInputSplit.isEmpty()) {
                return null;
            } else {
                return InstantiationUtil.deserializeObject(
                        serializedInputSplit.getInputSplitData(), userCodeClassLoader);
            }
        } catch (Exception e) {
            throw new InputSplitProviderException("Requesting the next input split failed.", e);
        }
    }
}
