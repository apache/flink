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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;

import java.util.concurrent.CompletableFuture;

/** RPC gateway for requesting {@link org.apache.flink.runtime.messages.ThreadInfoSample}. */
public interface TaskExecutorThreadInfoGateway {
    /**
     * Request a thread info sample from the given task.
     *
     * @param taskExecutionAttemptId identifying the task to sample
     * @param requestParams parameters of the request
     * @param timeout of the request
     * @return Future of stack trace sample response
     */
    CompletableFuture<TaskThreadInfoResponse> requestThreadInfoSamples(
            ExecutionAttemptID taskExecutionAttemptId,
            ThreadInfoSamplesRequest requestParams,
            @RpcTimeout Time timeout);
}
