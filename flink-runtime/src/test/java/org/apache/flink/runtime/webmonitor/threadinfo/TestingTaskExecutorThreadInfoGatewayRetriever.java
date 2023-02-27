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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.webmonitor.retriever.AddressBasedGatewayRetriever;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Mock testing retriever for {@link TaskExecutorThreadInfoGateway}. */
public class TestingTaskExecutorThreadInfoGatewayRetriever
        implements AddressBasedGatewayRetriever<TaskExecutorThreadInfoGateway> {
    private final Map<String, TaskExecutorThreadInfoGateway> addressToGateway;

    public TestingTaskExecutorThreadInfoGatewayRetriever(
            Map<String, TaskExecutorThreadInfoGateway> addressToGateway) {
        this.addressToGateway = Preconditions.checkNotNull(addressToGateway);
    }

    @Override
    public CompletableFuture<TaskExecutorThreadInfoGateway> getFutureFromAddress(String address) {
        TaskExecutorThreadInfoGateway taskExecutorThreadInfoGateway = addressToGateway.get(address);
        return taskExecutorThreadInfoGateway == null
                ? FutureUtils.completedExceptionally(
                        new IllegalStateException("can not find target gateway."))
                : CompletableFuture.completedFuture(taskExecutorThreadInfoGateway);
    }
}
