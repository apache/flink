/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import java.util.concurrent.CompletableFuture;

/** {@link ExecutionGraphCache} which does nothing. */
public enum NoOpExecutionGraphCache implements ExecutionGraphCache {
    INSTANCE;

    @Override
    public int size() {
        return 0;
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> getExecutionGraphInfo(
            JobID jobId, RestfulGateway restfulGateway) {
        return FutureUtils.completedExceptionally(
                new UnsupportedOperationException(
                        "NoOpExecutionGraphCache does not support to retrieve execution graphs"));
    }

    @Override
    public void cleanup() {}

    @Override
    public void close() {}
}
