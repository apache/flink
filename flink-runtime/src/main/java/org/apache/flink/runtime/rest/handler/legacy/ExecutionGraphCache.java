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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

/**
 * Cache for {@link ExecutionGraphInfo} which are obtained from the Flink cluster. Every cache entry
 * has an associated time to live after which a new request will trigger the reloading of the {@link
 * ExecutionGraphInfo} from the cluster.
 */
public interface ExecutionGraphCache extends Closeable {

    /** Gets the number of cache entries. */
    int size();

    /**
     * Gets the {@link ExecutionGraphInfo} for the given {@link JobID} and caches it. The {@link
     * ExecutionGraphInfo} will be requested again after the refresh interval has passed or if the
     * graph could not be retrieved from the given gateway.
     *
     * @param jobId identifying the {@link ExecutionGraphInfo} to get
     * @param restfulGateway to request the {@link ExecutionGraphInfo} from
     * @return Future containing the requested {@link ExecutionGraphInfo}
     */
    CompletableFuture<ExecutionGraphInfo> getExecutionGraphInfo(
            JobID jobId, RestfulGateway restfulGateway);

    /** Perform the cleanup of out dated cache entries. */
    void cleanup();

    /** Closes the execution graph cache. */
    @Override
    void close();
}
