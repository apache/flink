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

package org.apache.flink.streaming.connectors.dynamodb.batch.concurrent;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Unbounded task executor with unbounded internal queue. We do not want the internal quque to be
 * bounded as we would like to avoid rejections of the tasks. Throttling is controlled outside of
 * the executor.
 */
public class UnboundedTaskExecutor extends ThreadPoolExecutor {

    public UnboundedTaskExecutor(
            int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit) {
        super(
                corePoolSize,
                maximumPoolSize,
                keepAliveTime,
                unit,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public UnboundedTaskExecutor() {
        this(
                calculateCorePoolSize(),
                calculateCorePoolSize(),
                5L, // doesn't matter as core pool size is the same as maximum pool size
                TimeUnit.MINUTES);
    }

    /**
     * Get optimal core pool size using Brian Goetz formula: Number of threads = Number of Available
     * Cores * (1 + Wait time / Service time) Wait time is average time spent on IO (~ 40 ms for
     * DynamoDB) Service time is the service overhead to process request/response.
     */
    private static int calculateCorePoolSize() {
        return Runtime.getRuntime().availableProcessors() * (1 + 40 / 2);
    }
}
