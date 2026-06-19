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

package org.apache.flink.table.gateway.api.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/** Utils for thread pool executor. */
@Internal
public class ThreadUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadUtils.class);

    public static ThreadPoolExecutor newThreadPool(
            int poolSize, int poolQueueSize, long keepAliveMs, String threadPoolName) {
        LOG.info(
                "Created thread pool {} with core size {}, max size {} and keep alive time {}ms.",
                threadPoolName,
                poolSize,
                poolQueueSize,
                keepAliveMs);

        return new ThreadPoolExecutor(
                poolSize,
                poolQueueSize,
                keepAliveMs,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                new ExecutorThreadFactory(threadPoolName));
    }
}
