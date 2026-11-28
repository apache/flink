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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Holds the executors and the in-flight tasks that are used to lazily fetch the detail content of
 * job archives.
 *
 * <p>The registry is shared between the {@link HistoryServerArchiveFetcher} and the {@link
 * HistoryServerApplicationArchiveFetcher} so that a pending fetch task is visible to both of them,
 * independent of which fetcher scheduled it.
 *
 * <p>Tasks are scheduled on one of two executors:
 *
 * <ul>
 *   <li>the common executor, which processes all archives that were found by a directory scan;
 *   <li>the individual executor, which processes the archives that are requested by a user and
 *       therefore have to be prioritized.
 * </ul>
 */
class LazyFetchTaskRegistry implements AutoCloseable {

    /** Executor for loading archives found by a directory scan. */
    private final ExecutorService commonFetchExecutor;

    /** Executor for loading archives that are requested by a user. */
    private final ExecutorService individualFetchExecutor;

    private final Map<String, Future<?>> commonFetchTasks;
    private final Map<String, Future<?>> individualFetchTasks;

    LazyFetchTaskRegistry(int commonPoolSize, int individualPoolSize) {
        this.commonFetchExecutor =
                Executors.newFixedThreadPool(
                        commonPoolSize,
                        new ExecutorThreadFactory("HistoryServer-commonFetchExecutor"));
        this.individualFetchExecutor =
                Executors.newFixedThreadPool(
                        individualPoolSize,
                        new ExecutorThreadFactory("HistoryServer-individualFetchExecutor"));
        this.commonFetchTasks = new ConcurrentHashMap<>();
        this.individualFetchTasks = new ConcurrentHashMap<>();
    }

    ExecutorService getFetchExecutor(boolean individual) {
        return individual ? individualFetchExecutor : commonFetchExecutor;
    }

    Map<String, Future<?>> getFetchTasks(boolean individual) {
        return individual ? individualFetchTasks : commonFetchTasks;
    }

    boolean hasCommonFetchTask(String archiveId) {
        return commonFetchTasks.containsKey(archiveId);
    }

    boolean hasIndividualFetchTask(String archiveId) {
        return individualFetchTasks.containsKey(archiveId);
    }

    Future<?> getCommonFetchTask(String archiveId) {
        return commonFetchTasks.get(archiveId);
    }

    /** Cancels and forgets all tasks that are currently registered for the given archive. */
    void cancelFetchTasks(String archiveId) {
        Future<?> commonFetchTask = commonFetchTasks.get(archiveId);
        if (commonFetchTask != null) {
            commonFetchTask.cancel(true);
            commonFetchTasks.remove(archiveId);
        }
        Future<?> individualFetchTask = individualFetchTasks.get(archiveId);
        if (individualFetchTask != null) {
            individualFetchTask.cancel(true);
            individualFetchTasks.remove(archiveId);
        }
    }

    /** Waits until all tasks that are currently registered for the given archive are finished. */
    void waitFetchTasksFinished(String archiveId) throws Exception {
        Future<?> commonFetchTask = commonFetchTasks.get(archiveId);
        if (commonFetchTask != null) {
            commonFetchTask.get();
        }
        Future<?> individualFetchTask = individualFetchTasks.get(archiveId);
        if (individualFetchTask != null) {
            individualFetchTask.get();
        }
    }

    @Override
    public void close() {
        ExecutorUtils.gracefulShutdown(
                1L, TimeUnit.SECONDS, commonFetchExecutor, individualFetchExecutor);
    }
}
