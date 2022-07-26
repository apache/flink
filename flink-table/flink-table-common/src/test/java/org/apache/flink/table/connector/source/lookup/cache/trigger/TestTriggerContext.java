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

package org.apache.flink.table.connector.source.lookup.cache.trigger;

import java.util.concurrent.CompletableFuture;

/** Test implementation of {@link CacheReloadTrigger.Context}. */
public class TestTriggerContext implements CacheReloadTrigger.Context {

    private final TestReloadTask reloadTask = new TestReloadTask();

    public TestReloadTask getReloadTask() {
        return reloadTask;
    }

    @Override
    public long currentProcessingTime() {
        return System.currentTimeMillis();
    }

    @Override
    public long currentWatermark() {
        return System.currentTimeMillis();
    }

    @Override
    public CompletableFuture<Void> triggerReload() {
        reloadTask.run();
        return CompletableFuture.completedFuture(null);
    }

    /** Test reload task that counts number of loads. */
    public static class TestReloadTask implements Runnable {

        private int numLoads;

        @Override
        public void run() {
            numLoads++;
        }

        public int getNumLoads() {
            return numLoads;
        }
    }
}
