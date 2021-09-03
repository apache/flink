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

package org.apache.flink.connector.pulsar.testutils.runtime.mock;

import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.SafeRunnable;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.pulsar.shade.io.netty.util.concurrent.DefaultThreadFactory;

/** Override the default bookkeeper executor for executing in one thread executor. */
public class SameThreadOrderedSafeExecutor extends OrderedExecutor {

    public SameThreadOrderedSafeExecutor() {
        super(
                "same-thread-executor",
                1,
                new DefaultThreadFactory("test"),
                NullStatsLogger.INSTANCE,
                false,
                false,
                100000,
                -1,
                false);
    }

    @Override
    public void execute(Runnable r) {
        r.run();
    }

    @Override
    public void executeOrdered(int orderingKey, SafeRunnable r) {
        r.run();
    }

    @Override
    public void executeOrdered(long orderingKey, SafeRunnable r) {
        r.run();
    }
}
