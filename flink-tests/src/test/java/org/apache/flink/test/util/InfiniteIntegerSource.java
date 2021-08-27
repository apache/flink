/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.util;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/** Source that emits an integers indefinitely. */
public class InfiniteIntegerSource implements ParallelSourceFunction<Integer> {

    private static final long serialVersionUID = 1L;

    private volatile boolean running = true;

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        int counter = 0;
        while (running) {
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(counter++);
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
