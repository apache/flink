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

package org.apache.flink.connector.pulsar.testutils.function;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Fails after a checkpoint is taken and the next record was emitted. */
public class FailingOnCheckpoint extends RichMapFunction<Long, Long> implements CheckpointListener {
    private static final long serialVersionUID = -2595809047617609928L;

    private volatile long lastCheckpointId = 0;

    private final AtomicInteger emittedBetweenCheckpoint = new AtomicInteger(0);
    private final SharedReference<AtomicBoolean> failed;

    public FailingOnCheckpoint(SharedObjectsExtension sharedObjects) {
        this.failed = sharedObjects.add(new AtomicBoolean(false));
    }

    @Override
    public Long map(Long value) throws Exception {
        if (lastCheckpointId >= 1 && emittedBetweenCheckpoint.get() > 0 && !failed.get().get()) {
            failed.get().set(true);
            throw new RuntimeException("Planned exception.");
        }
        // Delay execution to ensure that at-least one checkpoint is triggered before finish
        Thread.sleep(50);
        emittedBetweenCheckpoint.incrementAndGet();

        return value;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        lastCheckpointId = checkpointId;
        emittedBetweenCheckpoint.set(0);
    }
}
