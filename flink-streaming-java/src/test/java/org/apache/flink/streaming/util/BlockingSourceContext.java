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

package org.apache.flink.streaming.util;

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Test SourceContext. */
public class BlockingSourceContext<T> implements SourceFunction.SourceContext<T> {

    private final String name;

    private final Object lock;
    private final OneShotLatch latchToTrigger;
    private final OneShotLatch latchToWait;
    private final ConcurrentHashMap<String, List<T>> collector;

    private final int threshold;
    private int counter = 0;

    private final List<T> localOutput;

    public BlockingSourceContext(
            String name,
            OneShotLatch latchToTrigger,
            OneShotLatch latchToWait,
            ConcurrentHashMap<String, List<T>> output,
            int elemToFire) {
        this.name = name;
        this.lock = new Object();
        this.latchToTrigger = latchToTrigger;
        this.latchToWait = latchToWait;
        this.collector = output;
        this.threshold = elemToFire;

        this.localOutput = new ArrayList<>();
        List<T> prev = collector.put(name, localOutput);
        assertThat(prev).isNull();
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {
        collect(element);
    }

    @Override
    public void collect(T element) {
        localOutput.add(element);
        if (++counter == threshold) {
            latchToTrigger.trigger();
            try {
                if (!latchToWait.isTriggered()) {
                    latchToWait.await();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void emitWatermark(Watermark mark) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void markAsTemporarilyIdle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getCheckpointLock() {
        return lock;
    }

    @Override
    public void close() {}
}
