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

package org.apache.flink.streaming.util;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

/** Collecting {@link SourceFunction.SourceContext}. */
public class CollectingSourceContext<T extends Serializable>
        implements SourceFunction.SourceContext<T> {

    private final Object lock;
    private final Collection<T> collection;

    public CollectingSourceContext(Object lock, Collection<T> collection) {
        this.lock = lock;
        this.collection = collection;
    }

    @Override
    public void collect(T element) {
        try {
            collection.add(CommonTestUtils.createCopySerializable(element));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void collectWithTimestamp(T element, long timestamp) {
        collect(element);
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
