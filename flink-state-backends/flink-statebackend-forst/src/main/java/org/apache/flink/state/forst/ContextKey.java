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

package org.apache.flink.state.forst;

import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.Objects;

/**
 * The composite key which contains some context information, such as keyGroup, etc.
 *
 * @param <K> The type of the raw key.
 */
@ThreadSafe
public class ContextKey<K> {

    private final RecordContext<K> recordContext;

    public ContextKey(RecordContext<K> recordContext) {
        this.recordContext = recordContext;
    }

    public K getRawKey() {
        return recordContext.getKey();
    }

    public int getKeyGroup() {
        return recordContext.getKeyGroup();
    }

    /**
     * Get the serialized key. If the cached serialized key within {@code RecordContext#payload} is
     * null, the provided serialization function will be called, and the serialization result will
     * be cached by {@code RecordContext#payload}.
     *
     * @param serializeKeyFunc the provided serialization function for this contextKey.
     * @return the serialized bytes.
     */
    public byte[] getOrCreateSerializedKey(
            FunctionWithException<ContextKey<K>, byte[], IOException> serializeKeyFunc)
            throws IOException {
        if (recordContext.getExtra() != null) {
            return (byte[]) recordContext.getExtra();
        }
        synchronized (recordContext) {
            if (recordContext.getExtra() == null) {
                byte[] serializedKey = serializeKeyFunc.apply(this);
                recordContext.setExtra(serializedKey);
            }
        }
        return (byte[]) recordContext.getExtra();
    }

    @Override
    public int hashCode() {
        return Objects.hash(recordContext);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ContextKey<?> that = (ContextKey<?>) o;
        return Objects.equals(recordContext, that.recordContext);
    }
}
