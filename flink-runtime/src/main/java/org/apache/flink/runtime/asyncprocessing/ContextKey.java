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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.util.function.FunctionWithException;

import javax.annotation.Nullable;
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

    private final K rawKey;

    private final int keyGroup;

    /**
     * A record in user layer may access the state multiple times. The {@code serializedKey} can be
     * used to cache the serialized key bytes after its first serialization, so that subsequent
     * state accesses with the same key can avoid being serialized repeatedly.
     */
    private @Nullable volatile byte[] serializedKey = null;

    public ContextKey(K rawKey, int keyGroup) {
        this.rawKey = rawKey;
        this.keyGroup = keyGroup;
        this.serializedKey = null;
    }

    public ContextKey(K rawKey, int keyGroup, byte[] serializedKey) {
        this.rawKey = rawKey;
        this.keyGroup = keyGroup;
        this.serializedKey = serializedKey;
    }

    public K getRawKey() {
        return rawKey;
    }

    public int getKeyGroup() {
        return keyGroup;
    }

    /**
     * Get the serialized key. If the cached serialized key is null, the provided serialization
     * function will be called, and the serialization result will be cached by ContextKey.
     *
     * @param serializeKeyFunc the provided serialization function for this contextKey.
     * @return the serialized bytes.
     */
    public byte[] getOrCreateSerializedKey(
            FunctionWithException<ContextKey<K>, byte[], IOException> serializeKeyFunc)
            throws IOException {
        if (serializedKey != null) {
            return serializedKey;
        }
        synchronized (this) {
            if (serializedKey == null) {
                this.serializedKey = serializeKeyFunc.apply(this);
            }
        }
        return serializedKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rawKey, keyGroup);
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
        if (!Objects.equals(rawKey, that.rawKey)) {
            return false;
        }
        return Objects.equals(keyGroup, that.keyGroup);
    }
}
