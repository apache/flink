/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests.artificialstate;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/** A custom stateful serializer to test that serializers are not used concurrently. */
public class StatefulComplexPayloadSerializer extends TypeSerializer<ComplexPayload> {

    private static final long serialVersionUID = 8766687317209282373L;

    /** This holds the thread that currently has exclusive ownership over the serializer. */
    private final AtomicReference<Thread> currentOwnerThread;

    public StatefulComplexPayloadSerializer() {
        this.currentOwnerThread = new AtomicReference<>(null);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ComplexPayload> duplicate() {
        return new StatefulComplexPayloadSerializer();
    }

    @Override
    public ComplexPayload createInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComplexPayload copy(ComplexPayload from) {
        try {
            Thread currentThread = Thread.currentThread();
            if (currentOwnerThread.compareAndSet(null, currentThread)) {
                return InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(from),
                        currentThread.getContextClassLoader());
            } else {
                throw new IllegalStateException("Concurrent access to type serializer detected!");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            currentOwnerThread.set(null);
        }
    }

    @Override
    public ComplexPayload copy(ComplexPayload from, ComplexPayload reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(ComplexPayload record, DataOutputView target) throws IOException {
        try {
            if (currentOwnerThread.compareAndSet(null, Thread.currentThread())) {
                target.write(InstantiationUtil.serializeObject(record));
            } else {
                throw new IllegalStateException("Concurrent access to type serializer detected!");
            }
        } finally {
            currentOwnerThread.set(null);
        }
    }

    @Override
    public ComplexPayload deserialize(DataInputView source) throws IOException {
        try (final DataInputViewStream inViewWrapper = new DataInputViewStream(source)) {
            Thread currentThread = Thread.currentThread();
            if (currentOwnerThread.compareAndSet(null, currentThread)) {
                return InstantiationUtil.deserializeObject(
                        inViewWrapper, currentThread.getContextClassLoader());
            } else {
                throw new IllegalStateException("Concurrent access to type serializer detected!");
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not deserialize object.", e);
        } finally {
            currentOwnerThread.set(null);
        }
    }

    @Override
    public ComplexPayload deserialize(ComplexPayload reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this;
    }

    @Override
    public int hashCode() {
        return 42;
    }

    @Override
    public Snapshot snapshotConfiguration() {
        return new Snapshot();
    }

    // ----------------------------------------------------------------------------------------

    /** Snapshot for the {@link StatefulComplexPayloadSerializer}. */
    public static class Snapshot extends SimpleTypeSerializerSnapshot<ComplexPayload> {
        public Snapshot() {
            super(StatefulComplexPayloadSerializer::new);
        }
    }
}
