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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

@Internal
public class SingleThreadAccessCheckingTypeSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 131020282727167064L;

    private final SingleThreadAccessChecker singleThreadAccessChecker;
    private final TypeSerializer<T> originalSerializer;

    public SingleThreadAccessCheckingTypeSerializer(TypeSerializer<T> originalSerializer) {
        this.singleThreadAccessChecker = new SingleThreadAccessChecker();
        this.originalSerializer = originalSerializer;
    }

    @Override
    public boolean isImmutableType() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.isImmutableType();
        }
    }

    @Override
    public TypeSerializer<T> duplicate() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return new SingleThreadAccessCheckingTypeSerializer<>(originalSerializer.duplicate());
        }
    }

    @Override
    public T createInstance() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.createInstance();
        }
    }

    @Override
    public T copy(T from) {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.copy(from);
        }
    }

    @Override
    public T copy(T from, T reuse) {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.copy(from, reuse);
        }
    }

    @Override
    public int getLength() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.getLength();
        }
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            originalSerializer.serialize(record, target);
        }
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.deserialize(source);
        }
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.deserialize(reuse, source);
        }
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            originalSerializer.copy(source, target);
        }
    }

    @Override
    public boolean equals(Object obj) {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return obj == this
                    || (obj != null
                            && obj.getClass() == getClass()
                            && originalSerializer.equals(obj));
        }
    }

    @Override
    public int hashCode() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return originalSerializer.hashCode();
        }
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            return new SingleThreadAccessCheckingTypeSerializerSnapshot<>(this);
        }
    }

    public static class SingleThreadAccessCheckingTypeSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<
                    T, SingleThreadAccessCheckingTypeSerializer<T>> {

        @SuppressWarnings({"unchecked", "unused"})
        public SingleThreadAccessCheckingTypeSerializerSnapshot() {
            super(
                    (Class<SingleThreadAccessCheckingTypeSerializer<T>>)
                            (Class<?>) SingleThreadAccessCheckingTypeSerializer.class);
        }

        SingleThreadAccessCheckingTypeSerializerSnapshot(
                SingleThreadAccessCheckingTypeSerializer<T> serializerInstance) {
            super(serializerInstance);
        }

        @Override
        protected int getCurrentOuterSnapshotVersion() {
            return 1;
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                SingleThreadAccessCheckingTypeSerializer<T> outerSerializer) {
            return new TypeSerializer[] {outerSerializer.originalSerializer};
        }

        @SuppressWarnings("unchecked")
        @Override
        protected SingleThreadAccessCheckingTypeSerializer<T>
                createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {

            return new SingleThreadAccessCheckingTypeSerializer<>(
                    (TypeSerializer<T>) nestedSerializers[0]);
        }
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        try (SingleThreadAccessCheck ignored =
                singleThreadAccessChecker.startSingleThreadAccessCheck()) {
            outputStream.defaultWriteObject();
        }
    }

    private static class SingleThreadAccessChecker implements Serializable {
        private static final long serialVersionUID = 131020282727167064L;

        private transient AtomicReference<Thread> currentThreadRef = new AtomicReference<>();

        SingleThreadAccessCheck startSingleThreadAccessCheck() {
            assert (currentThreadRef.compareAndSet(null, Thread.currentThread()))
                    : "The checker has concurrent access from " + currentThreadRef.get();
            return new SingleThreadAccessCheck(currentThreadRef);
        }

        private void readObject(ObjectInputStream inputStream)
                throws ClassNotFoundException, IOException {
            inputStream.defaultReadObject();
            currentThreadRef = new AtomicReference<>();
        }
    }

    private static class SingleThreadAccessCheck implements AutoCloseable {
        private final AtomicReference<Thread> currentThreadRef;

        private SingleThreadAccessCheck(AtomicReference<Thread> currentThreadRef) {
            this.currentThreadRef = currentThreadRef;
        }

        @Override
        public void close() {
            assert (currentThreadRef.compareAndSet(Thread.currentThread(), null))
                    : "The checker has concurrent access from " + currentThreadRef.get();
        }
    }
}
