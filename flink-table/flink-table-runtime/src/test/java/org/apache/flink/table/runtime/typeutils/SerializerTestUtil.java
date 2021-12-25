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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;

/** Utils for testing serializers. */
public class SerializerTestUtil {

    /** Snapshot and restore the given serializer. Returns the restored serializer. */
    public static <T> TypeSerializer<T> snapshotAndReconfigure(
            TypeSerializer<T> serializer, SerializerGetter<T> serializerGetter) throws IOException {
        TypeSerializerSnapshot<T> configSnapshot = serializer.snapshotConfiguration();

        byte[] serializedConfig;
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                    new DataOutputViewStreamWrapper(out), configSnapshot, serializer);
            serializedConfig = out.toByteArray();
        }

        TypeSerializerSnapshot<T> restoredConfig;
        try (ByteArrayInputStream in = new ByteArrayInputStream(serializedConfig)) {
            restoredConfig =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader(),
                            serializerGetter.getSerializer());
        }

        TypeSerializerSchemaCompatibility<T> strategy =
                restoredConfig.resolveSchemaCompatibility(serializerGetter.getSerializer());
        final TypeSerializer<T> restoredSerializer;
        if (strategy.isCompatibleAsIs()) {
            restoredSerializer = restoredConfig.restoreSerializer();
        } else if (strategy.isCompatibleWithReconfiguredSerializer()) {
            restoredSerializer = strategy.getReconfiguredSerializer();
        } else {
            throw new AssertionError("Unable to restore serializer with " + strategy);
        }
        assertEquals(serializer.getClass(), restoredSerializer.getClass());

        return restoredSerializer;
    }

    /** Used for snapshotAndReconfigure method to provide serializers when restoring. */
    public interface SerializerGetter<T> {
        TypeSerializer<T> getSerializer();
    }

    /** A simple POJO. */
    public static class MyObj {
        private int a;
        private int b;

        public MyObj(int a, int b) {
            this.a = a;
            this.b = b;
        }

        int getA() {
            return a;
        }

        int getB() {
            return b;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MyObj && ((MyObj) o).a == a && ((MyObj) o).b == b;
        }
    }

    /** Kryo serializer for POJO. */
    public static class MyObjSerializer extends Serializer<MyObj> implements Serializable {

        private static final long serialVersionUID = 1L;
        private static final int delta = 7;

        @Override
        public void write(Kryo kryo, Output output, MyObj myObj) {
            output.writeInt(myObj.getA() + delta);
            output.writeInt(myObj.getB() + delta);
        }

        @Override
        public MyObj read(Kryo kryo, Input input, Class<MyObj> aClass) {
            int a = input.readInt() - delta;
            int b = input.readInt() - delta;
            return new MyObj(a, b);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MyObjSerializer;
        }
    }
}
