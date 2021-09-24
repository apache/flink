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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.Message;
import org.apache.pulsar.client.api.Schema;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.haveProtobuf;
import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.isProtobufTypeClass;
import static org.apache.flink.util.Preconditions.checkState;

/** Wrap the pulsar {@code Schema} into a flink {@code TypeSerializer}. */
@Internal
public class PulsarSchemaTypeSerializer<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = 7771153330969433085L;

    private final PulsarSchema<T> schema;

    public PulsarSchemaTypeSerializer(PulsarSchema<T> schema) {
        this.schema = schema;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T createInstance() {
        Class<T> recordClass = schema.getRecordClass();

        // No exception wouldn't be thrown here if user don't provide protobuf-java.
        if (haveProtobuf() && isProtobufTypeClass(recordClass)) {
            try {
                Method newBuilderMethod = recordClass.getMethod("newBuilder");
                Message.Builder builder = (Message.Builder) newBuilderMethod.invoke(null);
                return (T) builder.build();
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        } else {
            return InstantiationUtil.instantiate(recordClass);
        }
    }

    @Override
    public T copy(T from) {
        return from;
    }

    @Override
    public T copy(T from, T reuse) {
        return from;
    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        Schema<T> pulsarSchema = schema.getPulsarSchema();
        byte[] bytes = pulsarSchema.encode(record);

        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        int len = source.readInt();
        byte[] bytes = new byte[len];
        int readLen = source.read(bytes);
        checkState(len == readLen);

        Schema<T> pulsarSchema = schema.getPulsarSchema();
        return pulsarSchema.decode(bytes);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        int len = source.readInt();
        byte[] bytes = new byte[len];
        int readLen = source.read(bytes);
        checkState(len == readLen);

        target.writeInt(bytes.length);
        target.write(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PulsarSchemaTypeSerializer) {
            PulsarSchemaTypeSerializer<?> that = (PulsarSchemaTypeSerializer<?>) obj;
            return Objects.equals(schema, that.schema);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return schema.hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new PulsarSchemaTypeSerializerSnapshot<>(schema);
    }

    /**
     * Snapshot for PulsarSchemaTypeSerializer, we only snapshot the SerializablePulsarSchema into
     * the state.
     */
    public static final class PulsarSchemaTypeSerializerSnapshot<T>
            implements TypeSerializerSnapshot<T> {

        private PulsarSchema<T> schema;

        public PulsarSchemaTypeSerializerSnapshot(PulsarSchema<T> schema) {
            this.schema = schema;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            byte[] bytes = InstantiationUtil.serializeObject(schema);
            out.writeInt(bytes.length);
            out.write(bytes);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            int len = in.readInt();
            byte[] bytes = new byte[len];
            int readLen = in.read(bytes);
            checkState(readLen == len);

            try {
                ClassLoader loader = Thread.currentThread().getContextClassLoader();
                this.schema = InstantiationUtil.deserializeObject(bytes, loader);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }

        @Override
        public TypeSerializer<T> restoreSerializer() {
            return new PulsarSchemaTypeSerializer<>(schema);
        }

        @Override
        public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
                TypeSerializer<T> newSerializer) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
    }
}
