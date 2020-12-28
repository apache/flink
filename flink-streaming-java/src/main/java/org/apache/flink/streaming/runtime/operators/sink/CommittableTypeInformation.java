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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplier;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeInformation} for {@link org.apache.flink.api.connector.sink.Sink}'s committable,
 * which uses the {@link SimpleVersionedSerializer} to serialize the object.
 *
 * <p>This should not be used as a general {@link TypeInformation}. It's meant to be used by sink's
 * operators.
 *
 * @param <CommT> The committable type of the {@link Committer}.
 */
@Internal
public final class CommittableTypeInformation<CommT> extends TypeInformation<CommT> {

    private final Class<CommT> typeClazz;

    private final SerializableSupplier<SimpleVersionedSerializer<CommT>>
            serializerSerializableSupplier;

    public CommittableTypeInformation(
            Class<CommT> typeClazz,
            SerializableSupplier<SimpleVersionedSerializer<CommT>> serializerSerializableSupplier) {
        this.typeClazz = checkNotNull(typeClazz);
        this.serializerSerializableSupplier = checkNotNull(serializerSerializableSupplier);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<CommT> getTypeClass() {
        return typeClazz;
    }

    @Override
    public boolean isKeyType() {
        return Comparable.class.isAssignableFrom(typeClazz);
    }

    @Override
    public TypeSerializer<CommT> createSerializer(ExecutionConfig config) {
        return new SimpleVersionedSerializerTypeSerializerProxy<>(serializerSerializableSupplier);
    }

    @Override
    public String toString() {
        return typeClazz.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CommittableTypeInformation) {
            if (this == o) {
                return true;
            }
            CommittableTypeInformation<?> that = (CommittableTypeInformation<?>) o;
            return typeClazz.equals(that.typeClazz)
                    && serializerSerializableSupplier
                            .get()
                            .equals(that.serializerSerializableSupplier.get());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeClazz, serializerSerializableSupplier.get());
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CommittableTypeInformation;
    }
}
