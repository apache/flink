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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializerTypeSerializerProxy;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;
import java.util.Objects;

/** Type information of {@link CompactorRequest}. Unsuitable for state. */
@Internal
public class CompactorRequestTypeInfo extends TypeInformation<CompactorRequest> {

    private final SerializableSupplierWithException<
                    SimpleVersionedSerializer<FileSinkCommittable>, IOException>
            committableSerializerSupplier;

    public CompactorRequestTypeInfo(
            SerializableSupplierWithException<
                            SimpleVersionedSerializer<FileSinkCommittable>, IOException>
                    committableSerializerSupplier) {
        this.committableSerializerSupplier = committableSerializerSupplier;
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
    public Class<CompactorRequest> getTypeClass() {
        return CompactorRequest.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<CompactorRequest> createSerializer(ExecutionConfig config) {
        return new SimpleVersionedSerializerTypeSerializerProxy<>(
                () -> new CompactorRequestSerializer(createCommittableSerializer()));
    }

    @Override
    public String toString() {
        return "CompactorRequestTypeInfo{" + "serializer=" + createCommittableSerializer() + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !canEqual(o)) {
            return false;
        }
        CompactorRequestTypeInfo that = (CompactorRequestTypeInfo) o;
        return Objects.equals(
                createCommittableSerializer().getClass(),
                that.createCommittableSerializer().getClass());
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof CompactorRequestTypeInfo;
    }

    @Override
    public int hashCode() {
        return Objects.hash(createCommittableSerializer().getClass());
    }

    private SimpleVersionedSerializer<FileSinkCommittable> createCommittableSerializer() {
        try {
            return committableSerializerSupplier.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
