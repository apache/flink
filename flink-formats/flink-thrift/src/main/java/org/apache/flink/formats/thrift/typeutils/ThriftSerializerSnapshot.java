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

package org.apache.flink.formats.thrift.typeutils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.thrift.TBase;
import org.apache.thrift.protocol.TProtocolFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.util.StringUtils.readString;
import static org.apache.flink.util.StringUtils.writeString;

/**
 * An {@code Thrift} specific implementation of a {@link TypeSerializerSnapshot}.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
public class ThriftSerializerSnapshot<T extends TBase> implements TypeSerializerSnapshot<T> {

    private Class<T> tClass;
    private Class<? extends TProtocolFactory> tPClass;

    public ThriftSerializerSnapshot() {
        // this constructor is used when restoring from a checkpoint.
    }

    ThriftSerializerSnapshot(Class<T> tClass, Class<? extends TProtocolFactory> tPClass) {
        this.tClass = tClass;
        this.tPClass = tPClass;
    }

    @Override
    public int getCurrentVersion() {
        return 0;
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        writeString(tClass.getName(), out);
        writeString(tPClass.getName(), out);
    }

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        final String tClassName = readString(in);
        final String tPClassName = readString(in);

        tClass = findClassOrThrow(userCodeClassLoader, tClassName);
        tPClass = findClassOrThrow(userCodeClassLoader, tPClassName);
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return new ThriftSerializer<>(tClass, tPClass);
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (!(newSerializer instanceof ThriftSerializer)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }

        ThriftSerializer thriftSerializer = (ThriftSerializer) newSerializer;
        if (thriftSerializer.tClass == tClass && thriftSerializer.tPClass == tPClass) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private static <T> Class<T> findClassOrThrow(
            ClassLoader userCodeClassLoader, String className) {
        try {
            Class<?> runtimeTarget = Class.forName(className, false, userCodeClassLoader);
            return (Class<T>) runtimeTarget;
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    ""
                            + "Unable to find the class '"
                            + className
                            + "' which is used to deserialize "
                            + "the elements of this serializer. "
                            + "Were the class was moved or renamed?",
                    e);
        }
    }
}
