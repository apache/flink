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
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * A base class for {@link TypeSerializerConfigSnapshot}s that do not have any parameters.
 *
 * @deprecated this snapshot class is no longer used by any serializers, and is maintained only for
 *     backward compatibility reasons. It is fully replaced by {@link SimpleTypeSerializerSnapshot}.
 */
@Internal
@Deprecated
public final class ParameterlessTypeSerializerConfig<T> extends TypeSerializerConfigSnapshot<T> {

    private static final int VERSION = 1;

    /**
     * A string identifier that encodes the serialization format used by the serializer.
     *
     * <p>TODO we might change this to a proper serialization format class in the future
     */
    private String serializationFormatIdentifier;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public ParameterlessTypeSerializerConfig() {}

    public ParameterlessTypeSerializerConfig(String serializationFormatIdentifier) {
        this.serializationFormatIdentifier =
                Preconditions.checkNotNull(serializationFormatIdentifier);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);
        out.writeUTF(serializationFormatIdentifier);
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);
        serializationFormatIdentifier = in.readUTF();
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (newSerializer instanceof TypeSerializerSingleton) {
            TypeSerializerSingleton<T> singletonSerializer =
                    (TypeSerializerSingleton<T>) newSerializer;
            return isCompatibleSerializationFormatIdentifier(
                            serializationFormatIdentifier, singletonSerializer)
                    ? TypeSerializerSchemaCompatibility.compatibleAsIs()
                    : TypeSerializerSchemaCompatibility.incompatible();
        }

        return super.resolveSchemaCompatibility(newSerializer);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    public String getSerializationFormatIdentifier() {
        return serializationFormatIdentifier;
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        return (other instanceof ParameterlessTypeSerializerConfig)
                && serializationFormatIdentifier.equals(
                        ((ParameterlessTypeSerializerConfig) other)
                                .getSerializationFormatIdentifier());
    }

    @Override
    public int hashCode() {
        return serializationFormatIdentifier.hashCode();
    }

    private static boolean isCompatibleSerializationFormatIdentifier(
            String identifier, TypeSerializerSingleton<?> newSingletonSerializer) {

        String name = newSingletonSerializer.getClass().getName();
        // we also need to check canonical name because some singleton serializers were using that
        // as the identifier
        String canonicalName = newSingletonSerializer.getClass().getCanonicalName();

        return identifier.equals(name) || identifier.equals(canonicalName);
    }
}
