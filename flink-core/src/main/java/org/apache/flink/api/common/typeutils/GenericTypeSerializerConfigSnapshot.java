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
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Configuration snapshot for serializers for generic types.
 *
 * @param <T> The type to be instantiated.
 */
@Internal
public abstract class GenericTypeSerializerConfigSnapshot<T>
        extends TypeSerializerConfigSnapshot<T> {

    private Class<T> typeClass;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public GenericTypeSerializerConfigSnapshot() {}

    public GenericTypeSerializerConfigSnapshot(Class<T> typeClass) {
        this.typeClass = Preconditions.checkNotNull(typeClass);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);

        // write only the classname to avoid Java serialization
        out.writeUTF(typeClass.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        String genericTypeClassname = in.readUTF();
        try {
            typeClass =
                    (Class<T>) Class.forName(genericTypeClassname, true, getUserCodeClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "Could not find the requested class " + genericTypeClassname + " in classpath.",
                    e);
        }
    }

    public Class<T> getTypeClass() {
        return typeClass;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        return (obj.getClass().equals(getClass()))
                && typeClass.equals(((GenericTypeSerializerConfigSnapshot) obj).getTypeClass());
    }

    @Override
    public int hashCode() {
        return typeClass.hashCode();
    }
}
