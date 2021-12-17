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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/** Snapshot of a tuple serializer's configuration. */
@Internal
public final class TupleSerializerConfigSnapshot<T>
        extends CompositeTypeSerializerConfigSnapshot<T> {

    private static final int VERSION = 1;

    private Class<T> tupleClass;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public TupleSerializerConfigSnapshot() {}

    public TupleSerializerConfigSnapshot(
            Class<T> tupleClass, TypeSerializer<?>[] fieldSerializers) {
        super(fieldSerializers);

        this.tupleClass = Preconditions.checkNotNull(tupleClass);
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);

        try (final DataOutputViewStream outViewWrapper = new DataOutputViewStream(out)) {
            InstantiationUtil.serializeObject(outViewWrapper, tupleClass);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        try (final DataInputViewStream inViewWrapper = new DataInputViewStream(in)) {
            tupleClass =
                    InstantiationUtil.deserializeObject(
                            inViewWrapper, getUserCodeClassLoader(), true);
        } catch (ClassNotFoundException e) {
            throw new IOException("Could not find requested tuple class in classpath.", e);
        }
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    public Class<T> getTupleClass() {
        return tupleClass;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj)
                && (obj instanceof TupleSerializerConfigSnapshot)
                && (tupleClass.equals(((TupleSerializerConfigSnapshot) obj).getTupleClass()));
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + tupleClass.hashCode();
    }
}
