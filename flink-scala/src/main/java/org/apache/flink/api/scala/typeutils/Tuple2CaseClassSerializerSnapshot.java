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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.scala.package$;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Objects;

import scala.Tuple2;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** {@link TypeSerializerSnapshot} for {@link ScalaCaseClassSerializer}. */
@Internal
public final class Tuple2CaseClassSerializerSnapshot<T1, T2>
        extends CompositeTypeSerializerSnapshot<
                scala.Tuple2<T1, T2>, ScalaCaseClassSerializer<scala.Tuple2<T1, T2>>> {

    private static final int VERSION = 2;

    private Class<Tuple2<T1, T2>> type;

    @SuppressWarnings("unused")
    public Tuple2CaseClassSerializerSnapshot() {
        super(correspondingSerializerClass());
    }

    public Tuple2CaseClassSerializerSnapshot(
            ScalaCaseClassSerializer<Tuple2<T1, T2>> serializerInstance) {
        super(serializerInstance);
        this.type = checkNotNull(serializerInstance.getTupleClass(), "tuple class can not be NULL");
    }

    @Override
    protected int getCurrentOuterSnapshotVersion() {
        return VERSION;
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(
            ScalaCaseClassSerializer<Tuple2<T1, T2>> outerSerializer) {
        return outerSerializer.getFieldSerializers();
    }

    @Override
    protected ScalaCaseClassSerializer<Tuple2<T1, T2>> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {

        checkState(type != null, "tuple class can not be NULL");
        return package$.MODULE$.tuple2Serializer(type, nestedSerializers);
    }

    @Override
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {
        checkState(type != null, "tuple class can not be NULL");
        out.writeUTF(type.getName());
    }

    @Override
    protected void readOuterSnapshot(
            int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        this.type = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
    }

    @Override
    protected CompositeTypeSerializerSnapshot.OuterSchemaCompatibility
            resolveOuterSchemaCompatibility(
                    ScalaCaseClassSerializer<Tuple2<T1, T2>> newSerializer) {
        return (Objects.equals(type, newSerializer.getTupleClass()))
                ? OuterSchemaCompatibility.COMPATIBLE_AS_IS
                : OuterSchemaCompatibility.INCOMPATIBLE;
    }

    private static <T1, T2>
            Class<ScalaCaseClassSerializer<scala.Tuple2<T1, T2>>> correspondingSerializerClass() {
        return package$.MODULE$.tuple2ClassForJava();
    }
}
