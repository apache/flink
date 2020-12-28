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
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Either;

import java.util.List;

/** Deprecated config snapshot retained for savepoint compatibility with Flink 1.6 and earlier. */
@Deprecated
@Internal
public final class EitherSerializerConfigSnapshot<L, R>
        extends CompositeTypeSerializerConfigSnapshot<Either<L, R>> {

    private static final int VERSION = 1;

    /** This empty nullary constructor is required for deserializing the configuration. */
    @SuppressWarnings("unused")
    public EitherSerializerConfigSnapshot() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TypeSerializerSchemaCompatibility<Either<L, R>> resolveSchemaCompatibility(
            TypeSerializer<Either<L, R>> newSerializer) {

        // this class was shared between the Java Either Serializer and the
        // Scala Either serializer
        if (newSerializer.getClass() == EitherSerializer.class) {
            List<Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>>> nestedSerializersAndConfigs =
                    getNestedSerializersAndConfigs();
            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    newSerializer,
                    new JavaEitherSerializerSnapshot<>(),
                    nestedSerializersAndConfigs.get(0).f1,
                    nestedSerializersAndConfigs.get(1).f1);
        } else {
            // fall back to the backwards compatibility path
            return super.resolveSchemaCompatibility(newSerializer);
        }
    }
}
