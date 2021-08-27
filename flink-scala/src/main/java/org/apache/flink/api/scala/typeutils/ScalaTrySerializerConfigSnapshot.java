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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;

import scala.util.Try;

/**
 * A {@link TypeSerializerConfigSnapshot} for the Scala {@link TrySerializer}.
 *
 * <p>This configuration snapshot class is implemented in Java because Scala does not allow calling
 * different base class constructors from subclasses, while we need that for the default empty
 * constructor.
 */
@Deprecated
public class ScalaTrySerializerConfigSnapshot<E>
        extends CompositeTypeSerializerConfigSnapshot<Try<E>> {

    private static final int VERSION = 1;

    /** This empty nullary constructor is required for deserializing the configuration. */
    public ScalaTrySerializerConfigSnapshot() {}

    public ScalaTrySerializerConfigSnapshot(
            TypeSerializer<E> elementSerializer, TypeSerializer<Throwable> throwableSerializer) {

        super(elementSerializer, throwableSerializer);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public TypeSerializerSchemaCompatibility<Try<E>> resolveSchemaCompatibility(
            TypeSerializer<Try<E>> newSerializer) {

        return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                newSerializer,
                new ScalaTrySerializerSnapshot<>(),
                getNestedSerializersAndConfigs().get(0).f1,
                getNestedSerializersAndConfigs().get(1).f1);
    }
}
