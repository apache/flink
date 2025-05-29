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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializerConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.SetSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;

import java.util.Set;

/**
 * A {@link TypeInformation} for the set types of the Java API, accepting null collection and null
 * elements.
 *
 * @param <T> The type of the elements in the set.
 */
@PublicEvolving
public class NullableSetTypeInfo<T> extends SetTypeInfo<T> {

    public NullableSetTypeInfo(Class<T> elementTypeClass) {
        super(elementTypeClass);
    }

    public NullableSetTypeInfo(TypeInformation<T> elementTypeInfo) {
        super(elementTypeInfo);
    }

    @Override
    public TypeSerializer<Set<T>> createSerializer(SerializerConfig config) {
        TypeSerializer<T> elementTypeSerializer = getElementTypeInfo().createSerializer(config);
        SetSerializer<T> setSerializer = new SetSerializer<>(elementTypeSerializer);
        return NullableSerializer.wrap(setSerializer, false);
    }

    @Override
    public String toString() {
        return "NullableSet<" + getElementTypeInfo() + '>';
    }
}
