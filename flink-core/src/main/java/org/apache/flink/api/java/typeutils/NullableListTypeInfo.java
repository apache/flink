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
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;

import java.util.List;

/**
 * A {@link TypeInformation} for the list types of the Java API, accepting null collection and null
 * elements.
 *
 * @param <T> The type of the elements in the list.
 */
@PublicEvolving
public class NullableListTypeInfo<T> extends ListTypeInfo<T> {

    public NullableListTypeInfo(Class<T> elementTypeClass) {
        super(elementTypeClass);
    }

    public NullableListTypeInfo(TypeInformation<T> elementTypeInfo) {
        super(elementTypeInfo);
    }

    @Override
    public TypeSerializer<List<T>> createSerializer(SerializerConfig config) {
        // ListSerializer does not support null elements
        TypeSerializer<T> elementTypeSerializer =
                NullableSerializer.wrap(getElementTypeInfo().createSerializer(config), false);
        ListSerializer<T> listSerializer = new ListSerializer<>(elementTypeSerializer);
        return NullableSerializer.wrap(listSerializer, false);
    }

    @Override
    public String toString() {
        return "NullableList<" + getElementTypeInfo() + '>';
    }
}
