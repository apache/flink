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
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;

import java.util.Map;

/**
 * A {@link TypeInformation} for the map types of the Java API, accepting null collection and null
 * key/values.
 *
 * @param <K> The type of the keys in the map.
 * @param <V> The type of the values in the map.
 */
@PublicEvolving
public class NullableMapTypeInfo<K, V> extends MapTypeInfo<K, V> {

    public NullableMapTypeInfo(TypeInformation<K> keyTypeInfo, TypeInformation<V> valueTypeInfo) {
        super(keyTypeInfo, valueTypeInfo);
    }

    public NullableMapTypeInfo(Class<K> keyClass, Class<V> valueClass) {
        super(keyClass, valueClass);
    }

    @Override
    public TypeSerializer<Map<K, V>> createSerializer(SerializerConfig config) {
        // MapSerializer does not support null key
        TypeSerializer<K> keyTypeSerializer =
                NullableSerializer.wrap(getKeyTypeInfo().createSerializer(config), false);
        TypeSerializer<V> valueTypeSerializer = getValueTypeInfo().createSerializer(config);
        MapSerializer<K, V> mapSerializer =
                new MapSerializer<>(keyTypeSerializer, valueTypeSerializer);
        return NullableSerializer.wrap(mapSerializer, false);
    }

    @Override
    public String toString() {
        return "NullableMap<" + getKeyTypeInfo() + ", " + getValueTypeInfo() + ">";
    }
}
