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

package org.apache.flink.api.common.typeinfo.descriptor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;

import java.util.Map;

/**
 * Implementation of {@link TypeDescriptor} to create {@link
 * org.apache.flink.api.java.typeutils.MapTypeInfo}. Note that this class is initiated via
 * reflection. So, changing its path or constructor will brake tests.
 *
 * @param <K> type for which key {@link TypeInformation} is created.
 * @param <V> type for which value {@link TypeInformation} is created.
 */
@Internal
public class MapTypeDescriptorImpl<K, V> implements TypeDescriptor<Map<K, V>> {

    private final MapTypeInfo<K, V> mapTypeInfo;

    public MapTypeDescriptorImpl(
            TypeDescriptor<K> keyTypeDescriptor, TypeDescriptor<V> valueTypeDescriptor) {
        mapTypeInfo =
                new MapTypeInfo<>(
                        keyTypeDescriptor.getTypeClass(), valueTypeDescriptor.getTypeClass());
    }

    public MapTypeInfo<K, V> getMapTypeInfo() {
        return mapTypeInfo;
    }

    @Override
    public Class<Map<K, V>> getTypeClass() {
        return mapTypeInfo.getTypeClass();
    }

    public Class<K> getKeyTypeClass() {
        return mapTypeInfo.getKeyTypeInfo().getTypeClass();
    }

    public Class<V> getKeyValueClass() {
        return mapTypeInfo.getValueTypeInfo().getTypeClass();
    }

    @Override
    public String toString() {
        return "MapTypeDescriptorImpl" + mapTypeInfo;
    }
}
