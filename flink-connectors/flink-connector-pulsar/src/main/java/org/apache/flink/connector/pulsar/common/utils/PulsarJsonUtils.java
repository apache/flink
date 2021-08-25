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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.type.MapType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** json util for pulsar configuration. */
@Internal
public final class PulsarJsonUtils {

    /**
     * Pulsar client shade a jackson and use jackson annotation in its config class. We have to use
     * this shaded jackson here.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    private PulsarJsonUtils() {
        // No need to create instance.
    }

    public static <K, V> Map<K, V> toMap(Class<K> kClass, Class<V> vClass, String json) {
        return toMap(HashMap.class, kClass, vClass, json);
    }

    public static <K, V> Map<K, V> toMap(
            Class<? extends Map> mapClass, Class<K> kClass, Class<V> vClass, String json) {
        MapType mapType = mapper.getTypeFactory().constructMapType(mapClass, kClass, vClass);
        try {
            return mapper.readValue(json, mapType);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> Set<T> toSet(Class<T> clazz, String json) {
        return toSet(TreeSet.class, clazz, json);
    }

    public static <T> Set<T> toSet(Class<? extends Set> setClass, Class<T> clazz, String json) {
        CollectionType collectionType =
                mapper.getTypeFactory().constructCollectionType(setClass, clazz);
        try {
            return mapper.readValue(json, collectionType);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static <T> List<T> toList(Class<T> clazz, String json) {
        return toList(ArrayList.class, clazz, json);
    }

    public static <T> List<T> toList(Class<? extends List> listClass, Class<T> clazz, String json) {
        CollectionType collectionType =
                mapper.getTypeFactory().constructCollectionType(listClass, clazz);
        try {
            return mapper.readValue(json, collectionType);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static String toString(Object o) {
        try {
            return mapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
