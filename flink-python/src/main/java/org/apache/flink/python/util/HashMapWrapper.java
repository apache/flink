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

package org.apache.flink.python.util;

import org.apache.flink.annotation.Internal;

import java.util.HashMap;

/**
 * Helper class to create a {@link HashMap} taking Numeric data as key or value from Python side.
 * This is to avoid unexpected boxing/unboxing happens, see https://github.com/py4j/py4j/issues/374
 * for more details.
 */
@SuppressWarnings("rawtypes,unchecked")
@Internal
public class HashMapWrapper {

    private final Class<?> keyCls;
    private final Class<?> valueCls;
    private final HashMap underlyingMap;

    public HashMapWrapper(Class<?> keyCls, Class<?> valueCls) {
        this.keyCls = keyCls;
        this.valueCls = valueCls;
        this.underlyingMap = new HashMap();
    }

    public void put(Object key, Object value) throws Exception {
        final Object typedKey;
        final Object typedValue;
        if (keyCls != null) {
            typedKey = convert(keyCls, key);
        } else {
            typedKey = key;
        }
        if (valueCls != null) {
            typedValue = convert(valueCls, value);
        } else {
            typedValue = value;
        }
        underlyingMap.put(typedKey, typedValue);
    }

    public HashMap asMap() {
        return underlyingMap;
    }

    private static Object convert(Class<?> cls, Object data) {
        final Number typedData = (Number) data;
        if (cls == Integer.class) {
            return typedData.intValue();
        } else if (cls == Long.class) {
            return typedData.longValue();
        } else if (cls == Float.class) {
            return typedData.floatValue();
        } else if (cls == Double.class) {
            return typedData.doubleValue();
        } else if (cls == Byte.class) {
            return typedData.byteValue();
        } else if (cls == Short.class) {
            return typedData.shortValue();
        } else {
            throw new RuntimeException("Unexpected class type: " + cls);
        }
    }
}
