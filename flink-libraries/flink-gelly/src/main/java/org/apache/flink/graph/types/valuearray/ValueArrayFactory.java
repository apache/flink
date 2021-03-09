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

package org.apache.flink.graph.types.valuearray;

import org.apache.flink.types.ByteValue;
import org.apache.flink.types.CharValue;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.FloatValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.types.ShortValue;
import org.apache.flink.types.StringValue;
import org.apache.flink.types.Value;

/**
 * A factory generator for {@link ValueArray} types is necessary because the contained {@link Value}
 * types do not currently implement a common interface for creating a {@link ValueArray}. Algorithms
 * must instantiate classes at runtime when the type information has been erased.
 *
 * <p>This mirrors creating {@link Value} using {@link CopyableValue#copy()}.
 */
public class ValueArrayFactory {

    /**
     * Produce a {@code ValueArray} for the given {@code Value} type.
     *
     * @param cls {@code Value} class
     * @return {@code ValueArray} for given {@code Value} class
     */
    @SuppressWarnings("unchecked")
    public static <T> ValueArray<T> createValueArray(Class<? extends Value> cls) {
        if (ByteValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new ByteValueArray();
        } else if (CharValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new CharValueArray();
        } else if (DoubleValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new DoubleValueArray();
        } else if (FloatValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new FloatValueArray();
        } else if (IntValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new IntValueArray();
        } else if (LongValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new LongValueArray();
        } else if (NullValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new NullValueArray();
        } else if (ShortValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new ShortValueArray();
        } else if (StringValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new StringValueArray();
        } else {
            throw new IllegalArgumentException(
                    "Unable to create unbounded ValueArray for type " + cls);
        }
    }

    /**
     * Produce a {@code ValueArray} for the given {@code Value} type with the given bounded size.
     *
     * @param cls {@code Value} class
     * @param bytes limit the array to the given number of bytes
     * @return {@code ValueArray} for given {@code Value} class
     */
    @SuppressWarnings("unchecked")
    public static <T> ValueArray<T> createValueArray(Class<? extends Value> cls, int bytes) {
        if (ByteValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new ByteValueArray(bytes);
        } else if (CharValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new CharValueArray(bytes);
        } else if (DoubleValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new DoubleValueArray(bytes);
        } else if (FloatValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new FloatValueArray(bytes);
        } else if (IntValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new IntValueArray(bytes);
        } else if (LongValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new LongValueArray(bytes);
        } else if (NullValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new NullValueArray(bytes);
        } else if (ShortValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new ShortValueArray(bytes);
        } else if (StringValue.class.isAssignableFrom(cls)) {
            return (ValueArray<T>) new StringValueArray(bytes);
        } else {
            throw new IllegalArgumentException(
                    "Unable to create bounded ValueArray for type " + cls);
        }
    }
}
