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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.ExceptionUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/** Reflection utilities. */
@Internal
public class ReflectionUtils {

    public static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    public static Object invoke(
            Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    /**
     * Gets and returns any of the {@code fieldNameCandidates} fields from the given Object {@code
     * object} using reflection.
     */
    public static Object getField(Object object, String... fieldNameCandidates) {
        return getField(object, object.getClass(), fieldNameCandidates);
    }

    /**
     * Gets and returns any of the {@code fieldNameCandidates} fields from the given Object {@code
     * object} using reflection.
     */
    public static Object getField(Object object, Class<?> clazz, String... fieldNameCandidates) {
        Exception finalException = null;
        for (String fieldNameCandidate : fieldNameCandidates) {
            try {
                Field field = clazz.getDeclaredField(fieldNameCandidate);
                field.setAccessible(true);
                return field.get(object);
            } catch (NoSuchFieldException | IllegalAccessException e) {
                finalException = ExceptionUtils.firstOrSuppressed(e, finalException);
            }
        }
        throw new RuntimeException("Incompatible KafkaProducer version", finalException);
    }

    /**
     * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
     * reflection.
     */
    public static void setField(Object object, String fieldName, Object value) {
        setField(object, object.getClass(), fieldName, value);
    }

    private static void setField(Object object, Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }
}
