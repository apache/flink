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

package org.apache.flink.runtime.asyncprocessing.declare;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Stream;

/** A named callback that can be identified and checkpoint. */
public abstract class NamedCallback {

    private final String name;

    private final boolean safeVariables;

    NamedCallback(String name, boolean safeVariables) {
        this.name = name;
        this.safeVariables = safeVariables;
    }

    /** Get the name of this callback. */
    public String getName() {
        return name;
    }

    public boolean isSafeVariables() {
        return safeVariables;
    }

    /**
     * Detect captured variables of a function and determine whether it can be snapshot.
     *
     * @param function the function to detect.
     * @return true if all the captured variables are safe.
     */
    public static boolean detectFunctionVariables(Object function) {
        Class<?> clazz = function.getClass();
        Field[] fields1 = clazz.getFields();
        // returns inherited members but not private members.
        Field[] fields2 = clazz.getDeclaredFields();
        // returns all members including private members but not inherited members.
        return Stream.concat(Stream.of(fields1), Stream.of(fields2))
                .allMatch(NamedCallback::detectSingleField);
    }

    private static boolean detectSingleField(Field field) {
        Class<?> type = field.getType();
        return DeclaredVariable.class.isAssignableFrom(type)
                || (type.isPrimitive() && Modifier.isFinal(field.getModifiers()))
                || Arrays.stream(field.getAnnotations())
                        .anyMatch(e -> e instanceof DeclarationSafe);
    }
}
