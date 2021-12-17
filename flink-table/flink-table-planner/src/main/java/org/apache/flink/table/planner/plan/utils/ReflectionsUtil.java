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

package org.apache.flink.table.planner.plan.utils;

import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Set;
import java.util.stream.Collectors;

/** An utility class for reflection operations on classes. */
public class ReflectionsUtil {

    public static <T> Set<Class<? extends T>> scanSubClasses(
            String packageName, Class<T> targetClass) {
        return scanSubClasses(packageName, targetClass, false, false);
    }

    public static <T> Set<Class<? extends T>> scanSubClasses(
            String packageName,
            Class<T> targetClass,
            boolean includingInterface,
            boolean includingAbstractClass) {
        Reflections reflections = new Reflections(packageName);
        return reflections.getSubTypesOf(targetClass).stream()
                .filter(
                        c -> {
                            if (c.isInterface()) {
                                return includingInterface;
                            } else if (Modifier.isAbstract(c.getModifiers())) {
                                return includingAbstractClass;
                            } else {
                                return true;
                            }
                        })
                .collect(Collectors.toSet());
    }

    private ReflectionsUtil() {}
}
