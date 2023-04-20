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

package org.apache.flink.architecture.common;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaParameterizedType;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.domain.properties.HasName;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.architecture.common.SourcePredicates.isJavaClass;

/** Common conditions for architecture tests. */
public class Conditions {

    /** Generic condition to check fulfillment of a predicate. */
    public static <T extends HasName> ArchCondition<T> fulfill(DescribedPredicate<T> predicate) {
        return new ArchCondition<T>(predicate.getDescription()) {
            @Override
            public void check(T item, ConditionEvents events) {
                if (!predicate.test(item)) {
                    final String message =
                            String.format(
                                    "%s does not satisfy: %s",
                                    item.getName(), predicate.getDescription());
                    events.add(SimpleConditionEvent.violated(item, message));
                }
            }
        };
    }

    /**
     * Tests leaf types of a method against the given predicate.
     *
     * <p>Given some {@link JavaType}, "leaf" types are recursively determined as described below.
     * Leaf types are taken from argument, return, and (declared) exception types.
     *
     * <ul>
     *   <li>If the type is an array type, check its base component type.
     *   <li>If the type is a generic type, check the type itself and all of its type arguments.
     *   <li>Otherwise, check just the type itself.
     * </ul>
     */
    public static ArchCondition<JavaMethod> haveLeafTypes(
            DescribedPredicate<JavaClass> typePredicate) {
        return haveLeafReturnTypes(typePredicate)
                .and(haveLeafArgumentTypes(typePredicate))
                .and(haveLeafExceptionTypes(typePredicate));
    }

    /**
     * Tests leaf return types of a method against the given predicate.
     *
     * <p>See {@link #haveLeafTypes(DescribedPredicate)} for details.
     */
    public static ArchCondition<JavaMethod> haveLeafReturnTypes(
            DescribedPredicate<JavaClass> typePredicate) {
        return new ArchCondition<JavaMethod>(
                "have leaf return types" + typePredicate.getDescription()) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                for (JavaClass leafType : getLeafTypes(method.getReturnType())) {
                    if (!isJavaClass(leafType)) {
                        continue;
                    }

                    if (!typePredicate.test(leafType)) {
                        final String message =
                                String.format(
                                        "%s: Returned leaf type %s does not satisfy: %s",
                                        method.getFullName(),
                                        leafType.getName(),
                                        typePredicate.getDescription());

                        events.add(SimpleConditionEvent.violated(method, message));
                    }
                }
            }
        };
    }

    /**
     * Tests leaf argument types of a method against the given predicate.
     *
     * <p>See {@link #haveLeafTypes(DescribedPredicate)} for details.
     */
    public static ArchCondition<JavaMethod> haveLeafArgumentTypes(
            DescribedPredicate<JavaClass> typePredicate) {
        return new ArchCondition<JavaMethod>(
                "have leaf argument types" + typePredicate.getDescription()) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                final List<JavaClass> leafArgumentTypes =
                        method.getParameterTypes().stream()
                                .flatMap(argumentType -> getLeafTypes(argumentType).stream())
                                .collect(Collectors.toList());

                for (JavaClass leafType : leafArgumentTypes) {
                    if (!isJavaClass(leafType)) {
                        continue;
                    }

                    if (!typePredicate.test(leafType)) {
                        final String message =
                                String.format(
                                        "%s: Argument leaf type %s does not satisfy: %s",
                                        method.getFullName(),
                                        leafType.getName(),
                                        typePredicate.getDescription());

                        events.add(SimpleConditionEvent.violated(method, message));
                    }
                }
            }
        };
    }

    /**
     * Tests leaf exception types of a method against the given predicate.
     *
     * <p>See {@link #haveLeafTypes(DescribedPredicate)} for details.
     */
    public static ArchCondition<JavaMethod> haveLeafExceptionTypes(
            DescribedPredicate<JavaClass> typePredicate) {
        return new ArchCondition<JavaMethod>(
                "have leaf exception types" + typePredicate.getDescription()) {
            @Override
            public void check(JavaMethod method, ConditionEvents events) {
                final List<JavaClass> leafArgumentTypes =
                        method.getExceptionTypes().stream()
                                .flatMap(argumentType -> getLeafTypes(argumentType).stream())
                                .collect(Collectors.toList());

                for (JavaClass leafType : leafArgumentTypes) {
                    if (!isJavaClass(leafType)) {
                        continue;
                    }

                    if (!typePredicate.test(leafType)) {
                        final String message =
                                String.format(
                                        "%s: Exception leaf type %s does not satisfy: %s",
                                        method.getFullName(),
                                        leafType.getName(),
                                        typePredicate.getDescription());

                        events.add(SimpleConditionEvent.violated(method, message));
                    }
                }
            }
        };
    }

    private static List<JavaClass> getLeafTypes(JavaType type) {
        final JavaClass baseComponentType = type.toErasure().getBaseComponentType();
        if (type instanceof JavaParameterizedType) {
            final JavaParameterizedType parameterizedType = (JavaParameterizedType) type;
            return Stream.concat(
                            Stream.of(baseComponentType),
                            parameterizedType.getActualTypeArguments().stream())
                    .flatMap(types -> getLeafTypes(types).stream())
                    .filter(SourcePredicates::isJavaClass)
                    .collect(Collectors.toList());
        }

        return Collections.singletonList(baseComponentType);
    }

    private Conditions() {}
}
