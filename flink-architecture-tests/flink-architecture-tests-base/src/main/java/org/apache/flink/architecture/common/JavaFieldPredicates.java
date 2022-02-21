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
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;

import java.lang.annotation.Annotation;

/** Fine-grained predicates focus on the JavaField. */
public class JavaFieldPredicates {

    /**
     * Match the public modifier of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has the public modifier.
     */
    public static DescribedPredicate<JavaField> isPublic() {
        return DescribedPredicate.describe(
                "public", field -> field.getModifiers().contains(JavaModifier.PUBLIC));
    }

    /**
     * Match the static modifier of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has the static modifier.
     */
    public static DescribedPredicate<JavaField> isStatic() {
        return DescribedPredicate.describe(
                "static", field -> field.getModifiers().contains(JavaModifier.STATIC));
    }

    /**
     * Match none static modifier of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has no static modifier.
     */
    public static DescribedPredicate<JavaField> isNotStatic() {
        return DescribedPredicate.describe(
                "not static", field -> !field.getModifiers().contains(JavaModifier.STATIC));
    }

    /**
     * Match the final modifier of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has the final modifier.
     */
    public static DescribedPredicate<JavaField> isFinal() {
        return DescribedPredicate.describe(
                "final", field -> field.getModifiers().contains(JavaModifier.FINAL));
    }

    /**
     * Match the {@link Class} of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has the same type of the given {@code clazz}.
     */
    public static DescribedPredicate<JavaField> ofType(Class<?> clazz) {
        return DescribedPredicate.describe(
                "of type " + clazz.getSimpleName(),
                field -> field.getRawType().isEquivalentTo(clazz));
    }

    /**
     * Match the {@link Class} of the {@link JavaField}'s assignability.
     *
     * @param clazz the Class type to check for assignability
     * @return a {@link DescribedPredicate} that returns {@code true}, if the respective {@link
     *     JavaField} is assignable to the supplied {@code clazz}.
     */
    public static DescribedPredicate<JavaField> isAssignableTo(Class<?> clazz) {
        return DescribedPredicate.describe(
                "is assignable to " + clazz.getSimpleName(),
                field -> field.getRawType().isAssignableTo(clazz));
    }

    /**
     * Match the single Annotation of the {@link JavaField}.
     *
     * @return A {@link DescribedPredicate} returning true, if and only if the tested {@link
     *     JavaField} has exactly the given Annotation {@code annotationType}.
     */
    public static DescribedPredicate<JavaField> annotatedWith(
            Class<? extends Annotation> annotationType) {
        return DescribedPredicate.describe(
                "annotated with @" + annotationType.getSimpleName(),
                field ->
                        field.getAnnotations().size() == 1
                                && field.getAnnotations()
                                        .iterator()
                                        .next()
                                        .getRawType()
                                        .isEquivalentTo(annotationType));
    }
}
