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
import com.tngtech.archunit.core.domain.JavaField;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;

import java.lang.annotation.Annotation;
import java.util.Arrays;

/** Common predicates for architecture tests. */
public class Predicates {

    @SafeVarargs
    public static DescribedPredicate<JavaClass> areDirectlyAnnotatedWithAtLeastOneOf(
            Class<? extends Annotation>... annotations) {
        return Arrays.stream(annotations)
                .map(CanBeAnnotated.Predicates::annotatedWith)
                .reduce(DescribedPredicate::or)
                .orElseThrow(IllegalArgumentException::new)
                .forSubtype();
    }

    /** Tests that the given field is {@code public static} and of the given type. */
    public static DescribedPredicate<JavaField> arePublicStaticOfType(Class<?> clazz) {
        return DescribedPredicate.describe(
                "are public, static, and of type " + clazz.getSimpleName(),
                field ->
                        field.getModifiers().contains(JavaModifier.PUBLIC)
                                && field.getModifiers().contains(JavaModifier.STATIC)
                                && field.getRawType().isEquivalentTo(clazz));
    }

    private Predicates() {}
}
