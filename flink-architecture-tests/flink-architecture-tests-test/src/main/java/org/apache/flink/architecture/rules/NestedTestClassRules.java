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

package org.apache.flink.architecture.rules;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Nested;

import java.util.List;

import static com.tngtech.archunit.core.domain.JavaModifier.ABSTRACT;
import static com.tngtech.archunit.core.domain.JavaModifier.PRIVATE;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;

/** Rules catching test classes that surefire and JUnit both silently skip when nested. */
public class NestedTestClassRules {

    private static final List<String> TEST_METHOD_ANNOTATIONS =
            List.of(
                    "org.junit.jupiter.api.Test",
                    "org.junit.jupiter.api.TestTemplate",
                    "org.junit.jupiter.api.RepeatedTest",
                    "org.junit.jupiter.api.TestFactory",
                    "org.junit.jupiter.params.ParameterizedTest");

    // includes inherited methods, e.g. a variant that only extends a base
    private static final DescribedPredicate<JavaClass> ARE_EXECUTABLE_TEST_CLASSES =
            DescribedPredicate.describe(
                    "are executable JUnit test classes",
                    clazz ->
                            clazz.getAllMethods().stream()
                                    .anyMatch(
                                            method ->
                                                    TEST_METHOD_ANNOTATIONS.stream()
                                                            .anyMatch(method::isAnnotatedWith)));

    // excludes manually-driven helpers (e.g. a SerializerTestInstance built with real
    // constructor args) that can never become @Nested regardless of static/inner shape
    private static final DescribedPredicate<JavaClass> HAS_NO_ARG_CONSTRUCTOR =
            DescribedPredicate.describe(
                    "declare a no-arg constructor",
                    clazz ->
                            clazz.getConstructors().stream()
                                    .anyMatch(
                                            constructor ->
                                                    constructor.getRawParameterTypes().isEmpty()));

    @ArchTest
    public static final ArchRule STATIC_NESTED_TEST_CLASSES_SHOULD_BE_INNER_CLASSES =
            javaClassesThat()
                    .areMemberClasses()
                    .and()
                    .doNotHaveModifier(ABSTRACT)
                    .and(ARE_EXECUTABLE_TEST_CLASSES)
                    .and(HAS_NO_ARG_CONSTRUCTOR)
                    .should()
                    .beInnerClasses()
                    // not every module has nested test classes
                    .allowEmptyShould(true)
                    .as(
                            "A concrete nested test class must be a non-static inner class "
                                    + "(annotated with @Nested), or surefire and JUnit both "
                                    + "silently skip it. Abstract static bases are unaffected.");

    @ArchTest
    public static final ArchRule NESTED_TEST_CLASSES_SHOULD_NOT_BE_PRIVATE =
            javaClassesThat()
                    .areAnnotatedWith(Nested.class)
                    .should()
                    .notHaveModifier(PRIVATE)
                    // not every module has @Nested test classes
                    .allowEmptyShould(true)
                    .as("A @Nested test class must not be private; JUnit cannot instantiate it");
}
