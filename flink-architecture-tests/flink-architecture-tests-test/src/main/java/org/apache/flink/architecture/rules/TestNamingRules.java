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

import java.util.Arrays;
import java.util.List;

import static com.tngtech.archunit.core.domain.JavaModifier.ABSTRACT;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;

/**
 * Rules ensuring executable test classes are named so the build actually runs them.
 *
 * <p>Surefire only runs the unit include pattern {@code **}{@code /*Test.*} in the {@code test}
 * phase; integration tests follow the {@code *ITCase} convention. A concrete class that carries (or
 * inherits) JUnit test methods but is named otherwise (e.g. {@code *Tests}) is silently skipped by
 * the unit run. This rule flags such classes so they are renamed to {@code *Test} or {@code
 * *ITCase}.
 */
public class TestNamingRules {

    /** JUnit 5 and (for modules still mid-migration) JUnit 4 test method annotations. */
    private static final List<String> TEST_METHOD_ANNOTATIONS =
            Arrays.asList(
                    "org.junit.jupiter.api.Test",
                    "org.junit.jupiter.api.TestTemplate",
                    "org.junit.jupiter.api.RepeatedTest",
                    "org.junit.jupiter.api.TestFactory",
                    "org.junit.jupiter.params.ParameterizedTest",
                    "org.junit.Test");

    /**
     * A class JUnit would execute: it declares or inherits a test method. {@code getAllMethods()}
     * covers inherited {@code @TestTemplate} methods, e.g. semantic-test suites that only extend a
     * base and add no annotation themselves.
     */
    private static final DescribedPredicate<JavaClass> ARE_EXECUTABLE_TEST_CLASSES =
            DescribedPredicate.describe(
                    "are executable JUnit test classes",
                    clazz ->
                            clazz.getAllMethods().stream()
                                    .anyMatch(
                                            method ->
                                                    TEST_METHOD_ANNOTATIONS.stream()
                                                            .anyMatch(method::isAnnotatedWith)));

    @ArchTest
    public static final ArchRule TEST_CLASSES_SHOULD_BE_NAMED_TEST_OR_ITCASE =
            javaClassesThat()
                    .areTopLevelClasses()
                    .and()
                    .doNotHaveModifier(ABSTRACT)
                    .and(ARE_EXECUTABLE_TEST_CLASSES)
                    .should()
                    .haveSimpleNameEndingWith("Test")
                    .orShould()
                    .haveSimpleNameEndingWith("Tests")
                    .orShould()
                    .haveSimpleNameEndingWith("ITCase")
                    // not every module has such classes
                    .allowEmptyShould(true)
                    .as(
                            "Executable test classes must be named *Test[s] or *ITCase so the surefire "
                                    + "include pattern runs them");
}
