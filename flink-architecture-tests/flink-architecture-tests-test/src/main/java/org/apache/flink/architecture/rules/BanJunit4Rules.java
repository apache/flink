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

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.lang.conditions.ArchConditions.be;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;

/** Rules for modules already completed the migration of junit5. */
public class BanJunit4Rules {

    private static final DescribedPredicate<JavaClass> JUNIT4_CLASSES =
            resideInAnyPackage("junit..", "org.junit..")
                    .and(not(resideInAnyPackage("org.junit.jupiter..", "org.junit.platform..")))
                    .as("JUnit 4 classes");

    // Catches tests that get JUnit 4 through a base class such as TestLogger without referencing
    // JUnit 4 themselves.
    private static final DescribedPredicate<JavaClass> SUBCLASS_OF_JUNIT4_CLASS =
            DescribedPredicate.describe(
                    "a subclass of a class that depends on JUnit 4",
                    clazz ->
                            clazz.getAllRawSuperclasses().stream()
                                    .anyMatch(BanJunit4Rules::dependsOnJunit4));

    @ArchTest
    public static final ArchRule NO_NEW_ADDED_JUNIT4_TEST_RULE =
            freeze(
                    noClasses()
                            .that()
                            .resideInAPackage("org.apache.flink..")
                            .should()
                            .dependOnClassesThat(JUNIT4_CLASSES)
                            .orShould(be(SUBCLASS_OF_JUNIT4_CLASS))
                            .as("Junit4 is forbidden, please use Junit5 instead"));

    private static boolean dependsOnJunit4(JavaClass clazz) {
        return clazz.getDirectDependenciesFromSelf().stream()
                .anyMatch(dependency -> JUNIT4_CLASSES.test(dependency.getTargetClass()));
    }
}
