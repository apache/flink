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

import org.apache.flink.architecture.common.Predicates;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;

import static com.tngtech.archunit.core.domain.JavaModifier.ABSTRACT;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;
import static org.apache.flink.architecture.common.Conditions.fulfill;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;
import static org.apache.flink.architecture.common.JavaFieldPredicates.annotatedWith;
import static org.apache.flink.architecture.common.Predicates.areFieldOfType;
import static org.apache.flink.architecture.common.Predicates.arePublicFinalOfTypeWithAnnotation;
import static org.apache.flink.architecture.common.Predicates.arePublicStaticFinalOfTypeWithAnnotation;
import static org.apache.flink.architecture.common.Predicates.areStaticFinalOfTypeWithAnnotation;
import static org.apache.flink.architecture.common.Predicates.containAnyFieldsInClassHierarchyThat;
import static org.apache.flink.architecture.common.Predicates.getClassSimpleNameFromFqName;

/** Rules for Integration Tests. */
public class ITCaseRules {

    private static final String ABSTRACT_TEST_BASE_FQ =
            "org.apache.flink.test.util.AbstractTestBase";
    private static final String INTERNAL_MINI_CLUSTER_EXTENSION_FQ_NAME =
            "org.apache.flink.runtime.testutils.InternalMiniClusterExtension";
    private static final String MINI_CLUSTER_EXTENSION_FQ_NAME =
            "org.apache.flink.test.junit5.MiniClusterExtension";
    private static final String MINI_CLUSTER_TEST_ENVIRONMENT_FQ_NAME =
            "org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment";
    private static final String TEST_ENV_FQ_NAME =
            "org.apache.flink.connector.testframe.junit.annotations.TestEnv";

    @ArchTest
    public static final ArchRule INTEGRATION_TEST_ENDING_WITH_ITCASE =
            freeze(
                            javaClassesThat()
                                    .areAssignableTo(
                                            DescribedPredicate.describe(
                                                    "is assignable to " + ABSTRACT_TEST_BASE_FQ,
                                                    javaClass ->
                                                            javaClass
                                                                    .getName()
                                                                    .equals(ABSTRACT_TEST_BASE_FQ)))
                                    .and()
                                    .doNotHaveModifier(ABSTRACT)
                                    .should()
                                    .haveSimpleNameEndingWith("ITCase"))
                    // FALSE by default since 0.23.0 however not every module has inheritors of
                    // AbstractTestBase
                    .allowEmptyShould(true)
                    .as(
                            "Tests inheriting from AbstractTestBase should have name ending with ITCase");

    /**
     * In order to pass this check, IT cases must fulfill at least one of the following conditions.
     *
     * <p>1. For JUnit 5 test, both fields are required like:
     *
     * <pre>{@code
     * @RegisterExtension
     * public static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
     *         new MiniClusterExtension(
     *                 new MiniClusterResourceConfiguration.Builder()
     *                         .setConfiguration(getFlinkConfiguration())
     *                         .build());
     * }</pre>
     *
     * <p>2. For JUnit 5 test, use {@link ExtendWith}:
     *
     * <pre>{@code
     * @ExtendWith(MiniClusterExtension.class)
     * }</pre>
     *
     * <p>3. For JUnit 4 test via @Rule like:
     *
     * <pre>{@code
     * @Rule
     *  public final MiniClusterWithClientResource miniClusterResource =
     *          new MiniClusterWithClientResource(
     *                  new MiniClusterResourceConfiguration.Builder()
     *                          .setNumberTaskManagers(1)
     *                          .setNumberSlotsPerTaskManager(PARALLELISM)
     *                          .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
     *                          .withHaLeadershipControl()
     *                          .build());
     * }</pre>
     *
     * <p>4. For JUnit 4 test via @ClassRule like:
     *
     * <pre>{@code
     * @ClassRule
     * public static final MiniClusterWithClientResource MINI_CLUSTER =
     *         new MiniClusterWithClientResource(
     *                 new MiniClusterResourceConfiguration.Builder()
     *                         .setConfiguration(new Configuration())
     *                         .build());
     * }</pre>
     */
    @ArchTest
    public static final ArchRule ITCASE_USE_MINICLUSTER =
            freeze(
                            javaClassesThat()
                                    .haveSimpleNameEndingWith("ITCase")
                                    .and()
                                    .areTopLevelClasses()
                                    .and()
                                    .doNotHaveModifier(ABSTRACT)
                                    .should(
                                            fulfill(
                                                    // JUnit 5 violation check
                                                    miniClusterExtensionRule()
                                                            // JUnit 4 violation check, which should
                                                            // be
                                                            // removed
                                                            // after the JUnit 4->5 migration is
                                                            // closed.
                                                            // Please refer to FLINK-25858.
                                                            .or(
                                                                    miniClusterWithClientResourceClassRule())
                                                            .or(
                                                                    miniClusterWithClientResourceRule()))))
                    // FALSE by default since 0.23.0 however not every module has *ITCase tests
                    .allowEmptyShould(true)
                    .as("ITCASE tests should use a MiniCluster resource or extension");

    private static DescribedPredicate<JavaClass> miniClusterWithClientResourceClassRule() {
        return containAnyFieldsInClassHierarchyThat(
                arePublicStaticFinalOfTypeWithAnnotation(
                        "org.apache.flink.test.util.MiniClusterWithClientResource",
                        ClassRule.class));
    }

    private static DescribedPredicate<JavaClass> miniClusterWithClientResourceRule() {
        return containAnyFieldsInClassHierarchyThat(
                arePublicFinalOfTypeWithAnnotation(
                        "org.apache.flink.test.util.MiniClusterWithClientResource", Rule.class));
    }

    private static DescribedPredicate<JavaClass> inFlinkRuntimePackages() {
        return JavaClass.Predicates.resideInAPackage("org.apache.flink.runtime.*");
    }

    private static DescribedPredicate<JavaClass> outsideFlinkRuntimePackages() {
        return JavaClass.Predicates.resideOutsideOfPackage("org.apache.flink.runtime.*");
    }

    private static DescribedPredicate<JavaClass> miniClusterExtensionRule() {
        // Only flink-runtime should use InternalMiniClusterExtension,
        // other packages should use MiniClusterExtension
        return Predicates.exactlyOneOf(
                inFlinkRuntimePackages()
                        .and(
                                containAnyFieldsInClassHierarchyThat(
                                        areStaticFinalOfTypeWithAnnotation(
                                                INTERNAL_MINI_CLUSTER_EXTENSION_FQ_NAME,
                                                RegisterExtension.class))),
                outsideFlinkRuntimePackages()
                        .and(
                                containAnyFieldsInClassHierarchyThat(
                                        areStaticFinalOfTypeWithAnnotation(
                                                        MINI_CLUSTER_EXTENSION_FQ_NAME,
                                                        RegisterExtension.class)
                                                .or(
                                                        areFieldOfType(
                                                                        MINI_CLUSTER_TEST_ENVIRONMENT_FQ_NAME)
                                                                .and(
                                                                        annotatedWith(
                                                                                TEST_ENV_FQ_NAME))))),
                inFlinkRuntimePackages()
                        .and(
                                isAnnotatedWithExtendWithUsingExtension(
                                        INTERNAL_MINI_CLUSTER_EXTENSION_FQ_NAME)),
                outsideFlinkRuntimePackages()
                        .and(
                                isAnnotatedWithExtendWithUsingExtension(
                                        MINI_CLUSTER_EXTENSION_FQ_NAME)));
    }

    private static DescribedPredicate<JavaClass> isAnnotatedWithExtendWithUsingExtension(
            String extensionClassFqName) {
        return DescribedPredicate.describe(
                "is annotated with @ExtendWith with class "
                        + getClassSimpleNameFromFqName(extensionClassFqName),
                clazz ->
                        clazz.isAnnotatedWith(ExtendWith.class)
                                && Arrays.stream(
                                                clazz.getAnnotationOfType(ExtendWith.class).value())
                                        .map(Class::getCanonicalName)
                                        .anyMatch(s -> s.equals(extensionClassFqName)));
    }
}
