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
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.runtime.testutils.InternalMiniClusterExtension;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
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

/** Rules for Integration Tests. */
public class ITCaseRules {

    @ArchTest
    public static final ArchRule INTEGRATION_TEST_ENDING_WITH_ITCASE =
            freeze(
                            javaClassesThat()
                                    .areAssignableTo(AbstractTestBase.class)
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
                        MiniClusterWithClientResource.class, ClassRule.class));
    }

    private static DescribedPredicate<JavaClass> miniClusterWithClientResourceRule() {
        return containAnyFieldsInClassHierarchyThat(
                arePublicFinalOfTypeWithAnnotation(
                        MiniClusterWithClientResource.class, Rule.class));
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
                                                InternalMiniClusterExtension.class,
                                                RegisterExtension.class))),
                outsideFlinkRuntimePackages()
                        .and(
                                containAnyFieldsInClassHierarchyThat(
                                        areStaticFinalOfTypeWithAnnotation(
                                                        MiniClusterExtension.class,
                                                        RegisterExtension.class)
                                                .or(
                                                        areFieldOfType(
                                                                        MiniClusterTestEnvironment
                                                                                .class)
                                                                .and(
                                                                        annotatedWith(
                                                                                TestEnv.class))))),
                inFlinkRuntimePackages()
                        .and(
                                isAnnotatedWithExtendWithUsingExtension(
                                        InternalMiniClusterExtension.class)),
                outsideFlinkRuntimePackages()
                        .and(isAnnotatedWithExtendWithUsingExtension(MiniClusterExtension.class)));
    }

    private static DescribedPredicate<JavaClass> isAnnotatedWithExtendWithUsingExtension(
            Class<? extends Extension> extensionClass) {
        return DescribedPredicate.describe(
                "is annotated with @ExtendWith with class " + extensionClass.getSimpleName(),
                clazz ->
                        clazz.isAnnotatedWith(ExtendWith.class)
                                && Arrays.asList(
                                                clazz.getAnnotationOfType(ExtendWith.class).value())
                                        .contains(extensionClass));
    }
}
