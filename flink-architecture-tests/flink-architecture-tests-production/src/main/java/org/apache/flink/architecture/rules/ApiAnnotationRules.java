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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideOutsideOfPackage;
import static com.tngtech.archunit.core.domain.properties.CanBeAnnotated.Predicates.annotatedWith;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;
import static org.apache.flink.architecture.common.Conditions.fulfill;
import static org.apache.flink.architecture.common.Conditions.haveLeafTypes;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;
import static org.apache.flink.architecture.common.GivenJavaClasses.noJavaClassesThat;
import static org.apache.flink.architecture.common.Predicates.areDirectlyAnnotatedWithAtLeastOneOf;
import static org.apache.flink.architecture.common.SourcePredicates.areJavaClasses;

/** Rules for API visibility annotations. */
public class ApiAnnotationRules {

    @ArchTest
    public static final ArchRule ANNOTATED_APIS =
            freeze(
                    javaClassesThat()
                            .resideInAPackage("org.apache.flink..api..")
                            .and()
                            .resideOutsideOfPackage("..internal..")
                            .and()
                            .arePublic()
                            .should(
                                    fulfill(
                                            areDirectlyAnnotatedWithAtLeastOneOf(
                                                    Internal.class,
                                                    Experimental.class,
                                                    PublicEvolving.class,
                                                    Public.class,
                                                    Deprecated.class)))
                            .as(
                                    "Classes in API packages should have at least one API visibility annotation."));

    @ArchTest
    public static final ArchRule PUBLIC_API_METHODS_USE_ONLY_PUBLIC_API_TYPES =
            freeze(
                    methods()
                            .that()
                            .areAnnotatedWith(Public.class)
                            .or()
                            .areDeclaredInClassesThat(
                                    areJavaClasses().and(annotatedWith(Public.class)))
                            .and()
                            .arePublic()
                            .and()
                            .areNotAnnotatedWith(PublicEvolving.class)
                            .and()
                            .areNotAnnotatedWith(Internal.class)
                            .and()
                            .areNotAnnotatedWith(Deprecated.class)
                            .and()
                            .areNotAnnotatedWith(Experimental.class)
                            .should(
                                    haveLeafTypes(
                                            resideOutsideOfPackage("org.apache.flink..")
                                                    .or(resideInShadedPackage())
                                                    .or(
                                                            areDirectlyAnnotatedWithAtLeastOneOf(
                                                                    Public.class,
                                                                    Deprecated.class))))
                            .as(
                                    "Return and argument types of methods annotated with @Public must be annotated with @Public."));

    @ArchTest
    public static final ArchRule PUBLIC_EVOLVING_API_METHODS_USE_ONLY_PUBLIC_EVOLVING_API_TYPES =
            freeze(
                    methods()
                            .that()
                            .areAnnotatedWith(PublicEvolving.class)
                            .or()
                            .areDeclaredInClassesThat(
                                    areJavaClasses()
                                            .and(
                                                    areDirectlyAnnotatedWithAtLeastOneOf(
                                                            PublicEvolving.class)))
                            .and()
                            .arePublic()
                            .and()
                            .areNotAnnotatedWith(Internal.class)
                            .and()
                            .areNotAnnotatedWith(Deprecated.class)
                            .and()
                            .areNotAnnotatedWith(Experimental.class)
                            .should(
                                    haveLeafTypes(
                                            resideOutsideOfPackage("org.apache.flink..")
                                                    .or(resideInShadedPackage())
                                                    .or(
                                                            areDirectlyAnnotatedWithAtLeastOneOf(
                                                                    Public.class,
                                                                    PublicEvolving.class,
                                                                    Deprecated.class))))
                            .as(
                                    "Return and argument types of methods annotated with @PublicEvolving must be annotated with @Public(Evolving)."));

    @ArchTest
    public static final ArchRule NO_CALLS_TO_VISIBLE_FOR_TESTING_METHODS =
            freeze(
                    noJavaClassesThat()
                            .resideInAPackage("org.apache.flink..")
                            .should()
                            .callMethodWhere(
                                    new DescribedPredicate<JavaMethodCall>(
                                            "the target is annotated @"
                                                    + VisibleForTesting.class.getSimpleName()) {
                                        @Override
                                        public boolean test(JavaMethodCall call) {
                                            final JavaClass targetOwner = call.getTargetOwner();
                                            final JavaClass originOwner = call.getOriginOwner();

                                            // no violation for caller annotated with
                                            // @VisibleForTesting
                                            if (call.getOrigin()
                                                    .isAnnotatedWith(VisibleForTesting.class)) {
                                                return false;
                                            }

                                            if (originOwner.equals(targetOwner)) {
                                                return false;
                                            }
                                            if (originOwner
                                                    .getEnclosingClass()
                                                    .map(targetOwner::equals)
                                                    .orElse(false)) {
                                                return false;
                                            }
                                            if (targetOwner
                                                    .getEnclosingClass()
                                                    .map(originOwner::equals)
                                                    .orElse(false)) {
                                                return false;
                                            }

                                            return call.getTarget()
                                                    .isAnnotatedWith(VisibleForTesting.class);
                                        }
                                    })
                            .as(
                                    "Production code must not call methods annotated with @VisibleForTesting"));

    private static DescribedPredicate<JavaClass> resideInShadedPackage() {
        return resideInAnyPackage("..shaded..");
    }
}
