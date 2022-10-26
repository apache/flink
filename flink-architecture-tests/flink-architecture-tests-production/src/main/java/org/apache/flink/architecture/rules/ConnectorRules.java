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

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.thirdparty.com.google.common.base.Joiner;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage;
import static com.tngtech.archunit.core.domain.JavaModifier.PUBLIC;
import static com.tngtech.archunit.core.domain.properties.HasModifiers.Predicates.modifier;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;
import static org.apache.flink.architecture.common.GivenJavaClasses.noJavaClassesThat;
import static org.apache.flink.architecture.common.Predicates.areDirectlyAnnotatedWithAtLeastOneOf;

/** Rules for Flink connectors. */
public class ConnectorRules {
    private static final String[] CONNECTOR_PACKAGES = {
        "org.apache.flink.connector..", "org.apache.flink.streaming.connectors.."
    };

    private static DescribedPredicate<JavaClass> areNotPublicAndResideOutsideOfPackages(
            String... packageIdentifiers) {
        return JavaClass.Predicates.resideOutsideOfPackages(packageIdentifiers)
                .and(
                        not(areDirectlyAnnotatedWithAtLeastOneOf(
                                        Public.class, PublicEvolving.class))
                                .and(not(modifier(PUBLIC))))
                .as(
                        "are not public and reside outside of packages ['%s']",
                        Joiner.on("', '").join(packageIdentifiers));
    }

    @ArchTest
    public static final ArchRule CONNECTOR_CLASSES_ONLY_DEPEND_ON_PUBLIC_API =
            freeze(
                    noJavaClassesThat(resideInAnyPackage(CONNECTOR_PACKAGES))
                            .and()
                            .areNotAnnotatedWith(Deprecated.class)
                            .should()
                            .dependOnClassesThat(
                                    areNotPublicAndResideOutsideOfPackages(CONNECTOR_PACKAGES))
                            .as(
                                    "Connector production code must not depend on non-public API outside of connector packages"));
}
