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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.Source;
import com.tngtech.archunit.core.importer.Location;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

import java.util.Optional;
import java.util.regex.Pattern;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noFields;
import static com.tngtech.archunit.library.freeze.FreezingArchRule.freeze;
import static org.apache.flink.architecture.common.Conditions.fulfill;
import static org.apache.flink.architecture.common.GivenJavaClasses.javaClassesThat;
import static org.apache.flink.architecture.common.Predicates.areDirectlyAnnotatedWithAtLeastOneOf;
import static org.apache.flink.architecture.common.Predicates.arePublicStaticOfType;
import static org.apache.flink.architecture.common.SourcePredicates.areJavaClasses;

/** Rules for Table API. */
public class TableApiRules {

    private static final Pattern TABLE_API_MODULES =
            Pattern.compile(".*/flink-table-(api-(bridge-base|java(|-bridge))|common)/.*");

    @ArchTest
    public static final ArchRule CONFIG_OPTIONS_IN_OPTIONS_CLASSES =
            fields().that(arePublicStaticOfType(ConfigOption.class))
                    .and()
                    .areDeclaredInClassesThat(
                            areJavaClasses().and(resideInAPackage("org.apache.flink.table..")))
                    .should()
                    .beDeclaredInClassesThat()
                    .haveSimpleNameEndingWith("Options")
                    .orShould()
                    .beDeclaredIn(FactoryUtil.class);

    @ArchTest
    public static final ArchRule TABLE_FACTORIES_CONTAIN_NO_CONFIG_OPTIONS =
            noFields()
                    .that(arePublicStaticOfType(ConfigOption.class))
                    .and()
                    .areDeclaredInClassesThat(
                            areJavaClasses().and(resideInAPackage("org.apache.flink.table..")))
                    .should()
                    .beDeclaredInClassesThat()
                    .implement(DynamicTableFactory.class);

    @ArchTest
    public static final ArchRule CONNECTOR_OPTIONS_PACKAGE =
            freeze(
                    javaClassesThat()
                            .haveSimpleNameEndingWith("ConnectorOptions")
                            .or()
                            .haveSimpleNameEndingWith("FormatOptions")
                            .and()
                            .haveSimpleNameNotContaining("Json")
                            .should()
                            .resideInAPackage("org.apache.flink..table")
                            .andShould(
                                    fulfill(
                                            areDirectlyAnnotatedWithAtLeastOneOf(
                                                    PublicEvolving.class, Public.class)))
                            .as(
                                    "Options for connectors and formats should reside in a consistent package and be public API."));

    @ArchTest
    public static final ArchRule ALL_CLASSES_IN_TABLE_API_SHOULD_HAVE_VISIBILITY_ANNOTATIONS =
            javaClassesThat(resideInPublicTableApiModules())
                    .and()
                    .resideInAPackage("org.apache.flink.table..")
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
                            "All public classes residing in the flink-table-api-java, "
                                    + "flink-table-api-java-bridge, flink-table-common or flink-table-api-bridge-base "
                                    + "modules should be explicitly marked with a visibility annotation");

    private static DescribedPredicate<JavaClass> resideInPublicTableApiModules() {
        return new DescribedPredicate<JavaClass>("Reside in public TableApi modules") {
            @Override
            public boolean test(JavaClass input) {
                Optional<Source> sourceOptional = input.getSource();
                if (sourceOptional.isPresent()) {
                    Source source = sourceOptional.get();
                    return Location.of(source.getUri()).matches(TABLE_API_MODULES);
                }
                return false;
            }
        };
    }
}
