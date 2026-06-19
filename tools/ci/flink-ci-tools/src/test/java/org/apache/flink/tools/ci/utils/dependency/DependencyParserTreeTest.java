/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.tools.ci.utils.dependency;

import org.apache.flink.tools.ci.utils.shared.Dependency;
import org.apache.flink.tools.ci.utils.shared.DependencyTree;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the parsing of {@code dependency:tree}. */
class DependencyParserTreeTest {

    private static Stream<String> getTestDependencyTree() {
        return Stream.of(
                "[INFO] --- maven-dependency-plugin:3.2.0:tree (default-cli) @ m1 ---",
                "[INFO] internal:m1:jar:1.1",
                "[INFO] +- external:dependency1:jar:2.1:compile",
                "[INFO] |  +- external:dependency2:jar:2.2:compile (optional)",
                "[INFO] |  |  \\- external:dependency3:jar:2.3:provided",
                "[INFO] |  +- external:dependency4:jar:classifier:2.4:compile",
                "[INFO]",
                "[INFO] --- maven-dependency-plugin:3.2.0:tree (default-cli) @ m2 ---",
                "[INFO] internal:m2:jar:1.2",
                "[INFO] +- internal:m1:jar:1.1:compile",
                "[INFO] |  +- external:dependency4:jar:2.4:compile");
    }

    @Test
    void testTreeParsing() {
        final Map<String, DependencyTree> dependenciesByModule =
                DependencyParser.parseDependencyTreeOutput(getTestDependencyTree());

        assertThat(dependenciesByModule).containsOnlyKeys("m1", "m2");
        assertThat(dependenciesByModule.get("m1").flatten())
                .containsExactlyInAnyOrder(
                        Dependency.create("external", "dependency1", "2.1", null, "compile", false),
                        Dependency.create("external", "dependency2", "2.2", null, "compile", true),
                        Dependency.create(
                                "external", "dependency3", "2.3", null, "provided", false),
                        Dependency.create(
                                "external", "dependency4", "2.4", "classifier", "compile", false));
        assertThat(dependenciesByModule.get("m2").flatten())
                .containsExactlyInAnyOrder(
                        Dependency.create("internal", "m1", "1.1", null, "compile", false),
                        Dependency.create(
                                "external", "dependency4", "2.4", null, "compile", false));
    }

    @Test
    void testTreeLineParsingGroupId() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:compile"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getGroupId()).isEqualTo("external"));
    }

    @Test
    void testTreeLineParsingArtifactId() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:compile"))
                .hasValueSatisfying(
                        dependency ->
                                assertThat(dependency.getArtifactId()).isEqualTo("dependency1"));
    }

    @Test
    void testTreeLineParsingVersion() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:compile"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getVersion()).isEqualTo("1.0"));
    }

    @Test
    void testTreeLineParsingScope() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:provided"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getScope()).hasValue("provided"));
    }

    @Test
    void testTreeLineParsingWithNonJarType() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:pom:1.0:compile"))
                .hasValue(
                        Dependency.create(
                                "external", "dependency1", "1.0", null, "compile", false));
    }

    @Test
    void testTreeLineParsingWithClassifier() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:some_classifier:1.0:compile"))
                .hasValue(
                        Dependency.create(
                                "external",
                                "dependency1",
                                "1.0",
                                "some_classifier",
                                "compile",
                                false));
    }

    @Test
    void testTreeLineParsingWithoutOptional() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:compile"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.isOptional()).hasValue(false));
    }

    @Test
    void testTreeLineParsingWithOptional() {
        assertThat(
                        DependencyParser.parseTreeDependency(
                                "[INFO] +- external:dependency1:jar:1.0:compile (optional)"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.isOptional()).hasValue(true));
    }
}
