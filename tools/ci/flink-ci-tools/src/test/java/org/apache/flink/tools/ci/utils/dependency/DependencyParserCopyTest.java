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

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests the parsing of {@code dependency:copy}. */
class DependencyParserCopyTest {

    private static Stream<String> getTestDependencyCopy() {
        return Stream.of(
                "[INFO] --- maven-dependency-plugin:3.2.0:copy (copy) @ m1 ---",
                "[INFO] Configured Artifact: external:dependency1:2.1:jar",
                "[INFO] Configured Artifact: external:dependency4:classifier:2.4:jar",
                "[INFO] Copying dependency1-2.1.jar to /some/path/dependency1-2.1.jar",
                "[INFO] Copying dependency4-2.4.jar to /some/path/dependency4-2.4.jar",
                "[INFO]",
                "[INFO] --- maven-dependency-plugin:3.2.0:copy (copy) @ m2 ---",
                "[INFO] Configured Artifact: internal:m1:1.1:jar",
                "[INFO] Copying internal-1.1.jar to /some/path/m1-1.1.jar");
    }

    @Test
    void testCopyParsing() {
        final Map<String, Set<Dependency>> dependenciesByModule =
                DependencyParser.parseDependencyCopyOutput(getTestDependencyCopy());

        assertThat(dependenciesByModule).containsOnlyKeys("m1", "m2");
        assertThat(dependenciesByModule.get("m1"))
                .containsExactlyInAnyOrder(
                        Dependency.create("external", "dependency1", "2.1", null),
                        Dependency.create("external", "dependency4", "2.4", "classifier"));
        assertThat(dependenciesByModule.get("m2"))
                .containsExactlyInAnyOrder(Dependency.create("internal", "m1", "1.1", null));
    }

    @Test
    void testCopyLineParsingGroupId() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:jar"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getGroupId()).isEqualTo("external"));
    }

    @Test
    void testCopyLineParsingArtifactId() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:jar"))
                .hasValueSatisfying(
                        dependency ->
                                assertThat(dependency.getArtifactId()).isEqualTo("dependency1"));
    }

    @Test
    void testCopyLineParsingVersion() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:jar"))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getVersion()).isEqualTo("1.0"));
    }

    @Test
    void testCopyLineParsingScope() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:jar"))
                .hasValueSatisfying(dependency -> assertThat(dependency.getScope()).isEmpty());
    }

    @Test
    void testCopyLineParsingOptional() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:jar"))
                .hasValueSatisfying(dependency -> assertThat(dependency.isOptional()).isEmpty());
    }

    @Test
    void testCopyLineParsingWithNonJarType() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:1.0:pom"))
                .hasValue(Dependency.create("external", "dependency1", "1.0", null));
    }

    @Test
    void testCopyLineParsingClassifier() {
        assertThat(
                        DependencyParser.parseCopyDependency(
                                "[INFO] Configured Artifact: external:dependency1:some_classifier:1.0:jar"))
                .hasValueSatisfying(
                        dependency ->
                                assertThat(dependency.getClassifier()).hasValue("some_classifier"));
    }
}
