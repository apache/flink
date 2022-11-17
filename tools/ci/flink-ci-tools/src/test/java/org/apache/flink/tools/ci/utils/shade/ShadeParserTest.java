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

package org.apache.flink.tools.ci.utils.shade;

import org.apache.flink.tools.ci.utils.shared.Dependency;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ShadeParserTest {

    private static Stream<String> getTestDependencyCopy() {
        return Stream.of(
                "[INFO] --- maven-shade-plugin:3.2.0:shade (shade-flink) @ m1 ---",
                "[INFO] Including external:dependency1:jar:2.1 in the shaded jar.",
                "[INFO] Excluding external:dependency3:jar:2.3 from the shaded jar.",
                "[INFO] Including external:dependency4:jar:classifier:2.4 in the shaded jar.",
                "[INFO] Replacing original artifact with shaded artifact.",
                "[INFO] Replacing /some/path/m1.jar with /some/path/m1-shaded.jar",
                "[INFO]",
                "[INFO] --- maven-shade-plugin:3.2.0:shade (shade-flink) @ m2 ---",
                "[INFO] Including internal:m1:jar:1.1 in the shaded jar.",
                "[INFO] Replacing /some/path/m2.jar with /some/path/m2-shaded.jar");
    }

    @Test
    void testParsing() {
        final Map<String, Set<Dependency>> dependenciesByModule =
                ShadeParser.parseShadeOutput(getTestDependencyCopy());

        assertThat(dependenciesByModule).containsOnlyKeys("m1", "m2");
        assertThat(dependenciesByModule.get("m1"))
                .containsExactlyInAnyOrder(
                        Dependency.create("external", "dependency1", "2.1", null),
                        Dependency.create("external", "dependency4", "2.4", "classifier"));
        assertThat(dependenciesByModule.get("m2"))
                .containsExactlyInAnyOrder(Dependency.create("internal", "m1", "1.1", null));
    }

    @Test
    void testLineParsingGroupId() {
        assertThat(
                        ShadeParser.parseDependency(
                                "Including external:dependency1:jar:1.0 in the shaded jar."))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getGroupId()).isEqualTo("external"));
    }

    @Test
    void testLineParsingArtifactId() {
        assertThat(
                        ShadeParser.parseDependency(
                                "Including external:dependency1:jar:1.0 in the shaded jar."))
                .hasValueSatisfying(
                        dependency ->
                                assertThat(dependency.getArtifactId()).isEqualTo("dependency1"));
    }

    @Test
    void testLineParsingVersion() {
        assertThat(
                        ShadeParser.parseDependency(
                                "Including external:dependency1:jar:1.0 in the shaded jar."))
                .hasValueSatisfying(
                        dependency -> assertThat(dependency.getVersion()).isEqualTo("1.0"));
    }

    @Test
    void testLineParsingWithNonJarType() {
        assertThat(
                        ShadeParser.parseDependency(
                                "Including external:dependency1:pom:1.0 in the shaded jar."))
                .isPresent();
    }
}
