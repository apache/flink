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

package org.apache.flink.tools.ci.utils.shared;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DependencyTreeTest {
    private static final Dependency DEPENDENCY =
            Dependency.create("groupId", "artifactId", "version", null);

    @Test
    void testDependencyKeyIncludesGroupId() {
        testDependencyKeyInclusion(
                Dependency.create(
                        "xxx",
                        DEPENDENCY.getArtifactId(),
                        DEPENDENCY.getVersion(),
                        DEPENDENCY.getClassifier().orElse(null)));
    }

    @Test
    void testDependencyKeyIncludesArtifactId() {
        testDependencyKeyInclusion(
                Dependency.create(
                        DEPENDENCY.getGroupId(),
                        "xxx",
                        DEPENDENCY.getVersion(),
                        DEPENDENCY.getClassifier().orElse(null)));
    }

    @Test
    void testDependencyKeyIncludesVersion() {
        testDependencyKeyInclusion(
                Dependency.create(
                        DEPENDENCY.getGroupId(),
                        DEPENDENCY.getArtifactId(),
                        "xxx",
                        DEPENDENCY.getClassifier().orElse(null)));
    }

    @Test
    void testDependencyKeyIncludesClassifier() {
        testDependencyKeyInclusion(
                Dependency.create(
                        DEPENDENCY.getGroupId(),
                        DEPENDENCY.getArtifactId(),
                        DEPENDENCY.getVersion(),
                        "xxx"));
    }

    private static void testDependencyKeyInclusion(Dependency modifiedDependency) {
        final DependencyTree dependencyTree = new DependencyTree();
        dependencyTree.addDirectDependency(DEPENDENCY);
        dependencyTree.addDirectDependency(modifiedDependency);

        assertThat(dependencyTree.flatten()).containsExactly(DEPENDENCY, modifiedDependency);
    }

    @Test
    void testDependencyKeyIgnoresScopeAndOptionalFlag() {
        final Dependency dependencyWithScopeAndOptionalFlag =
                Dependency.create(
                        DEPENDENCY.getGroupId(),
                        DEPENDENCY.getArtifactId(),
                        DEPENDENCY.getVersion(),
                        DEPENDENCY.getClassifier().orElse(null),
                        "compile",
                        true);

        final DependencyTree dependencyTree = new DependencyTree();
        dependencyTree.addDirectDependency(DEPENDENCY);
        dependencyTree.addDirectDependency(dependencyWithScopeAndOptionalFlag);

        assertThat(dependencyTree.flatten()).containsExactly(DEPENDENCY);
        assertThat(dependencyTree.getPathTo(dependencyWithScopeAndOptionalFlag))
                .containsExactly(DEPENDENCY);
    }
}
