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

package org.apache.flink.tools.ci.optional;

import org.apache.flink.tools.ci.utils.shared.Dependency;
import org.apache.flink.tools.ci.utils.shared.DependencyTree;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ShadeOptionalCheckerTest {
    private static final String MODULE = "module";

    @Test
    void testNonBundledDependencyIsIgnored() {
        final Dependency dependency = createMandatoryDependency("a");
        final Set<Dependency> bundled = Collections.emptySet();
        final DependencyTree dependencyTree = new DependencyTree().addDirectDependency(dependency);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    @Test
    void testNonBundledDependencyIsIgnoredEvenIfOthersAreBundled() {
        final Dependency dependencyA = createMandatoryDependency("a");
        final Dependency dependencyB = createMandatoryDependency("B");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addDirectDependency(dependencyB);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).containsExactly(dependencyB);
    }

    @Test
    void testDirectBundledOptionalDependencyIsAccepted() {
        final Dependency dependency = createOptionalDependency("a");
        final Set<Dependency> bundled = Collections.singleton(dependency);
        final DependencyTree dependencyTree = new DependencyTree().addDirectDependency(dependency);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    @Test
    void testDirectBundledDependencyMustBeOptional() {
        final Dependency dependency = createMandatoryDependency("a");
        final Set<Dependency> bundled = Collections.singleton(dependency);
        final DependencyTree dependencyTree = new DependencyTree().addDirectDependency(dependency);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).containsExactly(dependency);
    }

    @Test
    void testTransitiveBundledOptionalDependencyIsAccepted() {
        final Dependency dependencyA = createMandatoryDependency("a");
        final Dependency dependencyB = createOptionalDependency("b");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addTransitiveDependencyTo(dependencyB, dependencyA);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    @Test
    void testTransitiveBundledDependencyMustBeOptional() {
        final Dependency dependencyA = createMandatoryDependency("a");
        final Dependency dependencyB = createMandatoryDependency("b");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addTransitiveDependencyTo(dependencyB, dependencyA);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).containsExactly(dependencyB);
    }

    @Test
    void testTransitiveBundledDependencyMayNotBeOptionalIfParentIsOptional() {
        final Dependency dependencyA = createOptionalDependency("a");
        final Dependency dependencyB = createMandatoryDependency("b");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addTransitiveDependencyTo(dependencyB, dependencyA);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    @Test
    void testTransitiveBundledDependencyMayNotBeOptionalIfParentHasTestScope() {
        final Dependency dependencyA = createTestDependency("a");
        final Dependency dependencyB = createMandatoryDependency("b");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addTransitiveDependencyTo(dependencyB, dependencyA);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    @Test
    void testTransitiveBundledDependencyMayNotBeOptionalIfParentHasProvidedScope() {
        final Dependency dependencyA = createProvidedDependency("a");
        final Dependency dependencyB = createMandatoryDependency("b");
        final Set<Dependency> bundled = Collections.singleton(dependencyB);
        final DependencyTree dependencyTree =
                new DependencyTree()
                        .addDirectDependency(dependencyA)
                        .addTransitiveDependencyTo(dependencyB, dependencyA);

        final Set<Dependency> violations =
                ShadeOptionalChecker.checkOptionalFlags(MODULE, bundled, dependencyTree);

        assertThat(violations).isEmpty();
    }

    private static Dependency createMandatoryDependency(String artifactId) {
        return Dependency.create("groupId", artifactId, "version", null);
    }

    private static Dependency createOptionalDependency(String artifactId) {
        return Dependency.create("groupId", artifactId, "version", null, "compile", true);
    }

    private static Dependency createProvidedDependency(String artifactId) {
        return Dependency.create("groupId", artifactId, "version", null, "provided", false);
    }

    private static Dependency createTestDependency(String artifactId) {
        return Dependency.create("groupId", artifactId, "version", null, "test", false);
    }
}
