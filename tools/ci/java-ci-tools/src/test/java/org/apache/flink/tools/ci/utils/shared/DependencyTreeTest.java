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

package org.apache.flink.tools.ci.utils.shared;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class DependencyTreeTest {

    @Test
    void testGetPathToDirectDependency() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency dependency = Dependency.create("internal", "m1", "1.0");

        dependencyTree.addDirectDependency(dependency);

        assertThat(dependencyTree.getPathTo(dependency)).containsExactly(dependency);
    }

    @Test
    void testGetPathToTransitiveDependency() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency dependency1 = Dependency.create("internal", "m1", "1.0");
        final Dependency dependency2 = Dependency.create("internal", "m2", "1.0");

        dependencyTree.addDirectDependency(dependency1);
        dependencyTree.addTransitiveDependencyTo(dependency2, dependency1);

        assertThat(dependencyTree.getPathTo(dependency2)).containsExactly(dependency1, dependency2);
    }

    @Test
    void testFlatten() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency dependency1 = Dependency.create("internal", "m1", "1.0");
        final Dependency dependency2 = Dependency.create("internal", "m2", "1.0");
        final Dependency dependency3 = Dependency.create("internal", "m3", "1.0");

        dependencyTree.addDirectDependency(dependency1);
        dependencyTree.addTransitiveDependencyTo(dependency2, dependency1);
        dependencyTree.addDirectDependency(dependency3);

        assertThat(dependencyTree.flatten()).containsExactly(dependency1, dependency2, dependency3);
    }

    @Test
    void testUniquenessDirectDependency() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency dependency = Dependency.create("internal", "m1", "1.0");
        final Dependency dependencyCopy = Dependency.create("internal", "m1", "1.0");

        dependencyTree.addDirectDependency(dependency);
        dependencyTree.addDirectDependency(dependencyCopy);

        assertThat(dependencyTree.flatten()).hasSize(1);
    }

    @Test
    void testUniquenessTransitiveDependency() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency directDependency1 = Dependency.create("internal", "m1", "1.0");
        final Dependency directDependency2 = Dependency.create("internal", "m2", "1.0");
        final Supplier<Dependency> copySupplier = () -> Dependency.create("internal", "m3", "1.0");

        dependencyTree.addDirectDependency(directDependency1);
        dependencyTree.addDirectDependency(directDependency2);
        dependencyTree.addTransitiveDependencyTo(copySupplier.get(), directDependency1);
        dependencyTree.addTransitiveDependencyTo(copySupplier.get(), directDependency2);

        assertThat(dependencyTree.flatten()).hasSize(3);
        assertThat(dependencyTree.getPathTo(copySupplier.get()))
                .containsExactly(directDependency1, copySupplier.get());
    }

    @Test
    void testLookupIgnoresOptionalFlagAndScope() {
        final DependencyTree dependencyTree = new DependencyTree();

        final Dependency slimDependency = Dependency.create("internal", "m1", "1.0");
        final Dependency extendedDependency =
                Dependency.create(
                        slimDependency.getGroupId(),
                        slimDependency.getArtifactId(),
                        slimDependency.getVersion(),
                        "compile",
                        false);

        dependencyTree.addDirectDependency(extendedDependency);

        assertThat(dependencyTree.getPathTo(slimDependency)).containsExactly(extendedDependency);
    }
}
