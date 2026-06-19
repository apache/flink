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

package org.apache.flink.tools.ci.optional;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.dependency.DependencyParser;
import org.apache.flink.tools.ci.utils.shade.ShadeParser;
import org.apache.flink.tools.ci.utils.shared.Dependency;
import org.apache.flink.tools.ci.utils.shared.DependencyTree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Verifies that all dependencies bundled with the shade-plugin are marked as optional in the pom.
 * This ensures compatibility with later maven versions and in general simplifies dependency
 * management as transitivity is no longer dependent on the shade-plugin.
 *
 * <p>In Maven 3.3 the dependency tree was made immutable at runtime, and thus can no longer be
 * changed by the shade plugin. The plugin would usually remove a dependency from the tree when it
 * is being bundled (known as dependency reduction). While dependency reduction still works for the
 * published poms (== what users consume) since it can still change the content of the final pom,
 * while developing Flink it no longer works. This breaks plenty of things, since suddenly a bunch
 * of dependencies are still visible to downstream modules that weren't before.
 *
 * <p>To workaround this we mark all dependencies that we bundle as optional; this makes them
 * non-transitive. To a downstream module, behavior-wise a non-transitive dependency is identical to
 * a removed dependency.
 *
 * <p>This checker analyzes the bundled dependencies (based on the shade-plugin output) and the set
 * of dependencies (based on the dependency plugin) to detect cases where a dependency is not marked
 * as optional as it should.
 *
 * <p>The enforced rule is rather simple: Any dependency that is bundled, or any of its parents,
 * must show up as optional in the dependency tree. The parent clause is required to cover cases
 * where a module has 2 paths to a bundled dependency. If a module depends on A1/A2, each depending
 * on B, with A1 and B being bundled, then even if A1 is marked as optional B is still shown as a
 * non-optional dependency (because the non-optional A2 still needs it!).
 */
public class ShadeOptionalChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ShadeOptionalChecker.class);

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println(
                    "Usage: ShadeOptionalChecker <pathShadeBuildOutput> <pathMavenDependencyOutput>");
            System.exit(1);
        }

        final Path shadeOutputPath = Paths.get(args[0]);
        final Path dependencyOutputPath = Paths.get(args[1]);

        final Map<String, Set<Dependency>> bundledDependenciesByModule =
                ShadeParser.parseShadeOutput(shadeOutputPath);
        final Map<String, DependencyTree> dependenciesByModule =
                DependencyParser.parseDependencyTreeOutput(dependencyOutputPath);

        final Map<String, Set<Dependency>> violations =
                checkOptionalFlags(bundledDependenciesByModule, dependenciesByModule);

        if (!violations.isEmpty()) {
            LOG.error(
                    "{} modules bundle in total {} dependencies without them being marked as optional in the pom.",
                    violations.keySet().size(),
                    violations.values().stream().mapToInt(Collection::size).sum());
            LOG.error(
                    "\tIn order for shading to properly work within Flink we require all bundled dependencies to be marked as optional in the pom.");
            LOG.error(
                    "\tFor verification purposes we require the dependency tree from the dependency-plugin to show the dependency as either:");
            LOG.error("\t\ta) an optional dependency,");
            LOG.error("\t\tb) a transitive dependency of another optional dependency.");
            LOG.error(
                    "\tIn most cases adding '<optional>${flink.markBundledAsOptional}</optional>' to the bundled dependency is sufficient.");
            LOG.error(
                    "\tThere are some edge cases where a transitive dependency might be associated with the \"wrong\" dependency in the tree, for example if a test dependency also requires it.");
            LOG.error(
                    "\tIn such cases you need to adjust the poms so that the dependency shows up in the right spot. This may require adding an explicit dependency (Management) entry, excluding dependencies, or at times even reordering dependencies in the pom.");
            LOG.error(
                    "\tSee the Dependencies page in the wiki for details: https://cwiki.apache.org/confluence/display/FLINK/Dependencies");

            for (String moduleWithViolations : violations.keySet()) {
                final Collection<Dependency> dependencyViolations =
                        violations.get(moduleWithViolations);
                LOG.error(
                        "\tModule {} ({} violation{}):",
                        moduleWithViolations,
                        dependencyViolations.size(),
                        dependencyViolations.size() == 1 ? "" : "s");
                for (Dependency dependencyViolation : dependencyViolations) {
                    LOG.error("\t\t{}", dependencyViolation);
                }
            }

            System.exit(1);
        }
    }

    private static Map<String, Set<Dependency>> checkOptionalFlags(
            Map<String, Set<Dependency>> bundledDependenciesByModule,
            Map<String, DependencyTree> dependenciesByModule) {

        final Map<String, Set<Dependency>> allViolations = new HashMap<>();

        for (String module : bundledDependenciesByModule.keySet()) {
            LOG.debug("Checking module '{}'.", module);
            if (!dependenciesByModule.containsKey(module)) {
                throw new IllegalStateException(
                        String.format(
                                "Module %s listed by shade-plugin, but not dependency-plugin.",
                                module));
            }

            final Collection<Dependency> bundledDependencies =
                    bundledDependenciesByModule.get(module);
            final DependencyTree dependencyTree = dependenciesByModule.get(module);

            final Set<Dependency> violations =
                    checkOptionalFlags(module, bundledDependencies, dependencyTree);

            if (violations.isEmpty()) {
                LOG.info("OK: {}", module);
            } else {
                allViolations.put(module, violations);
            }
        }

        return allViolations;
    }

    @VisibleForTesting
    static Set<Dependency> checkOptionalFlags(
            String module,
            Collection<Dependency> bundledDependencies,
            DependencyTree dependencyTree) {

        bundledDependencies =
                bundledDependencies.stream()
                        // force-shading isn't relevant for this check but breaks some shortcuts
                        .filter(
                                dependency ->
                                        !dependency
                                                .getArtifactId()
                                                .equals("flink-shaded-force-shading"))
                        .collect(Collectors.toSet());

        final Set<Dependency> violations = new HashSet<>();

        if (bundledDependencies.isEmpty()) {
            LOG.debug("\tModule is not bundling any dependencies.");
            return violations;
        }

        // The set of dependencies that the module directly depends on and which downstream modules
        // would pull in transitively.
        //
        // If this set is empty we do not need to check anything.
        // This allows us to avoid some edge-cases:
        //
        // Assume module M has the following (full) dependency tree, bundling dependency 1 and 2:
        //
        // +- dependency1 (compile/optional)",
        // |  \- dependency2 (compile) (implicitly optional because dependency1 is optional)
        // \- dependency3 (test)
        //    \- dependency2 (compile)
        //
        // However, in the dependency plugin output a dependency can only show up once, so Maven may
        // return this:
        //
        // +- dependency1 (compile/optional)",
        // \- dependency3 (test)
        //    \- dependency2 (compile)
        //
        // Given this tree, and knowing that dependency2 is bundled, we would draw the conclusion
        // that dependency2 is missing the optional flag.
        //
        // However, because dependency 1/3 are optional/test dependencies they are not transitive.
        // Without any direct transitive dependency nothing can leak through to downstream modules,
        // removing the need to check dependency 2 at all (and in turn, saving us from having to
        // resolve this problem).
        final List<Dependency> directTransitiveDependencies =
                dependencyTree.getDirectDependencies().stream()
                        .filter(
                                dependency ->
                                        !(isOptional(dependency)
                                                || hasProvidedScope(dependency)
                                                || hasTestScope(dependency)
                                                || isCommonCompileDependency(dependency)))
                        .collect(Collectors.toList());

        // if nothing would be exposed to downstream modules we exit early to reduce noise on CI
        if (directTransitiveDependencies.isEmpty()) {
            LOG.debug(
                    "Skipping deep-check of module {} because all direct dependencies are not transitive.",
                    module);
            return violations;
        }
        LOG.debug(
                "Running deep-check of module {} because there are direct dependencies that are transitive: {}",
                module,
                directTransitiveDependencies);

        for (Dependency bundledDependency : bundledDependencies) {
            LOG.debug("\tChecking dependency '{}'.", bundledDependency);

            final List<Dependency> dependencyPath = dependencyTree.getPathTo(bundledDependency);

            final boolean isOptional =
                    dependencyPath.stream().anyMatch(parent -> parent.isOptional().orElse(false));

            if (!isOptional) {
                violations.add(bundledDependency);
            }
        }

        return violations;
    }

    private static boolean isOptional(Dependency dependency) {
        return dependency.isOptional().orElse(false);
    }

    private static boolean hasProvidedScope(Dependency dependency) {
        return "provided".equals(dependency.getScope().orElse(null));
    }

    private static boolean hasTestScope(Dependency dependency) {
        return "test".equals(dependency.getScope().orElse(null));
    }

    /**
     * These are compile dependencies that are set up in the root pom. We do not require modules to
     * mark these as optional because all modules depend on them anyway; whether they leak through
     * or not is therefore irrelevant.
     */
    private static boolean isCommonCompileDependency(Dependency dependency) {
        return "flink-shaded-force-shading".equals(dependency.getArtifactId())
                || "jsr305".equals(dependency.getArtifactId())
                || "slf4j-api".equals(dependency.getArtifactId());
    }
}
