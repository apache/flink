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

package org.apache.flink.tools.ci.licensecheck;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.tools.ci.utils.dependency.DependencyParser;
import org.apache.flink.tools.ci.utils.deploy.DeployParser;
import org.apache.flink.tools.ci.utils.notice.NoticeContents;
import org.apache.flink.tools.ci.utils.notice.NoticeParser;
import org.apache.flink.tools.ci.utils.shade.ShadeParser;
import org.apache.flink.tools.ci.utils.shared.Dependency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class checking for proper NOTICE files based on the maven build output. */
public class NoticeFileChecker {

    private static final Logger LOG = LoggerFactory.getLogger(NoticeFileChecker.class);

    private static final List<String> MODULES_DEFINING_EXCESS_DEPENDENCIES =
            loadFromResources("modules-defining-excess-dependencies.modulelist");

    // Examples:
    // "- org.apache.htrace:htrace-core:3.1.0-incubating"
    // or
    // "This project bundles "net.jcip:jcip-annotations:1.0".
    private static final Pattern NOTICE_DEPENDENCY_PATTERN =
            Pattern.compile(
                    "- ([^ :]+):([^:]+):([^ ]+)($| )|.*bundles \"([^:]+):([^:]+):([^\"]+)\".*");

    static int run(File buildResult, Path root) throws IOException {
        // parse included dependencies from build output
        final Map<String, Set<Dependency>> modulesWithBundledDependencies =
                combineAndFilterFlinkDependencies(
                        ShadeParser.parseShadeOutput(buildResult.toPath()),
                        DependencyParser.parseDependencyCopyOutput(buildResult.toPath()));

        final Set<String> deployedModules = DeployParser.parseDeployOutput(buildResult);

        LOG.info(
                "Extracted "
                        + deployedModules.size()
                        + " modules that were deployed and "
                        + modulesWithBundledDependencies.keySet().size()
                        + " modules which bundle dependencies with a total of "
                        + modulesWithBundledDependencies.values().size()
                        + " dependencies");

        // find modules producing a shaded-jar
        List<Path> noticeFiles = findNoticeFiles(root);
        LOG.info("Found {} NOTICE files to check", noticeFiles.size());

        final Map<String, Optional<NoticeContents>> moduleToNotice =
                noticeFiles.stream()
                        .collect(
                                Collectors.toMap(
                                        NoticeFileChecker::getModuleFromNoticeFile,
                                        noticeFile -> {
                                            try {
                                                return NoticeParser.parseNoticeFile(noticeFile);
                                            } catch (IOException e) {
                                                // some machine issue
                                                throw new RuntimeException(e);
                                            }
                                        }));

        return run(modulesWithBundledDependencies, deployedModules, moduleToNotice);
    }

    @VisibleForTesting
    static int run(
            Map<String, Set<Dependency>> modulesWithBundledDependencies,
            Set<String> deployedModules,
            Map<String, Optional<NoticeContents>> noticeFiles)
            throws IOException {
        int severeIssueCount = 0;

        final Set<String> modulesSkippingDeployment =
                new HashSet<>(modulesWithBundledDependencies.keySet());
        modulesSkippingDeployment.removeAll(deployedModules);

        LOG.debug(
                "The following {} modules are skipping deployment: {}",
                modulesSkippingDeployment.size(),
                modulesSkippingDeployment.stream()
                        .sorted()
                        .collect(Collectors.joining("\n\t", "\n\t", "")));

        for (String moduleSkippingDeployment : modulesSkippingDeployment) {
            // TODO: this doesn't work for modules requiring a NOTICE that are bundled indirectly
            // TODO: via another non-deployed module
            boolean bundledByDeployedModule =
                    modulesWithBundledDependencies.entrySet().stream()
                            .filter(
                                    entry ->
                                            entry.getValue().stream()
                                                    .map(Dependency::getArtifactId)
                                                    .anyMatch(
                                                            artifactId ->
                                                                    artifactId.equals(
                                                                            moduleSkippingDeployment)))
                            .anyMatch(entry -> !modulesSkippingDeployment.contains(entry.getKey()));

            if (!bundledByDeployedModule) {
                modulesWithBundledDependencies.remove(moduleSkippingDeployment);
            } else {
                LOG.debug(
                        "Including module {} in license checks, despite not being deployed, because it is bundled by another deployed module.",
                        moduleSkippingDeployment);
            }
        }

        // check that all required NOTICE files exists
        severeIssueCount +=
                ensureRequiredNoticeFiles(modulesWithBundledDependencies, noticeFiles.keySet());

        // check each NOTICE file
        for (Map.Entry<String, Optional<NoticeContents>> noticeFile : noticeFiles.entrySet()) {
            severeIssueCount +=
                    checkNoticeFileAndLogProblems(
                            modulesWithBundledDependencies,
                            noticeFile.getKey(),
                            noticeFile.getValue().orElse(null));
        }

        return severeIssueCount;
    }

    private static Map<String, Set<Dependency>> combineAndFilterFlinkDependencies(
            Map<String, Set<Dependency>> modulesWithBundledDependencies,
            Map<String, Set<Dependency>> modulesWithCopiedDependencies) {

        final Map<String, Set<Dependency>> combinedAndFiltered = new LinkedHashMap<>();

        Stream.concat(
                        modulesWithBundledDependencies.entrySet().stream(),
                        modulesWithCopiedDependencies.entrySet().stream())
                .forEach(
                        (entry) -> {
                            final Set<Dependency> dependencies =
                                    combinedAndFiltered.computeIfAbsent(
                                            entry.getKey(), ignored -> new LinkedHashSet<>());

                            for (Dependency dependency : entry.getValue()) {
                                if (!dependency.getGroupId().contains("org.apache.flink")) {
                                    dependencies.add(dependency);
                                }
                            }
                        });

        return combinedAndFiltered;
    }

    private static int ensureRequiredNoticeFiles(
            Map<String, Set<Dependency>> modulesWithShadedDependencies,
            Collection<String> modulesWithNoticeFile) {
        int severeIssueCount = 0;
        Set<String> shadingModules = new HashSet<>(modulesWithShadedDependencies.keySet());
        shadingModules.removeAll(modulesWithNoticeFile);
        for (String moduleWithoutNotice : shadingModules) {
            if (modulesWithShadedDependencies.get(moduleWithoutNotice).stream()
                    .anyMatch(dependency -> !dependency.getGroupId().equals("org.apache.flink"))) {
                LOG.error(
                        "Module {} is missing a NOTICE file. It has shaded dependencies: {}",
                        moduleWithoutNotice,
                        modulesWithShadedDependencies.get(moduleWithoutNotice).stream()
                                .map(Dependency::toString)
                                .collect(Collectors.joining("\n\t", "\n\t", "")));
                severeIssueCount++;
            }
        }
        return severeIssueCount;
    }

    private static String getModuleFromNoticeFile(Path noticeFile) {
        Path moduleDirectory =
                noticeFile
                        .getParent() // META-INF
                        .getParent() // resources
                        .getParent() // main
                        .getParent() // src
                        .getParent(); // <-- module name
        return moduleDirectory.getFileName().toString();
    }

    private static int checkNoticeFileAndLogProblems(
            Map<String, Set<Dependency>> modulesWithShadedDependencies,
            String moduleName,
            @Nullable NoticeContents noticeContents)
            throws IOException {

        final Map<Severity, List<String>> problemsBySeverity =
                checkNoticeFile(modulesWithShadedDependencies, moduleName, noticeContents);

        final List<String> severeProblems =
                problemsBySeverity.getOrDefault(Severity.CRITICAL, Collections.emptyList());

        if (!problemsBySeverity.isEmpty()) {
            final List<String> toleratedProblems =
                    problemsBySeverity.getOrDefault(Severity.TOLERATED, Collections.emptyList());
            final List<String> expectedProblems =
                    problemsBySeverity.getOrDefault(Severity.SUPPRESSED, Collections.emptyList());

            LOG.info(
                    "Problems were detected for a NOTICE file.\n" + "\t{}:\n" + "{}{}{}",
                    moduleName,
                    convertProblemsToIndentedString(
                            severeProblems,
                            "These issue are legally problematic and MUST be fixed:"),
                    convertProblemsToIndentedString(
                            toleratedProblems,
                            "These issues are mistakes that aren't legally problematic. They SHOULD be fixed at some point, but we don't have to:"),
                    convertProblemsToIndentedString(
                            expectedProblems, "These issues are assumed to be false-positives:"));
        }

        return severeProblems.size();
    }

    @VisibleForTesting
    static Map<Severity, List<String>> checkNoticeFile(
            Map<String, Set<Dependency>> modulesWithShadedDependencies,
            String moduleName,
            @Nullable NoticeContents noticeContents) {

        final Map<Severity, List<String>> problemsBySeverity = new HashMap<>();

        if (noticeContents == null) {
            addProblem(problemsBySeverity, Severity.CRITICAL, "The NOTICE file was empty.");
        } else {
            // first line must be the module name.
            if (!noticeContents.getNoticeModuleName().equals(moduleName)) {
                addProblem(
                        problemsBySeverity,
                        Severity.TOLERATED,
                        String.format(
                                "First line does not start with module name. firstLine=%s",
                                noticeContents.getNoticeModuleName()));
            }

            // collect all declared dependencies from NOTICE file
            Set<Dependency> declaredDependencies = new HashSet<>();
            for (Dependency declaredDependency : noticeContents.getDeclaredDependencies()) {
                if (!declaredDependencies.add(declaredDependency)) {
                    addProblem(
                            problemsBySeverity,
                            Severity.CRITICAL,
                            String.format("Dependency %s is declared twice.", declaredDependency));
                }
            }

            // find all dependencies missing from NOTICE file
            Collection<Dependency> expectedDependencies =
                    modulesWithShadedDependencies.getOrDefault(moduleName, Collections.emptySet())
                            .stream()
                            .filter(
                                    dependency ->
                                            !dependency.getGroupId().equals("org.apache.flink"))
                            .collect(Collectors.toList());

            for (Dependency expectedDependency : expectedDependencies) {
                if (!declaredDependencies.contains(expectedDependency)) {
                    addProblem(
                            problemsBySeverity,
                            Severity.CRITICAL,
                            String.format("Dependency %s is not listed.", expectedDependency));
                }
            }

            boolean moduleDefinesExcessDependencies =
                    MODULES_DEFINING_EXCESS_DEPENDENCIES.contains(moduleName);

            // find all dependencies defined in NOTICE file, which were not expected
            for (Dependency declaredDependency : declaredDependencies) {
                if (!expectedDependencies.contains(declaredDependency)) {
                    final Severity severity =
                            moduleDefinesExcessDependencies
                                    ? Severity.SUPPRESSED
                                    : Severity.TOLERATED;
                    addProblem(
                            problemsBySeverity,
                            severity,
                            String.format(
                                    "Dependency %s is not bundled, but listed.",
                                    declaredDependency));
                }
            }
        }

        return problemsBySeverity;
    }

    private static void addProblem(
            Map<Severity, List<String>> problemsBySeverity, Severity severity, String problem) {
        problemsBySeverity.computeIfAbsent(severity, ignored -> new ArrayList<>()).add(problem);
    }

    @VisibleForTesting
    enum Severity {
        /** Issues that a legally problematic which must be fixed. */
        CRITICAL,
        /** Issues that affect the correctness but aren't legally problematic. */
        TOLERATED,
        /** Issues where we intentionally break the rules. */
        SUPPRESSED
    }

    private static String convertProblemsToIndentedString(List<String> problems, String header) {
        return problems.isEmpty()
                ? ""
                : problems.stream()
                        .map(s -> "\t\t\t" + s)
                        .collect(Collectors.joining("\n", "\t\t " + header + " \n", "\n"));
    }

    private static List<Path> findNoticeFiles(Path root) throws IOException {
        return Files.walk(root)
                .filter(
                        file -> {
                            int nameCount = file.getNameCount();
                            return file.getName(nameCount - 3).toString().equals("resources")
                                    && file.getName(nameCount - 2).toString().equals("META-INF")
                                    && file.getName(nameCount - 1).toString().equals("NOTICE");
                        })
                .collect(Collectors.toList());
    }

    private static List<String> loadFromResources(String fileName) {
        try {
            try (BufferedReader bufferedReader =
                    new BufferedReader(
                            new InputStreamReader(
                                    Objects.requireNonNull(
                                            NoticeFileChecker.class.getResourceAsStream(
                                                    "/" + fileName))))) {

                List<String> result =
                        bufferedReader
                                .lines()
                                .filter(line -> !line.startsWith("#") && !line.isEmpty())
                                .collect(Collectors.toList());
                LOG.debug("Loaded {} items from resource {}", result.size(), fileName);
                return result;
            }
        } catch (Throwable e) {
            // wrap anything in a RuntimeException to be callable from the static initializer
            throw new RuntimeException("Error while loading resource", e);
        }
    }
}
