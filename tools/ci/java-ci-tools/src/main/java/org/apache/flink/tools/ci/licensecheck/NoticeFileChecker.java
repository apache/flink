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

import org.apache.flink.tools.ci.utils.dependency.DependencyParser;
import org.apache.flink.tools.ci.utils.shared.Dependency;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility class checking for proper NOTICE files based on the maven build output. */
public class NoticeFileChecker {

    private static final Logger LOG = LoggerFactory.getLogger(NoticeFileChecker.class);

    private static final List<String> MODULES_SKIPPING_DEPLOYMENT =
            loadFromResources("modules-skipping-deployment.modulelist");

    private static final List<String> MODULES_DEFINING_EXCESS_DEPENDENCIES =
            loadFromResources("modules-defining-excess-dependencies.modulelist");

    // pattern for maven shade plugin
    private static final Pattern SHADE_NEXT_MODULE_PATTERN =
            Pattern.compile(
                    ".*:shade \\((shade-flink|shade-dist|default)\\) @ ([^ _]+)(_[0-9.]+)? --.*");
    private static final Pattern SHADE_INCLUDE_MODULE_PATTERN =
            Pattern.compile(".*Including ([^:]+):([^:]+):jar:([^ ]+) in the shaded jar");

    // Examples:
    // "- org.apache.htrace:htrace-core:3.1.0-incubating"
    // or
    // "This project bundles "net.jcip:jcip-annotations:1.0".
    private static final Pattern NOTICE_DEPENDENCY_PATTERN =
            Pattern.compile(
                    "- ([^ :]+):([^:]+):([^ ]+)($| )|.*bundles \"([^:]+):([^:]+):([^\"]+)\".*");

    static int run(File buildResult, Path root) throws IOException {
        int severeIssueCount = 0;
        // parse included dependencies from build output
        final Multimap<String, Dependency> modulesWithBundledDependencies =
                combine(
                        parseModulesFromBuildResult(buildResult),
                        DependencyParser.parseDependencyCopyOutput(buildResult.toPath()));

        LOG.info(
                "Extracted "
                        + modulesWithBundledDependencies.keySet().size()
                        + " modules with a total of "
                        + modulesWithBundledDependencies.values().size()
                        + " dependencies");

        // find modules producing a shaded-jar
        List<Path> noticeFiles = findNoticeFiles(root);
        LOG.info("Found {} NOTICE files to check", noticeFiles.size());

        // check that all required NOTICE files exists
        severeIssueCount += ensureRequiredNoticeFiles(modulesWithBundledDependencies, noticeFiles);

        // check each NOTICE file
        for (Path noticeFile : noticeFiles) {
            severeIssueCount += checkNoticeFile(modulesWithBundledDependencies, noticeFile);
        }

        return severeIssueCount;
    }

    private static Multimap<String, Dependency> combine(
            Multimap<String, Dependency> modulesWithBundledDependencies,
            Map<String, Set<Dependency>> modulesWithCopiedDependencies) {
        modulesWithCopiedDependencies.forEach(
                (module, copiedDependencies) -> {
                    copiedDependencies.stream()
                            .filter(
                                    dependency ->
                                            !dependency.getGroupId().contains("org.apache.flink"))
                            .forEach(
                                    dependency ->
                                            modulesWithBundledDependencies.put(module, dependency));
                });
        return modulesWithBundledDependencies;
    }

    private static int ensureRequiredNoticeFiles(
            Multimap<String, Dependency> modulesWithShadedDependencies, List<Path> noticeFiles) {
        int severeIssueCount = 0;
        Set<String> shadingModules = new HashSet<>(modulesWithShadedDependencies.keys());
        shadingModules.removeAll(
                noticeFiles.stream()
                        .map(NoticeFileChecker::getModuleFromNoticeFile)
                        .collect(Collectors.toList()));
        for (String moduleWithoutNotice : shadingModules) {
            if (!MODULES_SKIPPING_DEPLOYMENT.contains(moduleWithoutNotice)) {
                LOG.error(
                        "Module {} is missing a NOTICE file. It has shaded dependencies: {}",
                        moduleWithoutNotice,
                        modulesWithShadedDependencies.get(moduleWithoutNotice));
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

    private static int checkNoticeFile(
            Multimap<String, Dependency> modulesWithShadedDependencies, Path noticeFile)
            throws IOException {
        String moduleName = getModuleFromNoticeFile(noticeFile);

        // 1st line contains module name
        List<String> noticeContents = Files.readAllLines(noticeFile);

        final Map<Severity, List<String>> problemsBySeverity = new HashMap<>();

        if (noticeContents.isEmpty()) {
            addProblem(problemsBySeverity, Severity.CRITICAL, "The NOTICE file was empty.");
        }

        // first line must be the module name.
        if (!noticeContents.get(0).equals(moduleName)) {
            addProblem(
                    problemsBySeverity,
                    Severity.TOLERATED,
                    String.format(
                            "First line does not start with module name. firstLine=%s",
                            noticeContents.get(0)));
        }

        // collect all declared dependencies from NOTICE file
        Set<Dependency> declaredDependencies = new HashSet<>();
        for (String line : noticeContents) {
            Matcher noticeDependencyMatcher = NOTICE_DEPENDENCY_PATTERN.matcher(line);
            if (noticeDependencyMatcher.find()) {
                String groupId = noticeDependencyMatcher.group(1);
                String artifactId = noticeDependencyMatcher.group(2);
                String version = noticeDependencyMatcher.group(3);
                if (groupId == null && artifactId == null && version == null) { // "bundles" case
                    groupId = noticeDependencyMatcher.group(5);
                    artifactId = noticeDependencyMatcher.group(6);
                    version = noticeDependencyMatcher.group(7);
                }
                Dependency toAdd = Dependency.create(groupId, artifactId, version);
                if (!declaredDependencies.add(toAdd)) {
                    addProblem(
                            problemsBySeverity,
                            Severity.CRITICAL,
                            String.format("Dependency %s is declared twice.", toAdd));
                }
            }
        }
        // find all dependencies missing from NOTICE file
        Collection<Dependency> expectedDependencies = modulesWithShadedDependencies.get(moduleName);
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
                        moduleDefinesExcessDependencies ? Severity.SUPPRESSED : Severity.TOLERATED;
                addProblem(
                        problemsBySeverity,
                        severity,
                        String.format(
                                "Dependency %s is not bundled, but listed.", declaredDependency));
            }
        }

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

    private static void addProblem(
            Map<Severity, List<String>> problemsBySeverity, Severity severity, String problem) {
        problemsBySeverity.computeIfAbsent(severity, ignored -> new ArrayList<>()).add(problem);
    }

    private enum Severity {
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

    private static Multimap<String, Dependency> parseModulesFromBuildResult(File buildResult)
            throws IOException {
        Multimap<String, Dependency> result = ArrayListMultimap.create();

        try (Stream<String> lines = Files.lines(buildResult.toPath())) {
            // String line;
            String currentShadeModule = null;
            for (String line : (Iterable<String>) lines::iterator) {
                Matcher nextShadeModuleMatcher = SHADE_NEXT_MODULE_PATTERN.matcher(line);
                if (nextShadeModuleMatcher.find()) {
                    currentShadeModule = nextShadeModuleMatcher.group(2);
                }

                if (currentShadeModule != null) {
                    Matcher includeMatcher = SHADE_INCLUDE_MODULE_PATTERN.matcher(line);
                    if (includeMatcher.find()) {
                        String groupId = includeMatcher.group(1);
                        String artifactId = includeMatcher.group(2);
                        String version = includeMatcher.group(3);
                        if (!"org.apache.flink".equals(groupId)) {
                            result.put(
                                    currentShadeModule,
                                    Dependency.create(groupId, artifactId, version));
                        }
                    }
                }
                if (line.contains("Replacing original artifact with shaded artifact")) {
                    currentShadeModule = null;
                }
            }
        }
        return result;
    }

    private static List<String> loadFromResources(String fileName) {
        try {
            Path resource = Paths.get(NoticeFileChecker.class.getResource("/" + fileName).toURI());
            List<String> result =
                    Files.readAllLines(resource).stream()
                            .filter(line -> !line.startsWith("#") && !line.isEmpty())
                            .collect(Collectors.toList());
            LOG.debug("Loaded {} items from resource {}", result.size(), fileName);
            return result;
        } catch (Throwable e) {
            // wrap anything in a RuntimeException to be callable from the static initializer
            throw new RuntimeException("Error while loading resource", e);
        }
    }
}
