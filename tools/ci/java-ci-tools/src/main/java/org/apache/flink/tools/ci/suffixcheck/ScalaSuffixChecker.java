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

package org.apache.flink.tools.ci.suffixcheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Utility for checking the presence/absence of scala-suffixes. */
public class ScalaSuffixChecker {
    private static final Logger LOG = LoggerFactory.getLogger(ScalaSuffixChecker.class);

    // [INFO] --- maven-dependency-plugin:3.1.1:tree (default-cli) @ flink-annotations ---
    private static final Pattern moduleNamePattern =
            Pattern.compile(".* --- maven-dependency-plugin.* @ (.*) ---.*");

    // [INFO] +- junit:junit:jar:4.13.2:test
    // [INFO] |  \- org.hamcrest:hamcrest-core:jar:1.3:test
    // [INFO] \- org.apache.logging.log4j:log4j-1.2-api:jar:2.14.1:test
    private static final Pattern blockPattern = Pattern.compile(".* [+|\\\\].*");

    // [INFO] +- org.scala-lang:scala-reflect:jar:2.11.12:test
    private static final Pattern scalaSuffixPattern = Pattern.compile("_2.1[0-9]");

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: ScalaSuffixChecker <pathMavenBuildOutput> <pathFlinkRoot>");
            System.exit(1);
        }

        final Path mavenOutputPath = Paths.get(args[0]);
        final Path flinkRootPath = Paths.get(args[1]);

        final ParseResult parseResult = parseMavenOutput(mavenOutputPath);
        if (parseResult.getCleanModules().isEmpty()) {
            LOG.error("Parsing found 0 scala-free modules; the parsing is likely broken.");
            System.exit(1);
        }
        if (parseResult.getInfectedModules().isEmpty()) {
            LOG.error("Parsing found 0 scala-dependent modules; the parsing is likely broken.");
            System.exit(1);
        }

        final Collection<String> violations = checkScalaSuffixes(parseResult, flinkRootPath);

        if (!violations.isEmpty()) {
            LOG.error(
                    "Violations found:{}",
                    violations.stream().collect(Collectors.joining("\n\t", "\n\t", "")));
            System.exit(1);
        }
    }

    private static ParseResult parseMavenOutput(final Path path) throws IOException {
        final Set<String> cleanModules = new HashSet<>();
        final Set<String> infectedModules = new HashSet<>();

        try (final BufferedReader bufferedReader =
                Files.newBufferedReader(path, StandardCharsets.UTF_8)) {

            String line;
            while ((line = bufferedReader.readLine()) != null) {
                final Matcher matcher = moduleNamePattern.matcher(line);
                if (matcher.matches()) {
                    final String moduleName = stripScalaSuffix(matcher.group(1));
                    LOG.trace("Parsing module '{}'.", moduleName);

                    // skip: [INFO] org.apache.flink:flink-annotations:jar:1.14-SNAPSHOT
                    bufferedReader.readLine();

                    boolean infected = false;
                    line = bufferedReader.readLine();
                    while (blockPattern.matcher(line).matches()) {
                        final boolean dependsOnScala = dependsOnScala(line);
                        final boolean isTestDependency = line.endsWith(":test");
                        LOG.trace("\tline:{}", line);
                        LOG.trace("\t\tdepends-on-scala:{}", dependsOnScala);
                        LOG.trace("\t\tis-test-dependency:{}", isTestDependency);
                        if (dependsOnScala && !isTestDependency) {
                            LOG.trace("\t\tOutbreak detected at {}!", moduleName);
                            infected = true;
                        }

                        line = bufferedReader.readLine();
                    }

                    if (infected) {
                        infectedModules.add(moduleName);
                    } else {
                        cleanModules.add(moduleName);
                    }
                }
            }
        }

        return new ParseResult(cleanModules, infectedModules);
    }

    private static String stripScalaSuffix(final String moduleName) {
        final int i = moduleName.indexOf("_2.");
        return i > 0 ? moduleName.substring(0, i) : moduleName;
    }

    private static boolean dependsOnScala(final String line) {
        return line.contains("org.scala-lang") || scalaSuffixPattern.matcher(line).find();
    }

    private static Collection<String> checkScalaSuffixes(
            final ParseResult parseResult, Path flinkRootPath) throws IOException {
        final Collection<String> violations = new ArrayList<>();

        // exclude e2e modules and flink-docs for convenience as they
        // a) are not deployed during a release
        // b) exist only for dev purposes
        // c) no-one should depend on them
        final Collection<String> excludedModules = new ArrayList<>();
        excludedModules.add("flink-docs");
        excludedModules.addAll(getEndToEndTestModules(flinkRootPath));

        for (String excludedModule : excludedModules) {
            parseResult.getCleanModules().remove(excludedModule);
            parseResult.getInfectedModules().remove(excludedModule);
        }

        violations.addAll(checkCleanModules(parseResult.getCleanModules(), flinkRootPath));
        violations.addAll(checkInfectedModules(parseResult.getInfectedModules(), flinkRootPath));

        return violations;
    }

    private static Collection<String> getEndToEndTestModules(Path flinkRootPath)
            throws IOException {
        try (Stream<Path> pathStream =
                Files.walk(flinkRootPath.resolve("flink-end-to-end-tests"), 5)) {
            return pathStream
                    .filter(path -> path.getFileName().toString().equals("pom.xml"))
                    .map(path -> path.getParent().getFileName().toString())
                    .collect(Collectors.toList());
        }
    }

    private static Collection<String> checkCleanModules(
            Collection<String> modules, Path flinkRootPath) throws IOException {
        return checkModules(
                modules,
                flinkRootPath,
                "_${scala.binary.version}",
                "Scala-free module '%s' is referenced with scala suffix in '%s'.");
    }

    private static Collection<String> checkInfectedModules(
            Collection<String> modules, Path flinkRootPath) throws IOException {
        return checkModules(
                modules,
                flinkRootPath,
                "",
                "Scala-dependent module '%s' is referenced without scala suffix in '%s'.");
    }

    private static Collection<String> checkModules(
            Collection<String> modules,
            Path flinkRootPath,
            String moduleSuffix,
            String violationTemplate)
            throws IOException {

        final ArrayList<String> sortedModules = new ArrayList<>(modules);
        sortedModules.sort(String::compareTo);

        final Collection<String> violations = new ArrayList<>();
        for (String module : sortedModules) {
            int numPreviousViolations = violations.size();
            try (Stream<Path> pathStream = Files.walk(flinkRootPath, 3)) {
                final List<Path> pomFiles =
                        pathStream
                                .filter(path -> path.getFileName().toString().equals("pom.xml"))
                                .collect(Collectors.toList());

                for (Path pomFile : pomFiles) {
                    try (Stream<String> lines = Files.lines(pomFile, StandardCharsets.UTF_8)) {
                        final boolean existsCleanReference =
                                lines.anyMatch(
                                        line ->
                                                line.contains(
                                                        module + moduleSuffix + "</artifactId>"));

                        if (existsCleanReference) {
                            violations.add(
                                    String.format(
                                            violationTemplate,
                                            module,
                                            flinkRootPath.relativize(pomFile)));
                        }
                    }
                }
            }
            if (numPreviousViolations == violations.size()) {
                LOG.info("OK {}", module);
            }
        }
        return violations;
    }

    private static class ParseResult {

        private final Set<String> cleanModules;
        private final Set<String> infectedModules;

        private ParseResult(Set<String> cleanModules, Set<String> infectedModules) {
            this.cleanModules = cleanModules;
            this.infectedModules = infectedModules;
        }

        public Set<String> getCleanModules() {
            return cleanModules;
        }

        public Set<String> getInfectedModules() {
            return infectedModules;
        }
    }
}
