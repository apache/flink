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

package org.apache.flink.docs.util;

import org.apache.flink.annotation.docs.Documentation;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConfigurationOptionLocator}. */
class ConfigurationOptionLocatorTest {

    private static final String SOURCE_ROOT = "src/main/java";

    private static final String SECTION_ANNOTATION = "@Documentation.Section";

    private static final Set<String> PRUNED_DIRECTORIES =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList("target", "node_modules", ".git")));

    /** Mirrors the file names {@link ConfigurationOptionLocator} recognizes. */
    private static final Pattern OPTIONS_CLASS_FILE_NAME =
            Pattern.compile("[a-zA-Z]*(?:Options|Config|Parameters)\\.java");

    /**
     * Verifies that every option annotated with {@link Documentation.Section} sits in a package
     * that {@link ConfigurationOptionLocator} actually searches.
     *
     * <p>The annotation is an explicit statement that the option belongs in the generated
     * configuration reference, but discovery is driven by a hard-coded list of packages and does
     * not recurse into sub-packages. An option outside that list is therefore dropped from the
     * reference without any error, and {@code ConfigOptionsDocsCompletenessITCase} cannot catch it
     * because it derives its expectations from the same list.
     */
    @Test
    void testSectionAnnotatedOptionsAreAllDiscoverable() throws IOException {
        final Path rootDir = Paths.get(Utils.getProjectRootDir()).toAbsolutePath().normalize();

        final Set<String> searchedPackages =
                Arrays.stream(ConfigurationOptionLocator.getLocations())
                        .map(
                                location ->
                                        location.getModule()
                                                + '/'
                                                + location.getPackage().replace('.', '/'))
                        .collect(Collectors.toSet());

        final List<String> undiscoverable = new ArrayList<>();
        for (Path optionsClass : findSectionAnnotatedOptionClasses(rootDir)) {
            final String relativePath = toUnixPath(rootDir.relativize(optionsClass));
            final String modulePath = relativePath.substring(0, relativePath.indexOf(SOURCE_ROOT));
            final String packagePath =
                    relativePath.substring(
                            modulePath.length() + SOURCE_ROOT.length() + 1,
                            relativePath.lastIndexOf('/'));

            if (!searchedPackages.contains(modulePath + packagePath)) {
                undiscoverable.add(relativePath);
            }
        }

        assertThat(undiscoverable)
                .as(
                        "The options in these classes are annotated with @Documentation.Section but "
                                + "cannot be found by %s, so they are silently missing from the "
                                + "generated configuration reference. Add an %s entry for the "
                                + "containing package to %s#LOCATIONS.",
                        ConfigurationOptionLocator.class.getSimpleName(),
                        OptionsClassLocation.class.getSimpleName(),
                        ConfigurationOptionLocator.class.getSimpleName())
                .isEmpty();
    }

    private static List<Path> findSectionAnnotatedOptionClasses(Path rootDir) throws IOException {
        final List<Path> optionClasses = new ArrayList<>();

        Files.walkFileTree(
                rootDir,
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult preVisitDirectory(
                            Path dir, BasicFileAttributes attributes) {
                        return PRUNED_DIRECTORIES.contains(dir.getFileName().toString())
                                ? FileVisitResult.SKIP_SUBTREE
                                : FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                            throws IOException {
                        if (OPTIONS_CLASS_FILE_NAME.matcher(file.getFileName().toString()).matches()
                                && toUnixPath(file).contains('/' + SOURCE_ROOT + '/')
                                && isSectionAnnotated(file)) {
                            optionClasses.add(file);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                });

        return optionClasses;
    }

    private static boolean isSectionAnnotated(Path file) throws IOException {
        try (Stream<String> lines = Files.lines(file)) {
            return lines.anyMatch(line -> line.contains(SECTION_ANNOTATION));
        }
    }

    private static String toUnixPath(Path path) {
        return path.toString().replace(path.getFileSystem().getSeparator(), "/");
    }
}
