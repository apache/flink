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

package org.apache.flink.packaging;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils around jar packaging. */
public class PackagingTestUtils {

    /**
     * Verifies that all files in the jar match one of the provided allow strings.
     *
     * <p>An allow item ending on a {@code "/"} is treated as an allowed parent directory.
     * Otherwise, it is treated as an allowed file.
     *
     * <p>For example, given a jar containing a file {@code META-INF/NOTICES}:
     *
     * <p>These would pass:
     *
     * <ul>
     *   <li>{@code "META-INF/"}
     *   <li>{@code "META-INF/NOTICES"}
     * </ul>
     *
     * <p>These would fail:
     *
     * <ul>
     *   <li>{@code "META-INF"}
     *   <li>{@code "META-INF/NOTICE"}
     *   <li>{@code "META-INF/NOTICES/"}
     * </ul>
     */
    public static void assertJarContainsOnlyFilesMatching(
            Path jarPath, Collection<String> allowedPaths) throws Exception {
        final URI jar = jarPath.toUri();

        try (final FileSystem fileSystem =
                FileSystems.newFileSystem(
                        new URI("jar:file", jar.getHost(), jar.getPath(), jar.getFragment()),
                        Collections.emptyMap())) {
            try (Stream<Path> walk = Files.walk(fileSystem.getPath("/"))) {
                walk.filter(file -> !Files.isDirectory(file))
                        .map(file -> file.toAbsolutePath().toString())
                        .map(file -> file.startsWith("/") ? file.substring(1) : file)
                        .forEach(
                                file ->
                                        assertThat(allowedPaths)
                                                .as("Bad file in JAR: %s", file)
                                                .anySatisfy(
                                                        allowedPath -> {
                                                            if (allowedPath.endsWith("/")) {
                                                                assertThat(file)
                                                                        .startsWith(allowedPath);
                                                            } else {
                                                                assertThat(file)
                                                                        .isEqualTo(allowedPath);
                                                            }
                                                        }));
            }
        }
    }

    /**
     * Verifies that the given jar contains a service entry file for the given service.
     *
     * <p>Caution: This only checks that the file exists; the content is not verified.
     */
    public static void assertJarContainsServiceEntry(Path jarPath, Class<?> service)
            throws Exception {
        final URI jar = jarPath.toUri();

        try (final FileSystem fileSystem =
                FileSystems.newFileSystem(
                        new URI("jar:file", jar.getHost(), jar.getPath(), jar.getFragment()),
                        Collections.emptyMap())) {
            assertThat(fileSystem.getPath("META-INF", "services", service.getName())).exists();
        }
    }
}
