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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PackagingTestUtilsTest {

    @Test
    void testAssertJarContainsOnlyFilesMatching(@TempDir Path tmp) throws Exception {
        Path jar = createJar(tmp, Entry.fileEntry("", Arrays.asList("META-INF", "NOTICES")));

        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar, Collections.singleton("META-INF/"));
        PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                jar, Collections.singleton("META-INF/NOTICES"));

        assertThatThrownBy(
                        () ->
                                PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                                        jar, Collections.singleton("META-INF")))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(
                        () ->
                                PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                                        jar, Collections.singleton("META-INF/NOTICE")))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(
                        () ->
                                PackagingTestUtils.assertJarContainsOnlyFilesMatching(
                                        jar, Collections.singleton("META-INF/NOTICES/")))
                .isInstanceOf(AssertionError.class);
    }

    @Test
    void testAssertJarContainsServiceEntry(@TempDir Path tmp) throws Exception {
        final String service = PackagingTestUtilsTest.class.getName();
        Path jar =
                createJar(tmp, Entry.fileEntry("", Arrays.asList("META-INF", "services", service)));

        PackagingTestUtils.assertJarContainsServiceEntry(jar, PackagingTestUtilsTest.class);

        assertThatThrownBy(
                        () ->
                                PackagingTestUtils.assertJarContainsServiceEntry(
                                        jar, PackagingTestUtils.class))
                .isInstanceOf(AssertionError.class);
    }

    private static class Entry {
        final String contents;
        final List<String> path;
        final boolean isDirectory;

        public static Entry directoryEntry(List<String> path) {
            return new Entry("", path, true);
        }

        public static Entry fileEntry(String contents, List<String> path) {
            return new Entry(contents, path, false);
        }

        private Entry(String contents, List<String> path, boolean isDirectory) {
            this.contents = contents;
            this.path = path;
            this.isDirectory = isDirectory;
        }
    }

    private static Path createJar(Path tempDir, Entry... entries) throws Exception {
        final Path path = tempDir.resolve(UUID.randomUUID().toString() + ".jar");

        final URI uri = path.toUri();

        final URI jarUri = new URI("jar:file", uri.getHost(), uri.getPath(), uri.getFragment());

        // causes FileSystems#newFileSystem to automatically create a valid zip file
        // this is easier than manually creating a valid empty zip file manually
        final Map<String, String> env = new HashMap<>();
        env.put("create", "true");

        try (FileSystem zip = FileSystems.newFileSystem(jarUri, env)) {
            // shortcut to getting the single root
            final Path root = zip.getPath("/");
            for (Entry entry : entries) {
                final Path zipPath =
                        root.resolve(
                                zip.getPath(
                                        entry.path.get(0),
                                        entry.path
                                                .subList(1, entry.path.size())
                                                .toArray(new String[] {})));
                if (entry.isDirectory) {
                    Files.createDirectories(zipPath);
                } else {
                    Files.createDirectories(zipPath.getParent());
                    Files.write(zipPath, entry.contents.getBytes());
                }
            }
        }
        return path;
    }
}
