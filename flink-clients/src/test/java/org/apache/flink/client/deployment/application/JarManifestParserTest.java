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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.testjar.TestJob;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for JAR file manifest parsing. */
public class JarManifestParserTest {

    @TempDir java.nio.file.Path temporaryFolder;

    @Test
    void testFindEntryClassNoEntry() throws IOException {
        File jarFile = createJarFileWithManifest(ImmutableMap.of());

        Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

        assertThat(entry).isNotPresent();
    }

    @Test
    void testFindEntryClassAssemblerClass() throws IOException {
        File jarFile =
                createJarFileWithManifest(
                        ImmutableMap.of(
                                PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS,
                                "AssemblerClass"));

        Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

        assertThat(entry).get().isEqualTo("AssemblerClass");
    }

    @Test
    void testFindEntryClassMainClass() throws IOException {
        File jarFile =
                createJarFileWithManifest(
                        ImmutableMap.of(
                                PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS, "MainClass"));

        Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

        assertThat(entry).get().isEqualTo("MainClass");
    }

    @Test
    void testFindEntryClassAssemblerClassAndMainClass() throws IOException {
        // We want the assembler class entry to have precedence over main class
        File jarFile =
                createJarFileWithManifest(
                        ImmutableMap.of(
                                PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS,
                                "AssemblerClass",
                                PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS,
                                "MainClass"));

        Optional<String> entry = JarManifestParser.findEntryClass(jarFile);

        assertThat(entry).isPresent().get().isEqualTo("AssemblerClass");
    }

    @Test
    void testFindEntryClassWithTestJobJar() throws IOException {
        File jarFile = TestJob.getTestJobJar();

        Optional<String> entryClass = JarManifestParser.findEntryClass(jarFile);

        assertThat(entryClass).get().isEqualTo(TestJob.class.getCanonicalName());
    }

    @Test
    void testFindOnlyEntryClassEmptyArgument() {
        assertThatThrownBy(() -> JarManifestParser.findOnlyEntryClass(Collections.emptyList()))
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testFindOnlyEntryClassSingleJarWithNoManifest() {
        assertThatThrownBy(
                        () -> {
                            File jarWithNoManifest = createJarFileWithManifest(ImmutableMap.of());
                            JarManifestParser.findOnlyEntryClass(
                                    ImmutableList.of(jarWithNoManifest));
                        })
                .isInstanceOf(NoSuchElementException.class);
    }

    @Test
    void testFindOnlyEntryClassSingleJar() throws IOException {
        File jarFile = TestJob.getTestJobJar();

        JarManifestParser.JarFileWithEntryClass jarFileWithEntryClass =
                JarManifestParser.findOnlyEntryClass(ImmutableList.of(jarFile));

        assertThat(jarFileWithEntryClass.getEntryClass())
                .isEqualTo(TestJob.class.getCanonicalName());
    }

    @Test
    void testFindOnlyEntryClassMultipleJarsWithMultipleManifestEntries() throws IOException {
        assertThatThrownBy(
                        () -> {
                            File jarFile = TestJob.getTestJobJar();
                            JarManifestParser.findOnlyEntryClass(
                                    ImmutableList.of(jarFile, jarFile, jarFile));
                        })
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFindOnlyEntryClassMultipleJarsWithSingleManifestEntry() throws IOException {
        File jarWithNoManifest = createJarFileWithManifest(ImmutableMap.of());
        File jarFile = TestJob.getTestJobJar();

        JarManifestParser.JarFileWithEntryClass jarFileWithEntryClass =
                JarManifestParser.findOnlyEntryClass(ImmutableList.of(jarWithNoManifest, jarFile));

        assertThat(jarFileWithEntryClass.getEntryClass())
                .isEqualTo(TestJob.class.getCanonicalName());
    }

    @Test
    void testFindFirstManifestAttributeWithNoAttribute() throws IOException {
        assertThat(JarManifestParser.findFirstManifestAttribute(TestJob.getTestJobJar())).isEmpty();
    }

    @Test
    void testFindFirstManifestAttributeWithAttributes() throws IOException {
        Optional<String> optionalValue =
                JarManifestParser.findFirstManifestAttribute(
                        TestJob.getTestJobJar(), PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS);

        assertThat(optionalValue).get().isEqualTo("org.apache.flink.client.testjar.TestJob");
    }

    private File createJarFileWithManifest(Map<String, String> manifest) throws IOException {
        final File jarFile = temporaryFolder.resolve("test.jar").toFile();
        try (ZipOutputStream zos =
                        new ZipOutputStream(
                                new BufferedOutputStream(new FileOutputStream(jarFile)));
                PrintWriter pw = new PrintWriter(zos)) {
            zos.putNextEntry(new ZipEntry("META-INF/MANIFEST.MF"));
            manifest.forEach((key, value) -> pw.println(String.format("%s: %s", key, value)));
        }
        return jarFile;
    }
}
