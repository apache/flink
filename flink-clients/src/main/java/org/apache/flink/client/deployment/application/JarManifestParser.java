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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.program.PackagedProgram;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import static java.util.Objects.requireNonNull;

/** Utility that parses JAR manifest attributes. */
class JarManifestParser {

    static class JarFileWithEntryClass {
        private final File jarFile;
        private final String entryClass;

        private JarFileWithEntryClass(File jarFile, String entryClass) {
            this.jarFile = requireNonNull(jarFile, "jarFile");
            this.entryClass = requireNonNull(entryClass, "entryClass");
        }

        File getJarFile() {
            return jarFile;
        }

        String getEntryClass() {
            return entryClass;
        }

        @Override
        public String toString() {
            return String.format("%s (entry class: %s)", jarFile.getAbsolutePath(), entryClass);
        }
    }

    /**
     * Returns a JAR file with its entry class as specified in the manifest.
     *
     * @param jarFiles JAR files to parse
     * @throws NoSuchElementException if no JAR file contains an entry class attribute
     * @throws IllegalArgumentException if multiple JAR files contain an entry class manifest
     *     attribute
     */
    static JarFileWithEntryClass findOnlyEntryClass(Iterable<File> jarFiles) throws IOException {
        List<JarFileWithEntryClass> jarsWithEntryClasses = new ArrayList<>();
        for (File jarFile : jarFiles) {
            findEntryClass(jarFile)
                    .ifPresent(
                            entryClass ->
                                    jarsWithEntryClasses.add(
                                            new JarFileWithEntryClass(jarFile, entryClass)));
        }
        int size = jarsWithEntryClasses.size();
        if (size == 0) {
            throw new NoSuchElementException("No JAR with manifest attribute for entry class");
        }
        if (size == 1) {
            return jarsWithEntryClasses.get(0);
        }
        // else: size > 1
        throw new IllegalArgumentException(
                "Multiple JARs with manifest attribute for entry class: " + jarsWithEntryClasses);
    }

    /**
     * Returns the entry class as specified in the manifest of the provided JAR file.
     *
     * <p>The following manifest attributes are checked in order to find the entry class:
     *
     * <ol>
     *   <li>{@link PackagedProgram#MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS}
     *   <li>{@link PackagedProgram#MANIFEST_ATTRIBUTE_MAIN_CLASS}
     * </ol>
     *
     * @param jarFile JAR file to parse
     * @return Optional holding entry class
     * @throws IOException If there is an error accessing the JAR
     */
    @VisibleForTesting
    static Optional<String> findEntryClass(File jarFile) throws IOException {
        return findFirstManifestAttribute(
                jarFile,
                PackagedProgram.MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS,
                PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS);
    }

    /**
     * Returns the value of the first manifest attribute found in the provided JAR file.
     *
     * @param jarFile JAR file to parse
     * @param attributes Attributes to check
     * @return Optional holding value of first found attribute
     * @throws IOException If there is an error accessing the JAR
     */
    private static Optional<String> findFirstManifestAttribute(File jarFile, String... attributes)
            throws IOException {
        if (attributes.length == 0) {
            return Optional.empty();
        }
        try (JarFile f = new JarFile(jarFile)) {
            return findFirstManifestAttribute(f, attributes);
        }
    }

    private static Optional<String> findFirstManifestAttribute(
            JarFile jarFile, String... attributes) throws IOException {
        Manifest manifest = jarFile.getManifest();
        if (manifest == null) {
            return Optional.empty();
        }

        Attributes mainAttributes = manifest.getMainAttributes();
        for (String attribute : attributes) {
            String value = mainAttributes.getValue(attribute);
            if (value != null) {
                return Optional.of(value);
            }
        }

        return Optional.empty();
    }
}
