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
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@code FromClasspathEntryClassInformationProvider} assumes the passed job class being available
 * on some classpath.
 */
public class FromClasspathEntryClassInformationProvider implements EntryClassInformationProvider {

    private final String jobClassName;

    /**
     * Creates a {@code FromClasspathEntryClassInformationProvider} based on the passed job class
     * and classpath.
     *
     * @param jobClassName The job's class name.
     * @param classpath The classpath the job class should be part of.
     * @return The {@code FromClasspathEntryClassInformationProvider} instances collecting the
     *     necessary information.
     * @throws IOException If some Jar listed on the classpath wasn't accessible.
     * @throws FlinkException If the passed job class is not present on the passed classpath.
     */
    public static FromClasspathEntryClassInformationProvider create(
            String jobClassName, Iterable<URL> classpath) throws IOException, FlinkException {
        Preconditions.checkNotNull(jobClassName, "No job class name passed.");
        Preconditions.checkNotNull(classpath, "No classpath passed.");

        return new FromClasspathEntryClassInformationProvider(jobClassName);
    }

    /**
     * Creates a {@code FromClasspathEntryClassInformationProvider} looking for the entry class
     * providing the main method on the passed classpath.
     *
     * @param classpath The classpath the job class is expected to be part of.
     * @return The {@code FromClasspathEntryClassInformationProvider} providing the job class found
     *     on the passed classpath.
     * @throws IOException If some Jar listed on the classpath wasn't accessible.
     * @throws FlinkException Either no or too many main methods were found on the classpath.
     */
    public static FromClasspathEntryClassInformationProvider createFromClasspath(
            Iterable<URL> classpath) throws IOException, FlinkException {
        return new FromClasspathEntryClassInformationProvider(
                extractJobClassFromUrlClasspath(classpath));
    }

    /**
     * Creates a {@code FromClasspathEntryClassInformationProvider} looking for the entry class
     * providing the main method on the system classpath.
     *
     * @return The {@code FromClasspathEntryClassInformationProvider} providing the job class found
     *     on the system classpath.
     * @throws IOException If some Jar listed on the system classpath wasn't accessible.
     * @throws FlinkException Either no or too many main methods were found on the system classpath.
     */
    public static FromClasspathEntryClassInformationProvider createFromSystemClasspath()
            throws IOException, FlinkException {
        return new FromClasspathEntryClassInformationProvider(extractJobClassFromSystemClasspath());
    }

    /**
     * Creates a {@code FromClasspathEntryClassInformationProvider} assuming that the passed job
     * class is available on the system classpath.
     *
     * @param jobClassName The job class name working as the entry point.
     * @return The {@code FromClasspathEntryClassInformationProvider} providing the job class found.
     */
    public static FromClasspathEntryClassInformationProvider
            createWithJobClassAssumingOnSystemClasspath(String jobClassName) {
        return new FromClasspathEntryClassInformationProvider(jobClassName);
    }

    private FromClasspathEntryClassInformationProvider(String jobClassName) {
        this.jobClassName = Preconditions.checkNotNull(jobClassName, "No job class name set.");
    }

    /**
     * Always returns an empty {@code Optional} because this implementation relies on the JAR
     * archive being available on either the user or the system classpath.
     *
     * @return An empty {@code Optional}.
     */
    @Override
    public Optional<File> getJarFile() {
        return Optional.empty();
    }

    /**
     * Returns the job class name if it could be derived from the specified classpath or was
     * explicitly specified.
     *
     * @return The job class name or an empty {@code Optional} if none was specified and it couldn't
     *     be derived from the classpath.
     */
    @Override
    public Optional<String> getJobClassName() {
        return Optional.of(jobClassName);
    }

    @VisibleForTesting
    static Iterable<File> extractSystemClasspath() {
        final String classpathPropertyValue = System.getProperty("java.class.path", "");
        final String pathSeparator = System.getProperty("path.separator", ":");

        return Arrays.stream(classpathPropertyValue.split(pathSeparator))
                .filter(entry -> !StringUtils.isNullOrWhitespaceOnly(entry))
                .map(File::new)
                .filter(File::isFile)
                .filter(f -> isJarFilename(f.getName()))
                .collect(Collectors.toList());
    }

    private static String extractJobClassFromSystemClasspath() throws FlinkException, IOException {
        return extractJobClassNameFromFileClasspath(extractSystemClasspath());
    }

    private static String extractJobClassFromUrlClasspath(Iterable<URL> classpath)
            throws IOException, FlinkException {
        final List<File> jarFilesFromClasspath =
                StreamSupport.stream(classpath.spliterator(), false)
                        .map(url -> new File(url.getFile()))
                        .filter(f -> isJarFilename(f.getName()))
                        .collect(Collectors.toList());
        return extractJobClassNameFromFileClasspath(jarFilesFromClasspath);
    }

    private static String extractJobClassNameFromFileClasspath(Iterable<File> classpath)
            throws FlinkException, IOException {
        try {
            return JarManifestParser.findOnlyEntryClass(classpath).getEntryClass();
        } catch (NoSuchElementException e) {
            throw new FlinkException(
                    "No JAR found on classpath. Please provide a JAR explicitly.", e);
        } catch (IllegalArgumentException e) {
            throw new FlinkException(
                    "Multiple JAR archives with entry classes found on classpath. Please provide an entry class name.",
                    e);
        }
    }

    private static boolean isJarFilename(String filename) {
        return filename.endsWith(".jar");
    }
}
