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

package org.apache.flink.client.testjar;

import org.apache.flink.client.deployment.application.JarManifestParser;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * {@code ClasspathProvider} offers utility methods for creating a classpath based on actual jars.
 */
public class ClasspathProvider extends ExternalResource {

    private static final String CLASSPATH_PROPERTY_NAME = "java.class.path";

    private static final Path TEST_JOB_JAR_PATH = Paths.get("target", "maven-test-jar.jar");

    private static final Path JOB_JAR_PATH =
            Paths.get("target", "maven-test-user-classloader-job-jar.jar");
    private static final Path JOB_LIB_JAR_PATH =
            Paths.get("target", "maven-test-user-classloader-job-lib-jar.jar");

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String directoryNameSuffix;
    private final ThrowingConsumer<File, IOException> directoryContentCreator;
    @Nullable private final String jobClassName;
    @Nullable private final File jarFile;

    private File directory;

    private final String originalSystemClasspath;

    public static ClasspathProvider createWithNoEntryClass() {
        return new ClasspathProvider(
                "_user_dir_with_no_entry_class",
                directory -> {
                    copyJar(JOB_LIB_JAR_PATH, directory);
                    createTestFile(directory);
                });
    }

    public static ClasspathProvider createWithSingleEntryClass() {
        return new ClasspathProvider(
                "_user_dir_with_single_entry_class",
                directory -> {
                    copyJar(JOB_LIB_JAR_PATH, directory);
                    copyJar(JOB_JAR_PATH, directory);
                    createTestFile(directory);
                },
                JOB_JAR_PATH.toFile());
    }

    public static ClasspathProvider createWithMultipleEntryClasses() {
        return new ClasspathProvider(
                "_user_dir_with_multiple_entry_classes",
                directory -> {
                    copyJar(JOB_LIB_JAR_PATH, directory);
                    // first jar with main method
                    copyJar(JOB_JAR_PATH, directory);
                    // second jar with main method
                    copyJar(TEST_JOB_JAR_PATH, directory);
                    createTestFile(directory);
                },
                TEST_JOB_JAR_PATH.toFile());
    }

    public static ClasspathProvider createWithTestJobOnly() {
        return new ClasspathProvider(
                "_user_dir_with_testjob_entry_class_only",
                directory -> copyJar(TEST_JOB_JAR_PATH, directory),
                TEST_JOB_JAR_PATH.toFile());
    }

    public static String[] parametersForTestJob(String strValue) {
        return new String[] {"--arg", strValue};
    }

    public static ClasspathProvider createWithTextFileOnly() {
        return new ClasspathProvider(
                "_user_dir_with_text_file_only", ClasspathProvider::createTestFile);
    }

    private static void copyJar(Path sourcePath, File targetDir) throws IOException {
        Files.copy(sourcePath, targetDir.toPath().resolve(sourcePath.toFile().getName()));
    }

    private static void createTestFile(File targetDir) throws IOException {
        Files.createFile(targetDir.toPath().resolve("test.txt"));
    }

    private ClasspathProvider(
            String directoryNameSuffix,
            ThrowingConsumer<File, IOException> directoryContentCreator) {
        this(directoryNameSuffix, directoryContentCreator, null, null);
    }

    private ClasspathProvider(
            String directoryNameSuffix,
            ThrowingConsumer<File, IOException> directoryContentCreator,
            File jarFile) {
        this(
                directoryNameSuffix,
                directoryContentCreator,
                jarFile,
                extractEntryClassNameFromJar(jarFile));
    }

    private ClasspathProvider(
            String directoryNameSuffix,
            ThrowingConsumer<File, IOException> directoryContentCreator,
            @Nullable File jarFile,
            @Nullable String jobClassName) {
        this.directoryNameSuffix =
                Preconditions.checkNotNull(directoryNameSuffix, "No directory specified.");
        this.directoryContentCreator =
                Preconditions.checkNotNull(
                        directoryContentCreator, "No logic for filling the directory specified.");
        this.jarFile = jarFile;
        this.jobClassName = jobClassName;

        this.originalSystemClasspath = System.getProperty(CLASSPATH_PROPERTY_NAME);
    }

    @Override
    public void before() throws IOException {
        temporaryFolder.create();

        directory = temporaryFolder.newFolder(directoryNameSuffix);
        directoryContentCreator.accept(directory);
    }

    @Override
    protected void after() {
        temporaryFolder.delete();
        resetSystemClasspath();
    }

    @Nullable
    public String getJobClassName() {
        return jobClassName;
    }

    public File getJobJar() {
        if (jarFile == null) {
            throw new UnsupportedOperationException(
                    "There's no job jar specified for " + directory.getName());
        }

        return jarFile;
    }

    private static String extractEntryClassNameFromJar(File f) {
        try {
            return JarManifestParser.findFirstManifestAttribute(
                            f, PackagedProgram.MANIFEST_ATTRIBUTE_MAIN_CLASS)
                    .orElseThrow(
                            () ->
                                    new IllegalArgumentException(
                                            "The passed file does not contain a main class: "
                                                    + f.getAbsolutePath()));
        } catch (Throwable t) {
            throw new AssertionError(
                    "Something went wrong with retrieving the main class from "
                            + f.getAbsolutePath(),
                    t);
        }
    }

    public File getDirectory() {
        return directory;
    }

    public Iterable<URL> getURLUserClasspath() throws MalformedURLException {
        List<URL> list = new ArrayList<>();
        for (File file : getFileUserClasspath(getDirectory())) {
            list.add(file.toURI().toURL());
        }
        return list;
    }

    public void setSystemClasspath() throws MalformedURLException {
        final String classpathStr = generateClasspathString(getURLUserClasspath());
        System.setProperty(CLASSPATH_PROPERTY_NAME, classpathStr);
    }

    public void resetSystemClasspath() {
        System.setProperty(CLASSPATH_PROPERTY_NAME, originalSystemClasspath);
    }

    private static String generateClasspathString(Iterable<URL> classpath) {
        final String pathSeparator = System.getProperty("path.separator");
        return StreamSupport.stream(classpath.spliterator(), false)
                .map(URL::toString)
                .collect(Collectors.joining(pathSeparator));
    }

    private static List<File> getFileUserClasspath(File parentFolder) {
        return Arrays.asList(Objects.requireNonNull(parentFolder.listFiles()));
    }
}
