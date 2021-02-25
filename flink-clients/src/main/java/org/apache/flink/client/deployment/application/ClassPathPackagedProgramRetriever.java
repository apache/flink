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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A classpath {@link PackagedProgramRetriever} which creates the {@link PackagedProgram} through
 * scanning the classpath for the job class.
 */
@Internal
public class ClassPathPackagedProgramRetriever extends AbstractPackagedProgramRetriever {

    private static final Logger LOG =
            LoggerFactory.getLogger(ClassPathPackagedProgramRetriever.class);

    private final Supplier<Iterable<File>> jarsOnClassPath;

    @Nullable private final File userLibDirectory;

    @Nullable private final String jobClassName;

    ClassPathPackagedProgramRetriever(
            String[] programArguments,
            Configuration configuration,
            @Nullable File userLibDirectory,
            @Nullable String jobClassName,
            @Nullable Supplier<Iterable<File>> jarsOnClassPath)
            throws IOException {
        super(programArguments, configuration, userLibDirectory);
        this.userLibDirectory = userLibDirectory;
        this.jobClassName = jobClassName;
        this.jarsOnClassPath = jarsOnClassPath == null ? JarsOnClassPath.INSTANCE : jarsOnClassPath;
    }

    @Override
    public PackagedProgram buildPackagedProgram(PackagedProgram.Builder packagedProgramBuilder)
            throws ProgramInvocationException, FlinkException {
        final String entryClass = getJobClassNameOrScanClassPath();
        return packagedProgramBuilder.setEntryPointClassName(entryClass).build();
    }

    private String getJobClassNameOrScanClassPath() throws FlinkException {
        if (jobClassName != null) {
            // check that we find the entry point class in the user lib directory.
            if (userLibDirectory != null && !userClassPathContainsJobClass(jobClassName)) {
                throw new FlinkException(
                        String.format(
                                "Could not find the provided job class (%s) in the user lib directory (%s).",
                                jobClassName, userLibDirectory));
            }
            return jobClassName;
        }

        try {
            return scanClassPathForJobJar();
        } catch (IOException | NoSuchElementException | IllegalArgumentException e) {
            throw new FlinkException(
                    "Failed to find job JAR on class path. Please provide the job class name explicitly.",
                    e);
        }
    }

    private boolean userClassPathContainsJobClass(String jobClassName) {
        for (URL userClassPath : userClassPaths) {
            try (final JarFile jarFile = new JarFile(userClassPath.getFile())) {
                if (jarContainsJobClass(jobClassName, jarFile)) {
                    return true;
                }
            } catch (IOException e) {
                ExceptionUtils.rethrow(
                        e,
                        String.format(
                                "Failed to open user class path %s. Make sure that all files on the user class path can be accessed.",
                                userClassPath));
            }
        }
        return false;
    }

    private boolean jarContainsJobClass(String jobClassName, JarFile jarFile) {
        return jarFile.stream()
                .map(JarEntry::getName)
                .filter(fileName -> fileName.endsWith(FileUtils.CLASS_FILE_EXTENSION))
                .map(FileUtils::stripFileExtension)
                .map(
                        fileName ->
                                fileName.replaceAll(
                                        Pattern.quote(File.separator), FileUtils.PACKAGE_SEPARATOR))
                .anyMatch(name -> name.equals(jobClassName));
    }

    private String scanClassPathForJobJar() throws IOException {
        final Iterable<File> jars;
        if (userLibDirectory == null) {
            LOG.info("Scanning system class path for job JAR");
            jars = jarsOnClassPath.get();
        } else {
            LOG.info("Scanning user class path for job JAR");
            jars =
                    userClassPaths.stream()
                            .map(url -> new File(url.getFile()))
                            .collect(Collectors.toList());
        }

        final JarManifestParser.JarFileWithEntryClass jobJar =
                JarManifestParser.findOnlyEntryClass(jars);
        LOG.info("Using {} as job jar", jobJar);
        return jobJar.getEntryClass();
    }

    @VisibleForTesting
    enum JarsOnClassPath implements Supplier<Iterable<File>> {
        INSTANCE;

        static final String JAVA_CLASS_PATH = "java.class.path";
        static final String PATH_SEPARATOR = "path.separator";
        static final String DEFAULT_PATH_SEPARATOR = ":";

        @Override
        public Iterable<File> get() {
            String classPath = System.getProperty(JAVA_CLASS_PATH, "");
            String pathSeparator = System.getProperty(PATH_SEPARATOR, DEFAULT_PATH_SEPARATOR);

            return Arrays.stream(classPath.split(pathSeparator))
                    .filter(JarsOnClassPath::notNullAndNotEmpty)
                    .map(File::new)
                    .filter(File::isFile)
                    .collect(Collectors.toList());
        }

        private static boolean notNullAndNotEmpty(String string) {
            return string != null && !string.equals("");
        }
    }
}
