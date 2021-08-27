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

import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Optional;

/**
 * {@code FromJarEntryClassInformationProvider} is used for cases where the Jar archive is
 * explicitly specified.
 */
public class FromJarEntryClassInformationProvider implements EntryClassInformationProvider {

    private final File jarFile;
    private final String jobClassName;

    /**
     * Creates a {@code FromJarEntryClassInformationProvider} for a custom Jar archive. At least the
     * {@code jarFile} or the {@code jobClassName} has to be set.
     *
     * @param jarFile The Jar archive.
     * @param jobClassName The name of the job class.
     * @return The {@code FromJarEntryClassInformationProvider} referring to the passed information.
     */
    public static FromJarEntryClassInformationProvider createFromCustomJar(
            File jarFile, @Nullable String jobClassName) {
        return new FromJarEntryClassInformationProvider(jarFile, jobClassName);
    }

    /**
     * Creates a {@code FromJarEntryClassInformationProvider} for a job implemented in Python.
     *
     * @return A {@code FromJarEntryClassInformationProvider} for a job implemented in Python
     */
    public static FromJarEntryClassInformationProvider createFromPythonJar() {
        return new FromJarEntryClassInformationProvider(
                new File(PackagedProgramUtils.getPythonJar().getPath()),
                PackagedProgramUtils.getPythonDriverClassName());
    }

    private FromJarEntryClassInformationProvider(File jarFile, @Nullable String jobClassName) {
        this.jarFile = Preconditions.checkNotNull(jarFile, "No jar archive is specified.");
        this.jobClassName = jobClassName;
    }

    /**
     * Returns the specified {@code jarFile}.
     *
     * @return The specified {@code jarFile}.
     * @see #getJobClassName()
     */
    @Override
    public Optional<File> getJarFile() {
        return Optional.of(jarFile);
    }

    /**
     * Returns the specified job class name that is either available in the corresponding {@code
     * jarFile}. It can return an empty {@code Optional} if the job class is the entry class of the
     * jar.
     *
     * @return Returns the job class that can be found in the respective {@code jarFile}. It can
     *     also return an empty {@code Optional} despite if the job class is the entry class of the
     *     jar.
     * @see #getJarFile()
     */
    @Override
    public Optional<String> getJobClassName() {
        return Optional.ofNullable(jobClassName);
    }
}
