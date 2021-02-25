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
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

/**
 * An abstract {@link PackagedProgramRetriever} which creates the {@link PackagedProgram} containing
 * the user's {@code main()} from a class on the class path.
 */
@Internal
public abstract class AbstractPackagedProgramRetriever implements PackagedProgramRetriever {

    private final String[] programArguments;

    private final Configuration configuration;

    /** User class paths in relative form to the working directory. */
    protected final Collection<URL> userClassPaths;

    AbstractPackagedProgramRetriever(
            String[] programArguments, Configuration configuration, @Nullable File userLibDirectory)
            throws IOException {
        this.programArguments = Preconditions.checkNotNull(programArguments);
        this.configuration = Preconditions.checkNotNull(configuration);
        this.userClassPaths = discoverUserClassPaths(userLibDirectory);
    }

    private static Collection<URL> discoverUserClassPaths(@Nullable File jobDir)
            throws IOException {
        if (jobDir == null) {
            return Collections.emptyList();
        }

        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        final Collection<URL> relativeJarURLs =
                FileUtils.listFilesInDirectory(jobDir.toPath(), FileUtils::isJarFile).stream()
                        .map(path -> FileUtils.relativizePath(workingDirectory, path))
                        .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                        .collect(Collectors.toList());
        return Collections.unmodifiableCollection(relativeJarURLs);
    }

    @Override
    public PackagedProgram getPackagedProgram() throws FlinkException {
        try {
            PackagedProgram.Builder packagedProgramBuilder =
                    PackagedProgram.newBuilder()
                            .setArguments(programArguments)
                            .setConfiguration(configuration)
                            .setUserClassPaths(new ArrayList<>(userClassPaths));
            return buildPackagedProgram(packagedProgramBuilder);
        } catch (ProgramInvocationException e) {
            throw new FlinkException("Could not load the provided entry point class.", e);
        }
    }

    protected abstract PackagedProgram buildPackagedProgram(
            PackagedProgram.Builder packagedProgramBuilder)
            throws ProgramInvocationException, FlinkException;
}
