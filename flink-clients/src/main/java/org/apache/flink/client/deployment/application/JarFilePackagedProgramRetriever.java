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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;

/**
 * A jar file {@link PackagedProgramRetriever} which creates the {@link PackagedProgram} with the
 * specified jar file.
 */
@Internal
public class JarFilePackagedProgramRetriever extends AbstractPackagedProgramRetriever {

    private final File jarFile;

    @Nullable private final String jobClassName;

    JarFilePackagedProgramRetriever(
            String[] programArguments,
            Configuration configuration,
            @Nullable File userLibDirectory,
            @Nullable String jobClassName,
            File jarFile)
            throws IOException {
        super(programArguments, configuration, userLibDirectory);
        this.jobClassName = jobClassName;
        this.jarFile = Preconditions.checkNotNull(jarFile);
    }

    @Override
    public PackagedProgram buildPackagedProgram(PackagedProgram.Builder packagedProgramBuilder)
            throws ProgramInvocationException {
        return packagedProgramBuilder
                .setJarFile(jarFile)
                .setEntryPointClassName(jobClassName)
                .build();
    }
}
