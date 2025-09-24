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

package org.apache.flink.client.program;

import org.apache.flink.client.deployment.application.PackagedProgramApplication;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import javax.annotation.Nullable;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

/**
 * Descriptor for {@link PackagedProgram}.
 *
 * <p>This class provides a serializable representation of {@link PackagedProgram} that can be used
 * to reconstruct the {@link PackagedProgram} after serialization and deserialization. It is mainly
 * used by {@link PackagedProgramApplication}.
 */
public class PackagedProgramDescriptor implements Serializable {

    @Nullable private final File jarFile;

    private final List<URL> userClassPaths;

    private final Configuration configuration;

    private final SavepointRestoreSettings savepointRestoreSettings;

    private final String[] programArgs;

    private final String mainClassName;

    public PackagedProgramDescriptor(
            @Nullable File jarFile,
            List<URL> userClassPaths,
            Configuration configuration,
            SavepointRestoreSettings savepointRestoreSettings,
            String[] programArgs,
            String mainClassName) {
        this.jarFile = jarFile;
        this.userClassPaths = userClassPaths;
        this.configuration = configuration;
        this.savepointRestoreSettings = savepointRestoreSettings;
        this.programArgs = programArgs;
        this.mainClassName = mainClassName;
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public PackagedProgram toPackageProgram() throws ProgramInvocationException {
        return PackagedProgram.newBuilder()
                .setJarFile(jarFile)
                .setEntryPointClassName(mainClassName)
                .setConfiguration(configuration)
                .setUserClassPaths(userClassPaths)
                .setArguments(programArgs)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();
    }

    @Override
    public String toString() {
        return "PackagedProgramDescriptor{"
                + "jarFile="
                + jarFile
                + ", userClassPaths="
                + userClassPaths
                + ", configuration="
                + configuration
                + ", savepointRestoreSettings="
                + savepointRestoreSettings
                + ", programArgs="
                + Arrays.toString(programArgs)
                + ", mainClassName="
                + mainClassName
                + '}';
    }
}
