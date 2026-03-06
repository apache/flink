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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.jobmanager.ApplicationStoreEntry;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;

/** {@code ApplicationStoreEntry} for {@code PackagedProgramApplication}. */
public class PackagedProgramApplicationEntry implements ApplicationStoreEntry {

    private final Configuration configuration;

    private final PermanentBlobKey userJarBlobKey;

    private final String entryClass;

    private final String[] programArgs;

    private final ApplicationID applicationId;

    private final String applicationName;

    private final boolean handleFatalError;

    private final boolean enforceSingleJobExecution;

    private final boolean submitFailedJobOnApplicationError;

    private final boolean shutDownOnFinish;

    public PackagedProgramApplicationEntry(
            Configuration configuration,
            PermanentBlobKey userJarBlobKey,
            String entryClass,
            String[] programArgs,
            ApplicationID applicationId,
            String applicationName,
            boolean handleFatalError,
            boolean enforceSingleJobExecution,
            boolean submitFailedJobOnApplicationError,
            boolean shutDownOnFinish) {
        this.configuration = configuration;
        this.userJarBlobKey = userJarBlobKey;
        this.entryClass = entryClass;
        this.programArgs = programArgs;
        this.applicationId = applicationId;
        this.applicationName = applicationName;
        this.handleFatalError = handleFatalError;
        this.enforceSingleJobExecution = enforceSingleJobExecution;
        this.submitFailedJobOnApplicationError = submitFailedJobOnApplicationError;
        this.shutDownOnFinish = shutDownOnFinish;
    }

    @Override
    public AbstractApplication getApplication(
            PermanentBlobService blobService,
            Collection<JobInfo> recoveredJobInfos,
            Collection<JobInfo> recoveredTerminalJobInfos) {
        File jarFile;
        try {
            jarFile = blobService.getFile(applicationId, userJarBlobKey);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get user jar file from blob", e);
        }

        if (!jarFile.exists()) {
            throw new RuntimeException(String.format("Jar file %s does not exist", jarFile));
        }

        PackagedProgram program;
        try {
            program =
                    PackagedProgram.newBuilder()
                            .setJarFile(jarFile)
                            .setEntryPointClassName(entryClass)
                            .setConfiguration(configuration)
                            .setUserClassPaths(getClasspaths())
                            .setArguments(programArgs)
                            .build();
        } catch (Exception e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to create PackagedProgram for application %s", applicationId));
        }

        return new PackagedProgramApplication(
                applicationId,
                program,
                recoveredJobInfos,
                recoveredTerminalJobInfos,
                configuration,
                handleFatalError,
                enforceSingleJobExecution,
                submitFailedJobOnApplicationError,
                shutDownOnFinish);
    }

    private List<URL> getClasspaths() {
        try {
            return ConfigUtils.decodeListFromConfig(
                    configuration, PipelineOptions.CLASSPATHS, URL::new);
        } catch (MalformedURLException e) {
            throw new RuntimeException(
                    String.format(
                            "Failed to extract '%s' as URLs. Provided value: %s",
                            PipelineOptions.CLASSPATHS.key(),
                            configuration.get(PipelineOptions.CLASSPATHS)));
        }
    }

    @Override
    public ApplicationID getApplicationId() {
        return applicationId;
    }

    @Override
    public String getName() {
        return applicationName;
    }
}
