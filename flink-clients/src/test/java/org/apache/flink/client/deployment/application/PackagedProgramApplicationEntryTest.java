/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not file except in compliance
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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.api.common.JobInfoImpl;
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.program.JarInfo;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.testjar.MultiExecuteJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobmanager.ApplicationStoreEntry;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Tests for the {@link PackagedProgramApplicationEntry}. */
class PackagedProgramApplicationEntryTest {

    @TempDir private static Path temporaryFolder;

    private static BlobServer blobServer;
    private static Configuration config;
    private static ApplicationID applicationId;
    private static String applicationName;
    private static File jarFile;
    private static String entryClass;
    private static String[] programArgs;
    private static PackagedProgram program;

    @BeforeAll
    static void setup() throws Exception {
        config = new Configuration();
        blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();

        applicationId = new ApplicationID();
        applicationName = MultiExecuteJob.class.getCanonicalName();
        entryClass = MultiExecuteJob.class.getName();
        programArgs = new String[] {"1", "true"};
        jarFile = new File(CliFrontendTestUtils.getTestJarPath());
        program =
                PackagedProgram.newBuilder()
                        .setJarFile(jarFile)
                        .setEntryPointClassName(entryClass)
                        .setConfiguration(config)
                        .setArguments(programArgs)
                        .build();
    }

    @Test
    void testConstructionWithoutJarBlob() {
        PackagedProgramApplication application =
                new PackagedProgramApplication(
                        applicationId, program, config, true, false, false, true);
        ApplicationStoreEntry entry = application.getApplicationStoreEntry().orElse(null);

        assertNull(entry);
    }

    @Test
    void testConstruction() throws Exception {
        PermanentBlobKey blobKey;
        try (InputStream is = new FileInputStream(jarFile)) {
            blobKey = blobServer.putPermanent(applicationId, is);
        }

        JarInfo userJarInfo = new JarInfo(jarFile.getName(), blobKey);
        PackagedProgramApplication application =
                new PackagedProgramApplication(
                        applicationId, program, config, true, false, false, true, userJarInfo);
        ApplicationStoreEntry entry = application.getApplicationStoreEntry().orElse(null);

        assertInstanceOf(PackagedProgramApplicationEntry.class, entry);
        assertEquals(applicationId, entry.getApplicationId());
        assertEquals(applicationName, entry.getName());

        JobInfo recoveredJob = new JobInfoImpl(new JobID(), "test");
        JobInfo recoveredTerminalJob = new JobInfoImpl(new JobID(), "test");
        AbstractApplication reconstructed =
                entry.getApplication(
                        blobServer,
                        Collections.singleton(recoveredJob),
                        Collections.singleton(recoveredTerminalJob));

        assertInstanceOf(PackagedProgramApplication.class, reconstructed);

        PackagedProgramApplication packagedProgramApplication =
                (PackagedProgramApplication) reconstructed;

        assertEquals(blobKey, packagedProgramApplication.getUserJarInfo().getJarBlobKey());
        assertThat(
                        packagedProgramApplication.getRecoveredJobInfos().stream()
                                .map(JobInfo::getJobId)
                                .collect(Collectors.toList()))
                .containsExactly(recoveredJob.getJobId());
        assertThat(
                        packagedProgramApplication.getRecoveredTerminalJobInfos().stream()
                                .map(JobInfo::getJobId)
                                .collect(Collectors.toList()))
                .containsExactly(recoveredTerminalJob.getJobId());
    }

    @Test
    void testConstructionFailsWithInvalidJar() throws Exception {
        PermanentBlobKey blobKey = blobServer.putPermanent(applicationId, new byte[] {1, 2, 3});

        PackagedProgramApplicationEntry entry = createEntryWithJarBlob(blobKey);

        assertThatThrownBy(
                        () ->
                                entry.getApplication(
                                        blobServer,
                                        Collections.emptyList(),
                                        Collections.emptyList()))
                .hasMessageContaining("Failed to create PackagedProgram for application");
    }

    @Test
    void testConstructionFailsWithMissingBlob() {
        PermanentBlobKey blobKey = new PermanentBlobKey();

        PackagedProgramApplicationEntry entry = createEntryWithJarBlob(blobKey);

        assertThatThrownBy(
                        () ->
                                entry.getApplication(
                                        blobServer,
                                        Collections.emptyList(),
                                        Collections.emptyList()))
                .hasMessageContaining("Failed to get user jar file from blob");
    }

    private PackagedProgramApplicationEntry createEntryWithJarBlob(PermanentBlobKey blobKey) {
        return new PackagedProgramApplicationEntry(
                config,
                new JarInfo(jarFile.getName(), blobKey),
                entryClass,
                programArgs,
                applicationId,
                applicationName,
                true,
                false,
                false,
                true);
    }
}
