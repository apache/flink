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

package org.apache.flink.runtime.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClientUtils}. */
public class ClientUtilsTest {

    @TempDir private static java.nio.file.Path temporaryFolder;

    private static BlobServer blobServer = null;

    @BeforeAll
    static void setup() throws IOException {
        Configuration config = new Configuration();
        blobServer =
                new BlobServer(
                        config, TempDirUtils.newFolder(temporaryFolder), new VoidBlobStore());
        blobServer.start();
    }

    @AfterAll
    static void teardown() throws IOException {
        if (blobServer != null) {
            blobServer.close();
        }
    }

    @Test
    void uploadAndSetUserJars() throws Exception {
        java.nio.file.Path tmpDir = TempDirUtils.newFolder(temporaryFolder).toPath();
        JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();

        Collection<Path> jars =
                Arrays.asList(
                        new Path(Files.createFile(tmpDir.resolve("jar1.jar")).toString()),
                        new Path(Files.createFile(tmpDir.resolve("jar2.jar")).toString()));

        jars.forEach(jobGraph::addJar);

        assertThat(jobGraph.getUserJars()).hasSameSizeAs(jars);
        assertThat(jobGraph.getUserJarBlobKeys()).isEmpty();

        ClientUtils.extractAndUploadJobGraphFiles(
                jobGraph,
                () ->
                        new BlobClient(
                                new InetSocketAddress("localhost", blobServer.getPort()),
                                new Configuration()));

        assertThat(jobGraph.getUserJars()).hasSameSizeAs(jars);
        assertThat(jobGraph.getUserJarBlobKeys()).hasSameSizeAs(jars);
        assertThat(jobGraph.getUserJarBlobKeys().stream().distinct()).hasSameSizeAs(jars);

        for (PermanentBlobKey blobKey : jobGraph.getUserJarBlobKeys()) {
            blobServer.getFile(jobGraph.getJobID(), blobKey);
        }
    }

    @Test
    void uploadAndSetUserArtifacts() throws Exception {
        java.nio.file.Path tmpDir = TempDirUtils.newFolder(temporaryFolder).toPath();
        JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();

        Collection<DistributedCache.DistributedCacheEntry> localArtifacts =
                Arrays.asList(
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art1")).toString(), true, true),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art2")).toString(), true, false),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art3")).toString(), false, true),
                        new DistributedCache.DistributedCacheEntry(
                                Files.createFile(tmpDir.resolve("art4")).toString(), true, false));

        Collection<DistributedCache.DistributedCacheEntry> distributedArtifacts =
                Collections.singletonList(
                        new DistributedCache.DistributedCacheEntry(
                                "hdfs://localhost:1234/test", true, false));

        for (DistributedCache.DistributedCacheEntry entry : localArtifacts) {
            jobGraph.addUserArtifact(entry.filePath, entry);
        }
        for (DistributedCache.DistributedCacheEntry entry : distributedArtifacts) {
            jobGraph.addUserArtifact(entry.filePath, entry);
        }

        final int totalNumArtifacts = localArtifacts.size() + distributedArtifacts.size();

        assertThat(jobGraph.getUserArtifacts()).hasSize(totalNumArtifacts);
        assertThat(
                        jobGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey != null))
                .isEmpty();

        ClientUtils.extractAndUploadJobGraphFiles(
                jobGraph,
                () ->
                        new BlobClient(
                                new InetSocketAddress("localhost", blobServer.getPort()),
                                new Configuration()));

        assertThat(jobGraph.getUserArtifacts()).hasSize(totalNumArtifacts);
        assertThat(
                        jobGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey != null))
                .hasSameSizeAs(localArtifacts);
        assertThat(
                        jobGraph.getUserArtifacts().values().stream()
                                .filter(entry -> entry.blobKey == null))
                .hasSameSizeAs(distributedArtifacts);
        // 1 unique key for each local artifact, and null for distributed artifacts
        assertThat(
                        jobGraph.getUserArtifacts().values().stream()
                                .map(entry -> entry.blobKey)
                                .distinct())
                .hasSize(localArtifacts.size() + 1);
        for (DistributedCache.DistributedCacheEntry original : localArtifacts) {
            assertState(
                    original,
                    jobGraph.getUserArtifacts().get(original.filePath),
                    false,
                    jobGraph.getJobID());
        }
        for (DistributedCache.DistributedCacheEntry original : distributedArtifacts) {
            assertState(
                    original,
                    jobGraph.getUserArtifacts().get(original.filePath),
                    true,
                    jobGraph.getJobID());
        }
    }

    private static void assertState(
            DistributedCache.DistributedCacheEntry original,
            DistributedCache.DistributedCacheEntry actual,
            boolean isBlobKeyNull,
            JobID jobId)
            throws Exception {
        assertThat(actual.isZipped).isEqualTo(original.isZipped);
        assertThat(actual.isExecutable).isEqualTo(original.isExecutable);
        assertThat(actual.filePath).isEqualTo(original.filePath);
        assertThat(actual.blobKey == null).isEqualTo(isBlobKeyNull);
        if (!isBlobKeyNull) {
            blobServer.getFile(
                    jobId,
                    InstantiationUtil.<PermanentBlobKey>deserializeObject(
                            actual.blobKey, ClientUtilsTest.class.getClassLoader()));
        }
    }
}
