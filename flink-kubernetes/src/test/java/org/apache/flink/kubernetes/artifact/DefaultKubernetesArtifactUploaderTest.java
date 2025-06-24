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

package org.apache.flink.kubernetes.artifact;

import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.testutils.TestingUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultKubernetesArtifactUploader}. */
class DefaultKubernetesArtifactUploaderTest {

    private final DefaultKubernetesArtifactUploader artifactUploader =
            new DefaultKubernetesArtifactUploader();

    @TempDir private Path tmpDir;

    private Configuration config;

    private DummyFs dummyFs;

    @BeforeEach
    void setup() throws IOException {
        config = new Configuration();
        config.set(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED, true);
        config.set(KubernetesConfigOptions.LOCAL_UPLOAD_TARGET, getTargetDirUri());

        dummyFs = (DummyFs) new org.apache.flink.core.fs.Path(getTargetDirUri()).getFileSystem();
        dummyFs.resetCallCounters();
    }

    @Test
    void testInvalidJobJar() {
        String msg = "The 'pipeline.jars' config must contain one JAR.";

        config.set(PipelineOptions.JARS, Collections.emptyList());
        assertThatThrownBy(() -> artifactUploader.uploadAll(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(msg);

        config.set(PipelineOptions.JARS, Arrays.asList("a", "b"));
        assertThatThrownBy(() -> artifactUploader.uploadAll(config))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(msg);
    }

    @Test
    void testUploadAllWithOneJobJar() throws Exception {
        File jar = getFlinkKubernetesJar();
        String localUri = "local://" + jar.getAbsolutePath();

        config.set(PipelineOptions.JARS, Collections.singletonList(localUri));
        artifactUploader.uploadAll(config);

        assertJobJarUri(jar.getName());
    }

    @Test
    void testUploadAllWithAdditionalArtifacts() throws Exception {
        File jobJar = getFlinkKubernetesJar();
        File addArtifact1 = TestingUtils.getClassFile(DefaultKubernetesArtifactUploader.class);
        File addArtifact2 = TestingUtils.getClassFile(KubernetesUtils.class);
        String localJobUri = "local://" + jobJar.getAbsolutePath();
        String localAddArtUri = "local://" + addArtifact1.getAbsolutePath();
        String nonLocalAddArtUri = "dummyfs://" + addArtifact2.getAbsolutePath();

        config.set(PipelineOptions.JARS, Collections.singletonList(localJobUri));
        config.set(
                ArtifactFetchOptions.ARTIFACT_LIST,
                Arrays.asList(nonLocalAddArtUri, localAddArtUri));
        artifactUploader.uploadAll(config);

        assertJobJarUri(jobJar.getName());

        List<String> additionalArtifactsResult = config.get(ArtifactFetchOptions.ARTIFACT_LIST);
        assertThat(additionalArtifactsResult).hasSize(2);
        assertThat(additionalArtifactsResult)
                .containsExactlyInAnyOrder(
                        nonLocalAddArtUri, "dummyfs:" + tmpDir.resolve(addArtifact1.getName()));
    }

    @Test
    void testMissingTargetConf() {
        config.removeConfig(KubernetesConfigOptions.LOCAL_UPLOAD_TARGET);

        assertThatThrownBy(() -> artifactUploader.upload(config, "local:///tmp/my-artifact.jar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Setting 'kubernetes.artifacts.local-upload-target' to a valid remote path is required.");
    }

    @Test
    void testRemoteUri() throws Exception {
        config.removeConfig(KubernetesConfigOptions.LOCAL_UPLOAD_TARGET);
        String remoteUri = "s3://my-bucket/my-artifact.jar";

        String finalUri = artifactUploader.upload(config, remoteUri);

        assertThat(finalUri).isEqualTo(remoteUri);
    }

    @Test
    void testUpload() throws Exception {
        File jar = getFlinkKubernetesJar();
        String localUri = "local://" + jar.getAbsolutePath();

        String expectedUri = "dummyfs:" + tmpDir.resolve(jar.getName());
        String resultUri = artifactUploader.upload(config, localUri);

        assertThat(resultUri).isEqualTo(expectedUri);
    }

    @Test
    void testUploadNoOverwrite() throws Exception {
        File jar = getFlinkKubernetesJar();
        String localUri = "local://" + jar.getAbsolutePath();
        Files.createFile(tmpDir.resolve(jar.getName()));

        artifactUploader.upload(config, localUri);

        assertThat(dummyFs.getExistsCallCounter()).isOne();
        assertThat(dummyFs.getCreateCallCounter()).isZero();
    }

    @Test
    void testUploadOverwrite() throws Exception {
        File jar = getFlinkKubernetesJar();
        String localUri = "local://" + jar.getAbsolutePath();
        Files.createFile(tmpDir.resolve(jar.getName()));

        config.set(KubernetesConfigOptions.LOCAL_UPLOAD_OVERWRITE, true);
        artifactUploader.upload(config, localUri);

        assertThat(dummyFs.getExistsCallCounter()).isEqualTo(2);
        assertThat(dummyFs.getCreateCallCounter()).isOne();
    }

    @Test
    void testUpdateConfig() {
        List<String> artifactList =
                Arrays.asList("local:///tmp/artifact1.jar", "s3://my-bucket/artifact2.jar");
        Configuration config = new Configuration();
        config.set(ArtifactFetchOptions.ARTIFACT_LIST, artifactList);

        List<String> uploadedArtifactList = new ArrayList<>(artifactList);
        uploadedArtifactList.set(0, getTargetDirUri() + "/artifact1.jar");
        artifactUploader.updateConfig(
                config, ArtifactFetchOptions.ARTIFACT_LIST, uploadedArtifactList);

        assertThat(config.get(ArtifactFetchOptions.ARTIFACT_LIST)).isEqualTo(uploadedArtifactList);
    }

    @Test
    void testNoUpdateConfig() {
        List<String> artifactList = Collections.singletonList("s3://my-bucket/my-artifact.jar");
        Configuration config = new Configuration();
        config.set(ArtifactFetchOptions.ARTIFACT_LIST, artifactList);

        artifactUploader.updateConfig(config, ArtifactFetchOptions.ARTIFACT_LIST, artifactList);

        assertThat(config.get(ArtifactFetchOptions.ARTIFACT_LIST)).isEqualTo(artifactList);
    }

    private String getTargetDirUri() {
        return "dummyfs://" + tmpDir;
    }

    private File getFlinkKubernetesJar() throws IOException {
        return TestingUtils.getFileFromTargetDir(
                DefaultKubernetesArtifactUploader.class,
                p ->
                        org.apache.flink.util.FileUtils.isJarFile(p)
                                && p.toFile().getName().startsWith("flink-kubernetes"));
    }

    private void assertJobJarUri(String filename) {
        String expectedUri = "dummyfs:" + tmpDir.resolve(filename);

        List<String> result = config.get(PipelineOptions.JARS);
        assertThat(result).hasSize(1);
        assertThat(result.get(0)).isEqualTo(expectedUri);
    }
}
