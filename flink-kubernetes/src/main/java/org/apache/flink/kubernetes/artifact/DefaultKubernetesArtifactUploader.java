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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Default {@link KubernetesArtifactUploader} implementation. */
public class DefaultKubernetesArtifactUploader implements KubernetesArtifactUploader {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultKubernetesArtifactUploader.class);

    @Override
    public void uploadAll(Configuration config) throws Exception {
        if (!config.get(KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED)) {
            LOG.info(
                    "Local artifact uploading is disabled. Set '{}' to enable.",
                    KubernetesConfigOptions.LOCAL_UPLOAD_ENABLED.key());
            return;
        }

        final String jobUri = upload(config, getJobUri(config));
        updateConfig(config, PipelineOptions.JARS, Collections.singletonList(jobUri));

        final List<String> additionalUris =
                config.getOptional(ArtifactFetchOptions.ARTIFACT_LIST)
                        .orElse(Collections.emptyList());

        final List<String> uploadedAdditionalUris =
                additionalUris.stream()
                        .map(
                                FunctionUtils.uncheckedFunction(
                                        artifactUri -> upload(config, artifactUri)))
                        .collect(Collectors.toList());

        updateConfig(config, ArtifactFetchOptions.ARTIFACT_LIST, uploadedAdditionalUris);
    }

    @VisibleForTesting
    String upload(Configuration config, String artifactUriStr)
            throws IOException, URISyntaxException {
        final URI artifactUri = PackagedProgramUtils.resolveURI(artifactUriStr);
        if (!"local".equals(artifactUri.getScheme())) {
            return artifactUriStr;
        }

        final String targetDir = config.get(KubernetesConfigOptions.LOCAL_UPLOAD_TARGET);
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(targetDir),
                String.format(
                        "Setting '%s' to a valid remote path is required.",
                        KubernetesConfigOptions.LOCAL_UPLOAD_TARGET.key()));

        final FileSystem.WriteMode writeMode =
                config.get(KubernetesConfigOptions.LOCAL_UPLOAD_OVERWRITE)
                        ? FileSystem.WriteMode.OVERWRITE
                        : FileSystem.WriteMode.NO_OVERWRITE;

        final File src = new File(artifactUri.getPath());
        final Path target = new Path(targetDir, src.getName());
        if (target.getFileSystem().exists(target)
                && writeMode == FileSystem.WriteMode.NO_OVERWRITE) {
            LOG.info(
                    "Skip uploading artifact '{}', as it already exists."
                            + " To overwrite existing artifacts, please set the '{}' config option.",
                    target,
                    KubernetesConfigOptions.LOCAL_UPLOAD_OVERWRITE.key());
        } else {
            final long start = System.currentTimeMillis();
            final FileSystem fs = target.getFileSystem();
            try (FSDataOutputStream os = fs.create(target, writeMode)) {
                FileUtils.copyFile(src, os);
            }
            LOG.debug(
                    "Copied file from {} to {}, cost {} ms",
                    src,
                    target,
                    System.currentTimeMillis() - start);
        }

        return target.toString();
    }

    @VisibleForTesting
    void updateConfig(
            Configuration config, ConfigOption<List<String>> configOption, List<String> newValue) {
        final List<String> originalValue =
                config.getOptional(configOption).orElse(Collections.emptyList());

        if (hasLocal(originalValue)) {
            LOG.info(
                    "Updating configuration '{}' after to replace local artifact: '{}'",
                    configOption.key(),
                    newValue);
            config.set(configOption, newValue);
        }
    }

    private String getJobUri(Configuration config) {
        final List<String> jars = config.get(PipelineOptions.JARS);
        checkArgument(
                jars.size() == 1,
                String.format("The '%s' config must contain one JAR.", PipelineOptions.JARS.key()));

        return config.get(PipelineOptions.JARS).get(0);
    }

    private boolean hasLocal(List<String> originalUris) {
        return originalUris.stream().anyMatch(uri -> uri.contains("local:/"));
    }
}
