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

import org.apache.flink.annotation.Internal;
import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;

/** Local artifact uploader for Kubernetes programs. */
@Internal
public interface KubernetesArtifactUploader {

    /**
     * Uploads all {@code local://} schemed artifact that is present, according to the given
     * configuration. Any remote artifact remains as it was passed originally.
     *
     * <p>Takes the job JAR from the {@link PipelineOptions#JARS} config and any additional
     * artifacts from the {@link ArtifactFetchOptions#ARTIFACT_LIST} config. After the upload,
     * replaces the URIs of any local JAR in these configs to point to the remotely available one.
     *
     * <p>Requires {@link KubernetesConfigOptions#LOCAL_UPLOAD_TARGET} to point to a valid, existing
     * DFS directory with read and write permissions.
     *
     * @param config given Flink configuration
     * @throws Exception when the upload process fails
     */
    void uploadAll(Configuration config) throws Exception;
}
