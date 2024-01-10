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

package org.apache.flink.client.program.artifact;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.function.FunctionUtils;

import org.apache.commons.io.FilenameUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class that manages the artifact loading process. */
public class ArtifactFetchManager {

    private final ArtifactFetcher localFetcher;
    private final ArtifactFetcher fsFetcher;
    private final ArtifactFetcher httpFetcher;

    private final Configuration conf;
    private final File baseDir;

    public ArtifactFetchManager(Configuration conf) {
        this(conf, null);
    }

    public ArtifactFetchManager(Configuration conf, @Nullable String baseDir) {
        this(
                new LocalArtifactFetcher(),
                new FsArtifactFetcher(),
                new HttpArtifactFetcher(),
                conf,
                baseDir);
    }

    @VisibleForTesting
    ArtifactFetchManager(
            ArtifactFetcher localFetcher,
            ArtifactFetcher fsFetcher,
            ArtifactFetcher httpFetcher,
            Configuration conf,
            @Nullable String baseDir) {
        this.localFetcher = checkNotNull(localFetcher);
        this.fsFetcher = checkNotNull(fsFetcher);
        this.httpFetcher = checkNotNull(httpFetcher);
        this.conf = checkNotNull(conf);
        this.baseDir =
                baseDir == null
                        ? new File(conf.get(ArtifactFetchOptions.BASE_DIR))
                        : new File(baseDir);
    }

    /**
     * Fetches artifacts from a given URI string array. The job jar and any additional artifacts are
     * mixed, in case of multiple artifacts the {@link DefaultPackagedProgramRetriever} logic will
     * be used to find the job jar.
     *
     * @param uris URIs to fetch
     * @return result with the fetched artifacts
     */
    public Result fetchArtifacts(String[] uris) {
        checkArgument(uris != null && uris.length > 0, "At least one URI is required.");

        ArtifactUtils.createMissingParents(baseDir);
        List<File> artifacts =
                Arrays.stream(uris)
                        .map(FunctionUtils.uncheckedFunction(this::fetchArtifact))
                        .collect(Collectors.toList());

        if (artifacts.size() > 1) {
            return new Result(null, artifacts);
        }

        if (artifacts.size() == 1) {
            return new Result(artifacts.get(0), null);
        }

        // Should not happen.
        throw new IllegalStateException("Corrupt artifact fetching state.");
    }

    /**
     * Fetches the job jar and any additional artifact if the given list is not null or empty.
     *
     * @param jobUri URI of the job jar
     * @param additionalUris URI(s) of any additional artifact to fetch
     * @return result with the fetched artifacts
     * @throws Exception
     */
    public Result fetchArtifacts(String jobUri, @Nullable List<String> additionalUris)
            throws Exception {
        checkArgument(jobUri != null && !jobUri.trim().isEmpty(), "The jobUri is required.");

        ArtifactUtils.createMissingParents(baseDir);
        File jobJar = fetchArtifact(jobUri);

        List<File> additionalArtifacts =
                additionalUris == null
                        ? Collections.emptyList()
                        : additionalUris.stream()
                                .map(FunctionUtils.uncheckedFunction(this::fetchArtifact))
                                .collect(Collectors.toList());

        return new Result(jobJar, additionalArtifacts);
    }

    @VisibleForTesting
    ArtifactFetcher getFetcher(URI uri) {
        if ("local".equals(uri.getScheme())) {
            return localFetcher;
        }

        if (isRawHttp(uri.getScheme()) || "https".equals(uri.getScheme())) {
            return httpFetcher;
        }

        return fsFetcher;
    }

    private File fetchArtifact(String uri) throws Exception {
        URI resolvedUri = PackagedProgramUtils.resolveURI(uri);
        File targetFile = new File(baseDir, FilenameUtils.getName(resolvedUri.getPath()));
        if (targetFile.exists()) {
            // Already fetched user artifacts are kept.
            return targetFile;
        }

        return getFetcher(resolvedUri).fetch(uri, conf, baseDir);
    }

    private boolean isRawHttp(String uriScheme) {
        if ("http".equals(uriScheme)) {
            if (conf.getBoolean(ArtifactFetchOptions.RAW_HTTP_ENABLED)) {
                return true;
            }
            throw new IllegalArgumentException(
                    String.format(
                            "Artifact fetching from raw HTTP endpoints are disabled. Set the '%s' property to override.",
                            ArtifactFetchOptions.RAW_HTTP_ENABLED.key()));
        }

        return false;
    }

    /** Artifact fetch result with all fetched artifact(s). */
    public static class Result {

        private final File jobJar;
        private final List<File> artifacts;

        private Result(@Nullable File jobJar, @Nullable List<File> additionalJars) {
            this.jobJar = jobJar;
            this.artifacts = additionalJars == null ? Collections.emptyList() : additionalJars;
        }

        @Nullable
        public File getJobJar() {
            return jobJar;
        }

        @Nullable
        public List<File> getArtifacts() {
            return artifacts.isEmpty() ? null : Collections.unmodifiableList(artifacts);
        }
    }
}
