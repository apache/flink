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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Request for submitting a job.
 *
 * <p>This request only contains the names of files that must be present on the server, and defines
 * how these files are interpreted.
 */
public final class JobSubmitRequestBody implements RequestBody {

    public static final String FIELD_NAME_JOB_GRAPH = "jobGraphFileName";
    private static final String FIELD_NAME_JOB_JARS = "jobJarFileNames";
    private static final String FIELD_NAME_JOB_ARTIFACTS = "jobArtifactFileNames";

    @JsonProperty(FIELD_NAME_JOB_GRAPH)
    @Nullable
    public final String jobGraphFileName;

    @JsonProperty(FIELD_NAME_JOB_JARS)
    @Nonnull
    public final Collection<String> jarFileNames;

    @JsonProperty(FIELD_NAME_JOB_ARTIFACTS)
    @Nonnull
    public final Collection<DistributedCacheFile> artifactFileNames;

    @JsonCreator
    public JobSubmitRequestBody(
            @Nullable @JsonProperty(FIELD_NAME_JOB_GRAPH) String jobGraphFileName,
            @Nullable @JsonProperty(FIELD_NAME_JOB_JARS) Collection<String> jarFileNames,
            @Nullable @JsonProperty(FIELD_NAME_JOB_ARTIFACTS)
                    Collection<DistributedCacheFile> artifactFileNames) {
        this.jobGraphFileName = jobGraphFileName;
        if (jarFileNames == null) {
            this.jarFileNames = Collections.emptyList();
        } else {
            this.jarFileNames = jarFileNames;
        }
        if (artifactFileNames == null) {
            this.artifactFileNames = Collections.emptyList();
        } else {
            this.artifactFileNames = artifactFileNames;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobSubmitRequestBody that = (JobSubmitRequestBody) o;
        return Objects.equals(jobGraphFileName, that.jobGraphFileName)
                && Objects.equals(jarFileNames, that.jarFileNames)
                && Objects.equals(artifactFileNames, that.artifactFileNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobGraphFileName, jarFileNames, artifactFileNames);
    }

    @Override
    public String toString() {
        return "JobSubmitRequestBody{"
                + "jobGraphFileName='"
                + jobGraphFileName
                + '\''
                + ", jarFileNames="
                + jarFileNames
                + ", artifactFileNames="
                + artifactFileNames
                + '}';
    }

    /** Descriptor for a distributed cache file. */
    public static class DistributedCacheFile {
        private static final String FIELD_NAME_ENTRY_NAME = "entryName";
        private static final String FIELD_NAME_FILE_NAME = "fileName";

        @JsonProperty(FIELD_NAME_ENTRY_NAME)
        public final String entryName;

        @JsonProperty(FIELD_NAME_FILE_NAME)
        public final String fileName;

        @JsonCreator
        public DistributedCacheFile(
                @JsonProperty(FIELD_NAME_ENTRY_NAME) String entryName,
                @JsonProperty(FIELD_NAME_FILE_NAME) String fileName) {
            this.entryName = entryName;
            this.fileName = fileName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DistributedCacheFile that = (DistributedCacheFile) o;
            return Objects.equals(entryName, that.entryName)
                    && Objects.equals(fileName, that.fileName);
        }

        @Override
        public int hashCode() {

            return Objects.hash(entryName, fileName);
        }

        @Override
        public String toString() {
            return "DistributedCacheFile{"
                    + "entryName='"
                    + entryName
                    + '\''
                    + ", fileName='"
                    + fileName
                    + '\''
                    + '}';
        }
    }
}
