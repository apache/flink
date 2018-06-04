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

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Request for submitting a job.
 *
 * <p>We currently require the job-jars to be uploaded through the blob-server.
 */
public final class JobSubmitRequestBody implements RequestBody {

	private static final String FIELD_NAME_SERIALIZED_JOB_GRAPH = "serializedJobGraph";

	/**
	 * The serialized job graph.
	 */
	@JsonProperty(FIELD_NAME_SERIALIZED_JOB_GRAPH)
	public final byte[] serializedJobGraph;

	private final java.nio.file.Path storageDirectory;
	private final List<Path> uploadedJars;
	private final List<Path> uploadedArtifacts;

	/**
	 * Client-side constructor.
	 */
	public JobSubmitRequestBody(JobGraph jobGraph) throws IOException {
		this(serializeJobGraph(jobGraph), Collections.emptyList(), Collections.emptyList(), null);
	}

	@JsonCreator
	public JobSubmitRequestBody(
			@JsonProperty(FIELD_NAME_SERIALIZED_JOB_GRAPH) byte[] serializedJobGraph) {
		this(serializedJobGraph, Collections.emptyList(), Collections.emptyList(), null);
	}

	/**
	 * Intermediate constructor used by the {@link org.apache.flink.runtime.rest.FileUploadHandler}.
	 */
	public JobSubmitRequestBody(byte[] serializedJobGraph, List<Path> uploadedJars, List<Path> uploadedArtifacts, java.nio.file.Path storageDirectory) {
		this.serializedJobGraph = Preconditions.checkNotNull(serializedJobGraph);
		this.uploadedJars = Preconditions.checkNotNull(uploadedJars);
		this.uploadedArtifacts = Preconditions.checkNotNull(uploadedArtifacts);
		this.storageDirectory = storageDirectory;
	}

	@JsonIgnore
	public List<Path> getUploadedJars() {
		return uploadedJars;
	}

	@JsonIgnore
	public List<Path> getUploadedArtifacts() {
		return uploadedArtifacts;
	}

	@Override
	public int hashCode() {
		return 71 * Arrays.hashCode(this.serializedJobGraph);
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof JobSubmitRequestBody) {
			JobSubmitRequestBody other = (JobSubmitRequestBody) object;
			return Arrays.equals(this.serializedJobGraph, other.serializedJobGraph);
		}
		return false;
	}

	private static byte[] serializeJobGraph(JobGraph jobGraph) throws IOException {
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream(64 * 1024)) {
			ObjectOutputStream out = new ObjectOutputStream(baos);

			out.writeObject(jobGraph);

			return baos.toByteArray();
		}
	}

	public void cleanup() {
		if (storageDirectory != null) {
			FileUtils.deleteDirectoryQuietly(storageDirectory.toFile());
		}
	}
}
