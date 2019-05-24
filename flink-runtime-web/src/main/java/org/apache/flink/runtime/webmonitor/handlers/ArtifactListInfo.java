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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Response type of the {@link ArtifactListHandler}.
 */
public class ArtifactListInfo implements ResponseBody {
	public static final String ARTIFACT_LIST_FIELD_ADDRESS = "address";
	public static final String ARTIFACT_LIST_FIELD_FILES = "files";

	@JsonProperty(ARTIFACT_LIST_FIELD_ADDRESS)
	private String address;

	@JsonProperty(ARTIFACT_LIST_FIELD_FILES)
	public List<ArtifactFileInfo> artifactFileList;

	@JsonCreator
	public ArtifactListInfo(
			@JsonProperty(ARTIFACT_LIST_FIELD_ADDRESS) String address,
			@JsonProperty(ARTIFACT_LIST_FIELD_FILES) List<ArtifactFileInfo> artifactFileList) {
		this.address = checkNotNull(address);
		this.artifactFileList = checkNotNull(artifactFileList);
	}

	@Override
	public int hashCode() {
		return Objects.hash(address, artifactFileList);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		ArtifactListInfo that = (ArtifactListInfo) o;
		return Objects.equals(address, that.address) &&
			Objects.equals(artifactFileList, that.artifactFileList);
	}

	//---------------------------------------------------------------------------------
	// Static helper classes
	//---------------------------------------------------------------------------------

	/**
	 * Nested class to encapsulate the artifact file info.
	 */
	public static class ArtifactFileInfo {
		public static final String ARTIFACT_FILE_FIELD_ID = "id";
		public static final String ARTIFACT_FILE_FIELD_NAME = "name";
		public static final String ARTIFACT_FILE_FIELD_UPLOADED = "uploaded";
		public static final String ARTIFACT_FILE_FIELD_ENTRY = "entry";

		@JsonProperty(ARTIFACT_FILE_FIELD_ID)
		public String id;

		@JsonProperty(ARTIFACT_FILE_FIELD_NAME)
		public String name;

		@JsonProperty(ARTIFACT_FILE_FIELD_UPLOADED)
		private long uploaded;

		@JsonProperty(ARTIFACT_FILE_FIELD_ENTRY)
		private List<ArtifactEntryInfo> artifactEntryList;

		@JsonCreator
		public ArtifactFileInfo(
				@JsonProperty(ARTIFACT_FILE_FIELD_ID) String id,
				@JsonProperty(ARTIFACT_FILE_FIELD_NAME) String name,
				@JsonProperty(ARTIFACT_FILE_FIELD_UPLOADED) long uploaded,
				@JsonProperty(ARTIFACT_FILE_FIELD_ENTRY) List<ArtifactEntryInfo> artifactEntryList) {
			this.id = checkNotNull(id);
			this.name = checkNotNull(name);
			this.uploaded = uploaded;
			this.artifactEntryList = checkNotNull(artifactEntryList);
		}

		@Override
		public int hashCode() {
			return Objects.hash(id, name, uploaded, artifactEntryList);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			ArtifactFileInfo that = (ArtifactFileInfo) o;
			return Objects.equals(id, that.id) &&
				Objects.equals(name, that.name) &&
				uploaded == that.uploaded &&
				Objects.equals(artifactEntryList, that.artifactEntryList);
		}
	}

	/**
	 * Nested class to encapsulate the artifact entry info.
	 */
	public static class ArtifactEntryInfo {
		public static final String ARTIFACT_ENTRY_FIELD_NAME = "name";
		public static final String ARTIFACT_ENTRY_FIELD_DESC = "description";

		@JsonProperty(ARTIFACT_ENTRY_FIELD_NAME)
		private String name;

		@JsonProperty(ARTIFACT_ENTRY_FIELD_DESC)
		@Nullable
		private String description;

		@JsonCreator
		public ArtifactEntryInfo(
				@JsonProperty(ARTIFACT_ENTRY_FIELD_NAME) String name,
				@JsonProperty(ARTIFACT_ENTRY_FIELD_DESC) @Nullable String description) {
			this.name = checkNotNull(name);
			this.description = description;
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, description);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}

			if (null == o || this.getClass() != o.getClass()) {
				return false;
			}

			ArtifactEntryInfo that = (ArtifactEntryInfo) o;
			return Objects.equals(name, that.name) &&
				Objects.equals(description, that.description);
		}
	}
}
