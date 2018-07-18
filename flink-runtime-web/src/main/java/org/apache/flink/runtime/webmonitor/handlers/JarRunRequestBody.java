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

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * {@link RequestBody} for running a jar.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JarRunRequestBody implements RequestBody {

	private static final String FIELD_NAME_ENTRY_CLASS = "entryClass";
	private static final String FIELD_NAME_PROGRAM_ARGUMENTS = "programArgs";
	private static final String FIELD_NAME_PARALLELISM = "parallelism";
	private static final String FIELD_NAME_ALLOW_NON_RESTORED_STATE = "allowNonRestoredState";
	private static final String FIELD_NAME_SAVEPOINT_PATH = "savepointPath";

	@JsonProperty(FIELD_NAME_ENTRY_CLASS)
	@Nullable
	private String entryClassName;

	@JsonProperty(FIELD_NAME_PROGRAM_ARGUMENTS)
	@Nullable
	private String programArguments;

	@JsonProperty(FIELD_NAME_PARALLELISM)
	@Nullable
	private Integer parallelism;

	@JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE)
	@Nullable
	private Boolean allowNonRestoredState;

	@JsonProperty(FIELD_NAME_SAVEPOINT_PATH)
	@Nullable
	private String savepointPath;

	public JarRunRequestBody() {
		this(null, null, null, null, null);
	}

	@JsonCreator
	public JarRunRequestBody(
			@JsonProperty(FIELD_NAME_ENTRY_CLASS) String entryClassName,
			@JsonProperty(FIELD_NAME_PROGRAM_ARGUMENTS) String programArguments,
			@JsonProperty(FIELD_NAME_PARALLELISM) Integer parallelism,
			@JsonProperty(FIELD_NAME_ALLOW_NON_RESTORED_STATE) Boolean allowNonRestoredState,
			@JsonProperty(FIELD_NAME_SAVEPOINT_PATH) String savepointPath) {
		this.entryClassName = entryClassName;
		this.programArguments = programArguments;
		this.parallelism = parallelism;
		this.allowNonRestoredState = allowNonRestoredState;
		this.savepointPath = savepointPath;
	}

	@Nullable
	@JsonIgnore
	public String getEntryClassName() {
		return entryClassName;
	}

	@Nullable
	@JsonIgnore
	public String getProgramArguments() {
		return programArguments;
	}

	@Nullable
	@JsonIgnore
	public Integer getParallelism() {
		return parallelism;
	}

	@Nullable
	@JsonIgnore
	public Boolean getAllowNonRestoredState() {
		return allowNonRestoredState;
	}

	@Nullable
	@JsonIgnore
	public String getSavepointPath() {
		return savepointPath;
	}
}
