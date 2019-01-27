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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Contains information about the TaskManager LogDetail.
 */
public class LogDetail implements ResponseBody {

	public static final String FIELD_DATA = "data";
	public static final String FIELD_FILE_SIZE = "file_size";

	@JsonProperty(FIELD_DATA)
	private final String data;

	@JsonProperty(FIELD_FILE_SIZE)
	private final Long fileSize;

	@JsonCreator
	public LogDetail(
			@JsonProperty(FIELD_DATA) String data,
			@JsonProperty(FIELD_FILE_SIZE) Long fileSize) {
		this.data = Preconditions.checkNotNull(data);
		this.fileSize = Preconditions.checkNotNull(fileSize);
	}

	public String getData() {
		return this.data;
	}

	public Long getFileSize() {
		return this.fileSize;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LogDetail that = (LogDetail) o;
		return Objects.equals(data, that.data);
	}

	@Override
	public int hashCode() {
		return Objects.hash(data);
	}

	public static LogDetail empty() {
		return new LogDetail("", 0L);
	}
}
