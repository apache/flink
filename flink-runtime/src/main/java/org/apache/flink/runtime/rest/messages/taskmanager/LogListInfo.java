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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Class containing a collection of {@link LogInfo}.
 */
public class LogListInfo implements ResponseBody, Serializable {

	public static final String FIELD_NAME_LOGS = "logs";

	private static final long serialVersionUID = 7531494560450830517L;

	@JsonProperty(FIELD_NAME_LOGS)
	private final Collection<LogInfo> logInfos;

	@JsonCreator
	public LogListInfo(@JsonProperty(FIELD_NAME_LOGS) Collection<LogInfo> logInfos) {
		this.logInfos = Preconditions.checkNotNull(logInfos);
	}

	public Collection<LogInfo> getLogInfos() {
		return logInfos;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		LogListInfo that = (LogListInfo) o;
		return Objects.equals(logInfos, that.logInfos);
	}

	@Override
	public int hashCode() {
		return Objects.hash(logInfos);
	}

	public static LogListInfo empty() {
		return new LogListInfo(Collections.emptyList());
	}
}
