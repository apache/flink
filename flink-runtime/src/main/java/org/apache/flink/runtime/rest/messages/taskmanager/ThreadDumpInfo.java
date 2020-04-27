/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

/**
 * Class containing thread dump information.
 */
public final class ThreadDumpInfo implements ResponseBody, Serializable {
	private static final long serialVersionUID = 1L;

	public static final String FIELD_NAME_THREAD_INFOS = "threadInfos";

	@JsonProperty(FIELD_NAME_THREAD_INFOS)
	private final Collection<ThreadInfo> threadInfos;

	private ThreadDumpInfo(Collection<ThreadInfo> threadInfos) {
		this.threadInfos = threadInfos;
	}

	public Collection<ThreadInfo> getThreadInfos() {
		return threadInfos;
	}

	@JsonCreator
	public static ThreadDumpInfo create(
			@JsonProperty(FIELD_NAME_THREAD_INFOS) Collection<ThreadInfo> threadInfos) {
		return new ThreadDumpInfo(threadInfos);
	}

	/**
	 * Class containing information about a thread.
	 */
	public static final class ThreadInfo implements Serializable {
		private static final long serialVersionUID = 1L;

		public static final String FIELD_NAME_THREAD_NAME = "threadName";

		public static final String FIELD_NAME_THREAD_INFO = "stringifiedThreadInfo";

		@JsonProperty(FIELD_NAME_THREAD_NAME)
		private final String threadName;

		@JsonProperty(FIELD_NAME_THREAD_INFO)
		private final String stringifiedThreadInfo;

		private ThreadInfo(String threadName, String stringifiedThreadInfo) {
			this.threadName = threadName;
			this.stringifiedThreadInfo = stringifiedThreadInfo;
		}

		@JsonCreator
		public static ThreadInfo create(
			@JsonProperty(FIELD_NAME_THREAD_NAME) String threadName,
			@JsonProperty(FIELD_NAME_THREAD_INFO) String stringifiedThreadInfo) {
			return new ThreadInfo(threadName, stringifiedThreadInfo);
		}

		public String getThreadName() {
			return threadName;
		}

		public String getStringifiedThreadInfo() {
			return stringifiedThreadInfo;
		}

		@Override
		public String toString() {
			return stringifiedThreadInfo;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			ThreadInfo that = (ThreadInfo) o;
			return Objects.equals(threadName, that.threadName) &&
				Objects.equals(stringifiedThreadInfo, that.stringifiedThreadInfo);
		}

		@Override
		public int hashCode() {
			return Objects.hash(threadName, stringifiedThreadInfo);
		}
	}
}
