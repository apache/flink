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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.TreeSet;

/**
 * /**
 * Response type of the {@link org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogListHandler}
 * and {@link org.apache.flink.runtime.rest.handler.files.JobManagerLogListHandler}.
 */
public class LogListInfo implements ResponseBody {
	@JsonProperty
	private final List<String> loglist;

	public LogListInfo(String[] loglist) {
		TreeSet<String> sortedFiles = new TreeSet<>(Comparator.naturalOrder());
		for (String file : loglist) {
			sortedFiles.add(file);
		}
		this.loglist = Lists.newArrayList(sortedFiles);
	}

	public List<String> getLoglist() {
		return loglist;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (null == o || this.getClass() != o.getClass()) {
			return false;
		}

		LogListInfo that = (LogListInfo) o;
		return that.loglist.equals(this.loglist);
	}

	@Override
	public int hashCode() {
		return Objects.hash(this.loglist);
	}
}
