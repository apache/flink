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
package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;

/**
 * This class provides utility-functions for handling scopes.
 */
@Internal
public class Scope {
	public static final String SCOPE_WILDCARD = "*";

	private static final String SCOPE_PREFIX = "<";
	private static final String SCOPE_SUFFIX = ">";
	private static final String SCOPE_SPLIT = ".";

	private Scope() {
	}

	/**
	 * Modifies the given string to resemble a scope variable.
	 *
	 * @param scope string to format
	 * @return formatted string
	 */
	public static String format(String scope) {
		return SCOPE_PREFIX + scope + SCOPE_SUFFIX;
	}

	/**
	 * Joins the given components into a single scope.
	 *
	 * @param components components to join
	 * @return joined scoped
	 */
	public static String concat(String... components) {
		StringBuilder sb = new StringBuilder();
		sb.append(components[0]);
		for (int x = 1; x < components.length; x++) {
			sb.append(SCOPE_SPLIT);
			sb.append(components[x]);
		}
		return sb.toString();
	}

	/**
	 * Splits the given scope into it's individual components.
	 *
	 * @param scope scope to split
	 * @return array of components
	 */
	public static String[] split(String scope) {
		return scope.split("\\" + SCOPE_SPLIT);
	}

	/**
	 * Simple container for component scope format strings.
	 */
	public static class ScopeFormat {
		
		private String operatorFormat = OperatorMetricGroup.DEFAULT_SCOPE_OPERATOR;
		private String taskFormat = TaskMetricGroup.DEFAULT_SCOPE_TASK;
		private String jobFormat = JobMetricGroup.DEFAULT_SCOPE_JOB;
		private String taskManagerFormat = TaskManagerMetricGroup.DEFAULT_SCOPE_TM;
		

		public ScopeFormat setOperatorFormat(String format) {
			this.operatorFormat = format;
			return this;
		}

		public ScopeFormat setTaskFormat(String format) {
			this.taskFormat = format;
			return this;
		}

		public ScopeFormat setJobFormat(String format) {
			this.jobFormat = format;
			return this;
		}

		public ScopeFormat setTaskManagerFormat(String format) {
			this.taskManagerFormat = format;
			return this;
		}

		public String getOperatorFormat() {
			return this.operatorFormat;
		}

		public String getTaskFormat() {
			return this.taskFormat;
		}

		public String getJobFormat() {
			return this.jobFormat;
		}

		public String getTaskManagerFormat() {
			return this.taskManagerFormat;
		}
	}
}
