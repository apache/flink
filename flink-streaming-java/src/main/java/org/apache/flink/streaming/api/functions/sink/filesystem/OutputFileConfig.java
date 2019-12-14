/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Part file name configuration.
 * This allow to define a prefix and a suffix to the part file name.
 */
public class OutputFileConfig implements Serializable {

	private final String partPrefix;

	private final String partSuffix;

	/**
	 *	Initiates the {@code PartFileConfig} with values passed as parameters.
	 *
	 * @param partPrefix - the beginning of part file name
	 * @param partSuffix - the ending of part file name
	 */
	public OutputFileConfig(final String partPrefix, final String partSuffix) {
		this.partPrefix = Preconditions.checkNotNull(partPrefix);
		this.partSuffix = Preconditions.checkNotNull(partSuffix);
	}

	/**
	 * The prefix for the part name.
	 */
	String getPartPrefix() {
		return partPrefix;
	}

	/**
	 * The suffix for the part name.
	 */
	String getPartSuffix() {
		return partSuffix;
	}

	public static OutputFileConfigBuilder builder() {
		return new OutputFileConfigBuilder();
	}

	/**
	 * A builder to create the part file configuration.
	 */
	@PublicEvolving
	public static class OutputFileConfigBuilder {

		private static final String DEFAULT_PART_PREFIX = "part";

		private static final String DEFAULT_PART_SUFFIX = "";

		private String partPrefix;

		private String partSuffix;

		private OutputFileConfigBuilder() {
			this.partPrefix = DEFAULT_PART_PREFIX;
			this.partSuffix = DEFAULT_PART_SUFFIX;
		}

		public OutputFileConfigBuilder withPartPrefix(String prefix) {
			this.partPrefix = prefix;
			return this;
		}

		public OutputFileConfigBuilder withPartSuffix(String suffix) {
			this.partSuffix = suffix;
			return this;
		}

		public OutputFileConfig build() {
			return new OutputFileConfig(partPrefix, partSuffix);
		}
	}
}
