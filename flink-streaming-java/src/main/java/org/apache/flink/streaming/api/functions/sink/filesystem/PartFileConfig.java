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

import org.apache.flink.util.Preconditions;

/**
 * Part file name configuration.
 * This allow to define a prefix and a suffix to the part file name.
 */
class PartFileConfig {

	public static final String DEFAULT_PART_PREFIX = "part";

	public static final String DEFAULT_PART_SUFFIX = "";

	private final String partPrefix;

	private final String partSuffix;

	PartFileConfig() {
		this(DEFAULT_PART_PREFIX, DEFAULT_PART_SUFFIX);
	}

	PartFileConfig(final String partPrefix, final String partSuffix) {
		this.partPrefix = Preconditions.checkNotNull(partPrefix);
		this.partSuffix = Preconditions.checkNotNull(partSuffix);
	}

	String getPartPrefix() {
		return partPrefix;
	}

	String getPartSuffix() {
		return partSuffix;
	}
}
