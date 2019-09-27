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

package org.apache.flink.python;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/**
 * Configuration options for the Python API.
 */
@PublicEvolving
public class PythonOptions {

	/**
	 * The maximum number of elements to include in a bundle.
	 */
	public static final ConfigOption<Integer> MAX_BUNDLE_SIZE = ConfigOptions
		.key("python.fn-execution.bundle.size")
		.defaultValue(1000)
		.withDescription("The maximum number of elements to include in a bundle. The elements " +
			"are processed asynchronously. One bundle of elements are processed before " +
			"processing the next bundle of elements");

	/**
	 * The maximum time to wait before finalising a bundle (in milliseconds).
	 */
	public static final ConfigOption<Long> MAX_BUNDLE_TIME_MILLS = ConfigOptions
		.key("python.fn-execution.bundle.time")
		.defaultValue(1000L)
		.withDescription("The maximum time to wait before finishing a bundle (in milliseconds).");
}
