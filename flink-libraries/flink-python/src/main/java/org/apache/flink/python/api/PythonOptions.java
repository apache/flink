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

package org.apache.flink.python.api;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the Python API.
 */
public class PythonOptions {

	/**
	 * The config parameter defining the path to the python binary to use.
	 */
	public static final ConfigOption<String> PYTHON_BINARY_PATH =
		key("python.binary.path")
			.defaultValue("python")
		.withDeprecatedKeys("python.binary.python2", "python.binary.python3");

	/**
	 * The config parameter defining the size of the memory-mapped files, in kb.
	 * This value must be large enough to ensure that the largest serialized record can be written completely into
	 * the file.
	 *
	 * <p>Every task will allocate 2 memory-files, each with this size.
	 */
	public static final ConfigOption<Long> MMAP_FILE_SIZE =
		key("python.mmap.size.kb")
			.defaultValue(4L);

	/**
	 * The config parameter defining where temporary plan-related files are stored on the client.
	 */
	public static final ConfigOption<String> PLAN_TMP_DIR =
		key("python.plan.tmp.dir")
			.noDefaultValue();

	/**
	 * The config parameter defining where the memory-mapped files will be created.
	 */
	public static final ConfigOption<String> DATA_TMP_DIR =
		key("python.mmap.tmp.dir")
			.noDefaultValue();

	private PythonOptions() {
	}
}
