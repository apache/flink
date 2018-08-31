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

package org.apache.flink.streaming.python;

import org.apache.flink.configuration.ConfigOption;

import java.io.File;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the Python API.
 */
public class PythonOptions {

	/**
	 * The config parameter defining where temporary plan-related files are stored on the client.
	 */
	public static final ConfigOption<String> PLAN_TMP_DIR =
		key("python.plan.tmp.dir")
			.noDefaultValue();

	/**
	 * The config parameter defining where the flink python library and user supplied files will be uploaded to before
	 * registering them with the Distributed Cache. This directory must be accessible from all worker nodes.
	 */
	public static final ConfigOption<String> DC_TMP_DIR =
		key("python.dc.tmp.dir")
			.defaultValue(System.getProperty("java.io.tmpdir") + File.separator + "flink_dc");

	private PythonOptions() {
	}
}
