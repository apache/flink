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

package org.apache.flink.yarn.configuration;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by Flink's YARN runners.
 * These options are not expected to be ever configured by users explicitly. 
 */
public class YarnConfigOptions {

	/**
	 * The hostname or address where the application master RPC system is listening.
	 */
	public static final ConfigOption<String> APP_MASTER_RPC_ADDRESS =
			key("yarn.appmaster.rpc.address")
			.noDefaultValue();

	/**
	 * The port where the application master RPC system is listening.
	 */
	public static final ConfigOption<Integer> APP_MASTER_RPC_PORT =
			key("yarn.appmaster.rpc.address")
			.defaultValue(-1);

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private YarnConfigOptions() {}
}
