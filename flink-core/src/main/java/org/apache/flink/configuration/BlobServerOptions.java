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
package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Configuration options for the BlobServer.
 */
@PublicEvolving
public class BlobServerOptions {

	/**
	 * The config parameter defining the storage directory to be used by the blob server.
	 */
	public static final ConfigOption<String> STORAGE_DIRECTORY =
		key("blob.storage.directory")
			.noDefaultValue();

	/**
	 * The config parameter defining number of retires for failed BLOB fetches.
	 */
	public static final ConfigOption<Integer> FETCH_RETRIES =
		key("blob.fetch.retries")
			.defaultValue(5);

	/**
	 * The config parameter defining the maximum number of concurrent BLOB fetches that the JobManager serves.
	 */
	public static final ConfigOption<Integer> FETCH_CONCURRENT =
		key("blob.fetch.num-concurrent")
			.defaultValue(50);

	/**
	 * The config parameter defining the backlog of BLOB fetches on the JobManager.
	 */
	public static final ConfigOption<Integer> FETCH_BACKLOG =
		key("blob.fetch.backlog")
			.defaultValue(1000);

	/**
	 * The config parameter defining the server port of the blob service.
	 * The port can either be a port, such as "9123",
	 * a range of ports: "50100-50200"
	 * or a list of ranges and or points: "50100-50200,50300-50400,51234"
	 *
	 * Setting the port to 0 will let the OS choose an available port.
	 */
	public static final ConfigOption<String> PORT =
		key("blob.server.port")
			.defaultValue("0");

	/**
	 * Flag to override ssl support for the blob service transport.
	 */
	public static final ConfigOption<Boolean> SSL_ENABLED =
		key("blob.service.ssl.enabled")
			.defaultValue(true);
}
