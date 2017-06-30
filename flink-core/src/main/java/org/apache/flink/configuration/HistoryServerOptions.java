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
 * The set of configuration options relating to the HistoryServer.
 */
@PublicEvolving
public class HistoryServerOptions {

	/**
	 * The interval at which the HistoryServer polls {@link HistoryServerOptions#HISTORY_SERVER_ARCHIVE_DIRS} for new archives.
	 */
	public static final ConfigOption<Long> HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL =
		key("historyserver.archive.fs.refresh-interval")
			.defaultValue(10000L);

	/**
	 * Comma-separated list of directories which the HistoryServer polls for new archives.
	 */
	public static final ConfigOption<String> HISTORY_SERVER_ARCHIVE_DIRS =
		key("historyserver.archive.fs.dir")
			.noDefaultValue();

	/**
	 * The local directory used by the HistoryServer web-frontend.
	 */
	public static final ConfigOption<String> HISTORY_SERVER_WEB_DIR =
		key("historyserver.web.tmpdir")
			.noDefaultValue();

	/**
	 * The address under which the HistoryServer web-frontend is accessible.
	 */
	public static final ConfigOption<String> HISTORY_SERVER_WEB_ADDRESS =
		key("historyserver.web.address")
			.noDefaultValue();

	/**
	 * The port under which the HistoryServer web-frontend is accessible.
	 */
	public static final ConfigOption<Integer> HISTORY_SERVER_WEB_PORT =
		key("historyserver.web.port")
			.defaultValue(8082);

	/**
	 * The refresh interval for the HistoryServer web-frontend in milliseconds.
	 */
	public static final ConfigOption<Long> HISTORY_SERVER_WEB_REFRESH_INTERVAL =
		key("historyserver.web.refresh-interval")
			.defaultValue(10000L);

	/**
	 * Enables/Disables SSL support for the HistoryServer web-frontend. Only relevant if
	 * {@link SecurityOptions#SSL_ENABLED} is enabled.
	 */
	public static final ConfigOption<Boolean> HISTORY_SERVER_WEB_SSL_ENABLED =
		key("historyserver.web.ssl.enabled")
			.defaultValue(false);

	private HistoryServerOptions() {
	}
}
