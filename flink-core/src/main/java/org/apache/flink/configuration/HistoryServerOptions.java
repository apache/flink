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

import static org.apache.flink.configuration.ConfigOptions.key;

public class HistoryServerOptions {
	public static final ConfigOption<String> HISTORY_SERVER_RPC_HOST =
		key("historyserver.rpc.host")
			.defaultValue("localhost");

	public static final ConfigOption<Integer> HISTORY_SERVER_RPC_PORT =
		key("historyserver.rpc.port")
			.defaultValue(-1);

	public static final ConfigOption<String> HISTORY_SERVER_DIR =
		key("historyserver.dir")
			.noDefaultValue();

	public static final ConfigOption<Integer> HISTORY_SERVER_WEB_PORT =
		key("historyserver.web.port")
			.defaultValue(8082);

	public static final ConfigOption<Long> HISTORY_SERVER_WEB_REFRESH_INTERVAL =
		key("historyserver.web.refresh-interval")
			.defaultValue(10000L);

	public static final ConfigOption<Boolean> HISTORY_SERVER_WEB_SSL_ENABLED =
		key("historyserver.web.ssl.enabled")
			.defaultValue(false);
}
