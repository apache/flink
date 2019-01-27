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

package org.apache.flink.api.java.io.jdbc;

import org.apache.flink.configuration.ConfigOption;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Options for jdbc.
 */
public class JDBCOptions {

	public static final ConfigOption<String> USER_NAME = key("username".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<String> PASSWORD = key("password".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<String> DRIVER_NAME = key("drivername".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<String> TABLE_NAME = key("tablename".toLowerCase())
			.noDefaultValue();

	public static final ConfigOption<String> DB_URL = key("dburl".toLowerCase())
			.noDefaultValue();

	public static final List<String> SUPPORTED_KEYS = Arrays.asList(
			USER_NAME.key(),
			PASSWORD.key(),
			DRIVER_NAME.key(),
			TABLE_NAME.key(),
			DB_URL.key());
}
