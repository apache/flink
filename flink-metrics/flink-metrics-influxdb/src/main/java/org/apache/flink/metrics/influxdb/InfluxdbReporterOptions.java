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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.metrics.MetricConfig;

import org.influxdb.InfluxDB;

/**
 * Config options for {@link InfluxdbReporter}.
 */
public class InfluxdbReporterOptions {

	public static final ConfigOption<String> HOST = ConfigOptions
		.key("host")
		.noDefaultValue()
		.withDescription("the InfluxDB server host");

	public static final ConfigOption<Integer> PORT = ConfigOptions
		.key("port")
		.defaultValue(8086)
		.withDescription("the InfluxDB server port");

	public static final ConfigOption<String> USERNAME = ConfigOptions
		.key("username")
		.noDefaultValue()
		.withDescription("(optional) InfluxDB username used for authentication");

	public static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.noDefaultValue()
		.withDescription("(optional) InfluxDB username's password used for authentication");

	public static final ConfigOption<String> DB = ConfigOptions
		.key("db")
		.noDefaultValue()
		.withDescription("the InfluxDB database to store metrics");

	public static final ConfigOption<String> RETENTION_POLICY = ConfigOptions
		.key("retentionPolicy")
		.defaultValue("")
		.withDescription("(optional) the InfluxDB retention policy for metrics");

	public static final ConfigOption<InfluxDB.ConsistencyLevel>  CONSISTENCY = ConfigOptions
		.key("consistency")
		.enumType(InfluxDB.ConsistencyLevel.class)
		.defaultValue(InfluxDB.ConsistencyLevel.ONE)
		.withDescription("(optional) the InfluxDB consistency level for metrics");

	public static final ConfigOption<Integer> CONNECT_TIMEOUT = ConfigOptions
		.key("connectTimeout")
		.defaultValue(10000)
		.withDescription("(optional) the InfluxDB connect timeout for metrics");

	public static final ConfigOption<Integer> WRITE_TIMEOUT = ConfigOptions
		.key("writeTimeout")
		.defaultValue(10000)
		.withDescription("(optional) the InfluxDB write timeout for metrics");

	static String getString(MetricConfig config, ConfigOption<String> key) {
		return config.getString(key.key(), key.defaultValue());
	}

	static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
		return config.getInteger(key.key(), key.defaultValue());
	}

	static InfluxDB.ConsistencyLevel getConsistencyLevel(MetricConfig config, ConfigOption<InfluxDB.ConsistencyLevel> key) {
		return InfluxDB.ConsistencyLevel.valueOf(config.getProperty(key.key(), key.defaultValue().name()));
	}
}
