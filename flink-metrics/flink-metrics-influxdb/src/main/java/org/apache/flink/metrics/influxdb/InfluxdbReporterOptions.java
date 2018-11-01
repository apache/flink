package org.apache.flink.metrics.influxdb;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.metrics.MetricConfig;

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
		.defaultValue("flink")
		.withDescription("the InfluxDB database to store metrics");

	static String getString(MetricConfig config, ConfigOption<String> key) {
		return config.getString(key.key(), key.defaultValue());
	}

	static int getInteger(MetricConfig config, ConfigOption<Integer> key) {
		return config.getInteger(key.key(), key.defaultValue());
	}
}
