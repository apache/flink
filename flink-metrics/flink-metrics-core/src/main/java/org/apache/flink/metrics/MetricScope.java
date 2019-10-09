package org.apache.flink.metrics;

import java.util.Map;

/**
 * Represents the scope of group, i.e., information used to identify metrics.
 */
public interface MetricScope {

	/**
	 * Returns a map of all variables and their associated value, for example
	 * {@code {"<host>"="host-7", "<tm_id>"="taskmanager-2"}}.
	 *
	 * @return map of all variables and their associated value
	 */
	Map<String, String> getAllVariables();

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @return fully qualified metric name
	 */
	String getMetricIdentifier(String metricName);

	/**
	 * Returns the fully qualified metric name, for example
	 * {@code "host-7.taskmanager-2.window_word_count.my-mapper.metricName"}.
	 *
	 * @param metricName metric name
	 * @param filter character filter which is applied to the scope components if not null.
	 * @return fully qualified metric name
	 */
	String getMetricIdentifier(String metricName, CharacterFilter filter);
}
