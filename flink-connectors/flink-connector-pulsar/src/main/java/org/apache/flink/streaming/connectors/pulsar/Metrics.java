package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.annotation.Internal;

/**
 * A collection of Pulsar consumer metrics related constant strings.
 *
 * <p>The names must not be changed, as that would break backward compatibility for the consumer's metrics.
 */
@Internal
public class Metrics {

	// ------------------------------------------------------------------------
	//  Per-subtask metrics
	// ------------------------------------------------------------------------

	public static final String COMMITS_SUCCEEDED_METRICS_COUNTER = "commitsSucceeded";
	public static final String COMMITS_FAILED_METRICS_COUNTER = "commitsFailed";

	private Metrics() {
	}
}
