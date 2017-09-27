package org.apache.flink.runtime.rest.handler.legacy.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import java.util.Map;

public class JobMetricsOverview implements ResponseBody {

	public static final String FIELD_NAME_METRICS = "metrics";

	@JsonProperty(FIELD_NAME_METRICS)
	private final  Map<String, String> metrics;

	@JsonCreator
	public JobMetricsOverview(
			@JsonProperty(FIELD_NAME_METRICS) Map<String, String> metrics) {
		this.metrics = metrics;
	}

	public Map<String, String> getMetrics() {
		return metrics;
	}
}
