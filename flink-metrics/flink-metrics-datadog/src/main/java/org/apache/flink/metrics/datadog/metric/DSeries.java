package org.apache.flink.metrics.datadog.metric;

import java.util.ArrayList;
import java.util.List;

/**
 *
 **/
public class DSeries {
	private List<DMetric> series;

	public DSeries() {
		series = new ArrayList<>();
	}

	public void addMetric(DMetric metric) {
		series.add(metric);
	}

	public List<DMetric> getSeries() {
		return series;
	}
}
