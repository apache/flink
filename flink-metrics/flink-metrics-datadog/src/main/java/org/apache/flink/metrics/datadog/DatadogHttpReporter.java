package org.apache.flink.metrics.datadog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.datadog.metric.DCounter;
import org.apache.flink.metrics.datadog.metric.DGauge;
import org.apache.flink.metrics.datadog.metric.DSeries;
import org.apache.flink.metrics.datadog.parser.MetricParser;
import org.apache.flink.metrics.datadog.parser.NameAndTags;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;


/**
 * Abbr: dghttp
 *
 * Metric Reporter for Datadog
 *
 * When 'metrics.reporter.dghttp.tags.enabled' is set to true,
 * metric scope formats have to be defined as followed:
 *
 * metrics.scope.jm: <host>.jobmanager
 * metrics.scope.jm.job: <host>.<job_name>.jobmanager.job
 * metrics.scope.tm: <host>.<tm_id>.taskmanager
 * metrics.scope.tm.job: <host>.<tm_id>.<job_name>.taskmanager.job
 * metrics.scope.task: <host>.<tm_id>.<job_name>.<subtask_index>.<task_name>.task
 * metrics.scope.operator: <host>.<tm_id>.<job_name>.<subtask_index>.<operator_name>.operator
 *
 * Variables will be separated from metric names, and sent to Datadog as tags
 * */
public class DatadogHttpReporter extends AbstractReporter implements Scheduled {
	private static final Logger LOG = LoggerFactory.getLogger(DatadogHttpReporter.class);

	public static final String DG_API_KEY = "DG_API_KEY";
	public static final String API_KEY = "apikey";

	private static final ObjectMapper MAPPER = new ObjectMapper();

	private DatadogHttpClient client;

	private MetricParser metricParser;

	@Override
	public void open(MetricConfig config) {
		client = new DatadogHttpClient(config.getString(API_KEY, null));
		log.info("Configured DatadogHttpReporter");

		metricParser = new MetricParser(config);
	}

	@Override
	public void close() {
		log.info("Shut down DatadogHttpReporter");
	}

	@Override
	public void report() {
		try {
			DatadogHttpRequest request = new DatadogHttpRequest(this);

			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				// Flink uses Gauge to store values more than numeric, like String, hashmap, etc.
				// Need to filter out those non-numeric ones
				if(entry.getKey().getValue() instanceof Number) {
					NameAndTags nat = metricParser.getNameAndTags(entry.getValue());

//                    LOG.info(nat.getName() + " " + nat.getTags());
						request.addGauge(
							new DGauge(
								nat.getName(),
								(Number) entry.getKey().getValue(),
								nat.getTags()));
				}
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				NameAndTags nat = metricParser.getNameAndTags(entry.getValue());

//                LOG.info(nat.getName() + " " + nat.getTags());
				request.addCounter(
					new DCounter(
						nat.getName(),
						entry.getKey().getCount(),
						nat.getTags()));
			}

			request.send();
		} catch (Exception e) {
			LOG.warn("Failed reporting metrics to Datadog.", e);
		}
	}

	@Override
	public String filterCharacters(String metricName) {
		return metricName;
	}

	/**
	 *
	 * */
	private static class DatadogHttpRequest {
		private final DatadogHttpReporter datadogHttpReporter;
		private final DSeries series;

		public DatadogHttpRequest(DatadogHttpReporter reporter) throws IOException {
			datadogHttpReporter = reporter;
			series = new DSeries();
		}

		public void addGauge(DGauge gauge) throws IOException {
			series.addMetric(gauge);
		}

		public void addCounter(DCounter counter) throws IOException {
			series.addMetric(counter);
		}

		public void send() throws Exception {
			datadogHttpReporter.client.syncPost(MAPPER.writeValueAsString(series));
		}
	}
}
