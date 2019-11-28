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

package org.apache.flink.metrics.prometheus.tests;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.prometheus.PrometheusReporter;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.FlinkDistribution;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.tests.util.AutoClosableProcess.runBlocking;
import static org.apache.flink.tests.util.AutoClosableProcess.runNonBlocking;

/**
 * End-to-end test for the PrometheusReporter.
 */
@Category(TravisGroup1.class)
public class PrometheusReporterEndToEndITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporterEndToEndITCase.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String PROMETHEUS_VERSION = "2.4.3";
	private static final String PROMETHEUS_FILE_NAME;

	static {
		final String base = "prometheus-" + PROMETHEUS_VERSION + '.';
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case MAC_OS:
				PROMETHEUS_FILE_NAME = base + "darwin-amd64";
				break;
			case WINDOWS:
				PROMETHEUS_FILE_NAME = base + "windows-amd64";
				break;
			default:
				PROMETHEUS_FILE_NAME = base + "linux-amd64";
				break;
		}
	}

	private static final Pattern LOG_REPORTER_PORT_PATTERN = Pattern.compile(".*Started PrometheusReporter HTTP server on port ([0-9]+).*");

	@BeforeClass
	public static void checkOS() {
		Assume.assumeFalse("This test does not run on Windows.", OperatingSystem.isWindows());
	}

	@Rule
	public final FlinkDistribution dist = new FlinkDistribution();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testReporter() throws Exception {
		dist.copyOptJarsToLib("flink-metrics-prometheus");

		final Configuration config = new Configuration();
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "prom." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, PrometheusReporter.class.getCanonicalName());
		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "prom.port", "9000-9100");

		dist.appendConfiguration(config);

		final Path tmpPrometheusDir = tmp.newFolder().toPath().resolve("prometheus");
		final Path prometheusArchive = tmpPrometheusDir.resolve(PROMETHEUS_FILE_NAME + ".tar.gz");
		final Path prometheusBinDir = tmpPrometheusDir.resolve(PROMETHEUS_FILE_NAME);
		final Path prometheusConfig = prometheusBinDir.resolve("prometheus.yml");
		final Path prometheusBinary = prometheusBinDir.resolve("prometheus");
		Files.createDirectory(tmpPrometheusDir);

		LOG.info("Downloading Prometheus.");
		AutoClosableProcess
			.create(
				CommandLineWrapper
					.wget("https://github.com/prometheus/prometheus/releases/download/v" + PROMETHEUS_VERSION + '/' + prometheusArchive.getFileName())
					.targetDir(tmpPrometheusDir)
					.build())
			.runBlocking(Duration.ofMinutes(5));

		LOG.info("Unpacking Prometheus.");
		runBlocking(
			CommandLineWrapper
				.tar(prometheusArchive)
				.extract()
				.zipped()
				.targetDir(tmpPrometheusDir)
				.build());

		LOG.info("Setting Prometheus scrape interval.");
		runBlocking(
			CommandLineWrapper
				.sed("s/\\(scrape_interval:\\).*/\\1 1s/", prometheusConfig)
				.inPlace()
				.build());

		dist.startFlinkCluster();

		final List<Integer> ports = dist
			.searchAllLogs(LOG_REPORTER_PORT_PATTERN, matcher -> matcher.group(1))
			.map(Integer::valueOf)
			.collect(Collectors.toList());

		final String scrapeTargets = ports.stream()
			.map(port -> "'localhost:" + port + "'")
			.collect(Collectors.joining(", "));

		LOG.info("Setting Prometheus scrape targets to {}.", scrapeTargets);
		runBlocking(
			CommandLineWrapper
				.sed("s/\\(targets:\\).*/\\1 [" + scrapeTargets + "]/", prometheusConfig)
				.inPlace()
				.build());

		LOG.info("Starting Prometheus server.");
		try (AutoClosableProcess prometheus = runNonBlocking(
			prometheusBinary.toAbsolutePath().toString(),
			"--config.file=" + prometheusConfig.toAbsolutePath(),
			"--storage.tsdb.path=" + prometheusBinDir.resolve("data").toAbsolutePath())) {

			final OkHttpClient client = new OkHttpClient();

			checkMetricAvailability(client, "flink_jobmanager_numRegisteredTaskManagers");
			checkMetricAvailability(client, "flink_taskmanager_Status_Network_TotalMemorySegments");
		}
	}

	private static void checkMetricAvailability(final OkHttpClient client, final String metric) throws InterruptedException {
		final Request jobManagerRequest = new Request.Builder()
			.get()
			.url("http://localhost:9090/api/v1/query?query=" + metric)
			.build();

		Exception reportedException = null;
		for (int x = 0; x < 30; x++) {
			try (Response response = client.newCall(jobManagerRequest).execute()) {
				if (response.isSuccessful()) {
					final String json = response.body().string();

					// Sample response:
					//{
					//	"status": "success",
					//	"data": {
					//		"resultType": "vector",
					//		"result": [{
					//			"metric": {
					//				"__name__": "flink_jobmanager_numRegisteredTaskManagers",
					//				"host": "localhost",
					//				"instance": "localhost:9000",
					//				"job": "prometheus"
					//			},
					//			"value": [1540548500.107, "1"]
					//		}]
					//	}
					//}
					OBJECT_MAPPER.readTree(json)
						.get("data")
						.get("result")
						.get(0)
						.get("value")
						.get(1).asInt();
					// if we reach this point some value for the given metric was reported to prometheus
					return;
				} else {
					LOG.info("Retrieving metric failed. Retrying... " + response.code() + ":" + response.message());
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				reportedException = ExceptionUtils.firstOrSuppressed(e, reportedException);
				Thread.sleep(1000);
			}
		}
		throw new AssertionError("Could not retrieve metric " + metric + " from Prometheus.", reportedException);
	}
}
