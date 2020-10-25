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
import org.apache.flink.metrics.prometheus.PrometheusReporterFactory;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.ProcessorArchitecture;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.metrics.prometheus.tests.PrometheusReporterEndToEndITCase.TestParams.InstantiationType.FACTORY;
import static org.apache.flink.metrics.prometheus.tests.PrometheusReporterEndToEndITCase.TestParams.InstantiationType.REFLECTION;
import static org.apache.flink.tests.util.AutoClosableProcess.runBlocking;
import static org.apache.flink.tests.util.AutoClosableProcess.runNonBlocking;

/**
 * End-to-end test for the PrometheusReporter.
 */
@Category(TravisGroup1.class)
@RunWith(Parameterized.class)
public class PrometheusReporterEndToEndITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(PrometheusReporterEndToEndITCase.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	private static final String PROMETHEUS_VERSION = "2.4.3";
	private static final String PROMETHEUS_FILE_NAME;
	private static final String PROMETHEUS_JAR_PREFIX = "flink-metrics-prometheus";

	static {
		final String base = "prometheus-" + PROMETHEUS_VERSION + '.';
		final String os;
		final String platform;
		switch (OperatingSystem.getCurrentOperatingSystem()) {
			case MAC_OS:
				os = "darwin";
				break;
			case WINDOWS:
				os = "windows";
				break;
			default:
				os = "linux";
				break;
		}
		switch (ProcessorArchitecture.getProcessorArchitecture()) {
			case X86:
				platform = "386";
				break;
			case AMD64:
				platform = "amd64";
				break;
			case ARMv7:
				platform = "armv7";
				break;
			case AARCH64:
				platform = "arm64";
				break;
			default:
				platform = "Unknown";
				break;
		}

		PROMETHEUS_FILE_NAME = base + os + "-" + platform;
	}

	private static final Pattern LOG_REPORTER_PORT_PATTERN = Pattern.compile(".*Started PrometheusReporter HTTP server on port ([0-9]+).*");

	@BeforeClass
	public static void checkOS() {
		Assume.assumeFalse("This test does not run on Windows.", OperatingSystem.isWindows());
	}

	@Parameterized.Parameters(name = "{index}: {0}")
	public static Collection<TestParams> testParameters() {
		return Arrays.asList(
			TestParams.from("Jar in 'lib'",
				builder -> builder.moveJar(PROMETHEUS_JAR_PREFIX, JarLocation.PLUGINS, JarLocation.LIB),
				REFLECTION),
			TestParams.from("Jar in 'lib'",
				builder -> builder.moveJar(PROMETHEUS_JAR_PREFIX, JarLocation.PLUGINS, JarLocation.LIB),
				FACTORY),
			TestParams.from("Jar in 'plugins'",
				builder -> {},
				REFLECTION),
			TestParams.from("Jar in 'plugins'",
				builder -> {},
				FACTORY),
			TestParams.from("Jar in 'lib' and 'plugins'",
				builder -> {
					builder.copyJar(PROMETHEUS_JAR_PREFIX, JarLocation.PLUGINS, JarLocation.LIB);
				},
				REFLECTION),
			TestParams.from("Jar in 'lib' and 'plugins'",
				builder -> {
					builder.copyJar(PROMETHEUS_JAR_PREFIX, JarLocation.PLUGINS, JarLocation.LIB);
				},
				FACTORY)
		);
	}

	@Rule
	public final FlinkResource dist;

	public PrometheusReporterEndToEndITCase(TestParams params) {
		final FlinkResourceSetup.FlinkResourceSetupBuilder builder = FlinkResourceSetup.builder();
		params.getBuilderSetup().accept(builder);
		builder.addConfiguration(getFlinkConfig(params.getInstantiationType()));
		dist = new LocalStandaloneFlinkResourceFactory().create(builder.build());
	}

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Rule
	public final DownloadCache downloadCache = DownloadCache.get();

	private static Configuration getFlinkConfig(TestParams.InstantiationType instantiationType) {
		final Configuration config = new Configuration();

		switch (instantiationType) {
			case FACTORY:
				config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "prom." + ConfigConstants.METRICS_REPORTER_FACTORY_CLASS_SUFFIX, PrometheusReporterFactory.class.getName());
				break;
			case REFLECTION:
				config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "prom." + ConfigConstants.METRICS_REPORTER_CLASS_SUFFIX, PrometheusReporter.class.getCanonicalName());
		}

		config.setString(ConfigConstants.METRICS_REPORTER_PREFIX + "prom.port", "9000-9100");
		return config;
	}

	@Test
	public void testReporter() throws Exception {
		final Path tmpPrometheusDir = tmp.newFolder().toPath().resolve("prometheus");
		final Path prometheusBinDir = tmpPrometheusDir.resolve(PROMETHEUS_FILE_NAME);
		final Path prometheusConfig = prometheusBinDir.resolve("prometheus.yml");
		final Path prometheusBinary = prometheusBinDir.resolve("prometheus");
		Files.createDirectory(tmpPrometheusDir);

		final Path prometheusArchive = downloadCache.getOrDownload(
			"https://github.com/prometheus/prometheus/releases/download/v" + PROMETHEUS_VERSION + '/' + PROMETHEUS_FILE_NAME + ".tar.gz",
			tmpPrometheusDir
		);

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

		try (ClusterController ignored = dist.startCluster(1)) {

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

	static class TestParams {
		private final String jarLocationDescription;
		private final Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup;
		private final InstantiationType instantiationType;

		private TestParams(String jarLocationDescription, Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup, InstantiationType instantiationType) {
			this.jarLocationDescription = jarLocationDescription;
			this.builderSetup = builderSetup;
			this.instantiationType = instantiationType;
		}

		public static TestParams from(String jarLocationDesription, Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup, InstantiationType instantiationType) {
			return new TestParams(jarLocationDesription, builderSetup, instantiationType);
		}

		public Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> getBuilderSetup() {
			return builderSetup;
		}

		public InstantiationType getInstantiationType() {
			return instantiationType;
		}

		@Override
		public String toString() {
			return jarLocationDescription + ", instantiated via " + instantiationType.name().toLowerCase();
		}

		public enum InstantiationType {
			REFLECTION,
			FACTORY
		}
	}
}
