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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.metrics.prometheus.PrometheusReporterFactory;
import org.apache.flink.tests.util.AutoClosableProcess;
import org.apache.flink.tests.util.CommandLineWrapper;
import org.apache.flink.tests.util.cache.DownloadCacheExtension;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResourceExtension;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.ProcessorArchitecture;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
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

import static org.apache.flink.tests.util.AutoClosableProcess.runBlocking;
import static org.apache.flink.tests.util.AutoClosableProcess.runNonBlocking;
import static org.assertj.core.api.Assumptions.assumeThat;

/** End-to-end test for the PrometheusReporter. */
@ExtendWith({ParameterizedTestExtension.class, TestLoggerExtension.class})
class PrometheusReporterEndToEndITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(PrometheusReporterEndToEndITCase.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String PROMETHEUS_VERSION = "3.11.2";
    private static final String PROMETHEUS_JAR_PREFIX = "flink-metrics-prometheus";

    private static String prometheusFileName;

    private static final Pattern LOG_REPORTER_PORT_PATTERN =
            Pattern.compile(".*Started PrometheusReporter HTTP server on port ([0-9]+).*");

    private static String getPrometheusFileName() {
        String base = "prometheus-" + PROMETHEUS_VERSION + '.';
        String os;
        String platform;
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

        return String.format("%s%s-%s", base, os, platform);
    }

    @BeforeAll
    static void beforeAll() {
        assumeThat(OperatingSystem.isWindows()).as("This test does not run on Windows.").isFalse();
        prometheusFileName = getPrometheusFileName();
    }

    @Parameters(name = "{0}")
    static Collection<TestParams> testParameters() {
        return Arrays.asList(
                TestParams.from(
                        "Jar in 'lib'",
                        builder ->
                                builder.moveJar(
                                        PROMETHEUS_JAR_PREFIX,
                                        JarLocation.PLUGINS,
                                        JarLocation.LIB)),
                TestParams.from("Jar in 'plugins'", builder -> {}),
                TestParams.from(
                        "Jar in 'lib' and 'plugins'",
                        builder -> {
                            builder.copyJar(
                                    PROMETHEUS_JAR_PREFIX, JarLocation.PLUGINS, JarLocation.LIB);
                        }));
    }

    @RegisterExtension private final FlinkResourceExtension dist;

    PrometheusReporterEndToEndITCase(TestParams params) {
        final FlinkResourceSetup.FlinkResourceSetupBuilder builder = FlinkResourceSetup.builder();
        params.getBuilderSetup().accept(builder);
        builder.addConfiguration(getFlinkConfig());
        dist =
                new FlinkResourceExtension(
                        new LocalStandaloneFlinkResourceFactory().create(builder.build()));
    }

    @TempDir private Path tmp;

    @RegisterExtension
    private final DownloadCacheExtension downloadCacheExtension = new DownloadCacheExtension();

    private static Configuration getFlinkConfig() {
        final Configuration config = new Configuration();

        MetricOptions.forReporter(config, "prom")
                .set(
                        MetricOptions.REPORTER_FACTORY_CLASS,
                        PrometheusReporterFactory.class.getName())
                .setString("port", "9000-9100");

        return config;
    }

    @TestTemplate
    void testReporter() throws Exception {
        final Path tmpPrometheusDir = tmp.resolve("prometheus");
        final Path prometheusBinDir = tmpPrometheusDir.resolve(prometheusFileName);
        final Path prometheusConfig = prometheusBinDir.resolve("prometheus.yml");
        final Path prometheusBinary = prometheusBinDir.resolve("prometheus");
        Files.createDirectory(tmpPrometheusDir);

        final Path prometheusArchive =
                downloadCacheExtension.getOrDownload(
                        "https://github.com/prometheus/prometheus/releases/download/v"
                                + PROMETHEUS_VERSION
                                + '/'
                                + prometheusFileName
                                + ".tar.gz",
                        tmpPrometheusDir);

        LOG.info("Unpacking Prometheus.");
        runBlocking(
                CommandLineWrapper.tar(prometheusArchive)
                        .extract()
                        .zipped()
                        .targetDir(tmpPrometheusDir)
                        .build());

        LOG.info("Setting Prometheus scrape interval.");
        runBlocking(
                CommandLineWrapper.sed("s/\\(scrape_interval:\\).*/\\1 1s/", prometheusConfig)
                        .inPlace()
                        .build());

        try (ClusterController ignored = dist.getFlinkResource().startCluster(1)) {

            final List<Integer> ports =
                    dist.getFlinkResource()
                            .searchAllLogs(LOG_REPORTER_PORT_PATTERN, matcher -> matcher.group(1))
                            .map(Integer::valueOf)
                            .collect(Collectors.toList());

            final String scrapeTargets =
                    ports.stream()
                            .map(port -> "'localhost:" + port + "'")
                            .collect(Collectors.joining(", "));

            LOG.info("Setting Prometheus scrape targets to {}.", scrapeTargets);
            runBlocking(
                    CommandLineWrapper.sed(
                                    "s/\\(targets:\\).*/\\1 [" + scrapeTargets + "]/",
                                    prometheusConfig)
                            .inPlace()
                            .build());

            LOG.info("Starting Prometheus server.");
            try (AutoClosableProcess prometheus =
                    runNonBlocking(
                            prometheusBinary.toAbsolutePath().toString(),
                            "--config.file=" + prometheusConfig.toAbsolutePath(),
                            "--storage.tsdb.path="
                                    + prometheusBinDir.resolve("data").toAbsolutePath())) {

                final OkHttpClient client = new OkHttpClient();

                checkMetricAvailability(client, "flink_jobmanager_numRegisteredTaskManagers");
                checkMetricAvailability(
                        client, "flink_taskmanager_Status_Shuffle_Netty_TotalMemorySegments");
            }
        }
    }

    private static void checkMetricAvailability(final OkHttpClient client, final String metric)
            throws InterruptedException {
        final Request jobManagerRequest =
                new Request.Builder()
                        .get()
                        .url("http://localhost:9090/api/v1/query?query=" + metric)
                        .build();

        Exception reportedException = null;
        for (int x = 0; x < 30; x++) {
            try (Response response = client.newCall(jobManagerRequest).execute()) {
                if (response.isSuccessful()) {
                    final String json = response.body().string();

                    // Sample response:
                    // {
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
                    // }
                    OBJECT_MAPPER
                            .readTree(json)
                            .get("data")
                            .get("result")
                            .get(0)
                            .get("value")
                            .get(1)
                            .asInt();
                    // if we reach this point some value for the given metric was reported to
                    // prometheus
                    return;
                } else {
                    LOG.info(
                            "Retrieving metric failed. Retrying... "
                                    + response.code()
                                    + ":"
                                    + response.message());
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                reportedException = ExceptionUtils.firstOrSuppressed(e, reportedException);
                Thread.sleep(1000);
            }
        }
        throw new AssertionError(
                "Could not retrieve metric " + metric + " from Prometheus.", reportedException);
    }

    private static class TestParams {
        private final String jarLocationDescription;
        private final Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup;

        private TestParams(
                String jarLocationDescription,
                Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup) {
            this.jarLocationDescription = jarLocationDescription;
            this.builderSetup = builderSetup;
        }

        private static TestParams from(
                String jarLocationDesription,
                Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup) {
            return new TestParams(jarLocationDesription, builderSetup);
        }

        private Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> getBuilderSetup() {
            return builderSetup;
        }

        @Override
        public String toString() {
            return jarLocationDescription;
        }
    }
}
