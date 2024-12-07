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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.rest.messages.job.coordination.ClientCoordinationRequestBody;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationRequest;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorCoordinator;
import org.apache.flink.test.junit5.InjectClusterClient;
import org.apache.flink.test.junit5.InjectClusterRESTAddress;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMethod;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the WebFrontend. */
class WebFrontendITCase {

    private static final Logger LOG = LoggerFactory.getLogger(WebFrontendITCase.class);

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int NUM_SLOTS = 4;

    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();
    private static final Configuration CLUSTER_CONFIGURATION = getClusterConfiguration();

    @RegisterExtension
    private static final MiniClusterExtension CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(CLUSTER_CONFIGURATION)
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS)
                            .build());

    private static Configuration getClusterConfiguration() {
        Configuration config = new Configuration();
        try {
            Path logDir = Files.createTempFile("TestBaseUtils-logdir", null);
            Files.delete(logDir);
            Files.createDirectories(logDir);
            Path logFile = logDir.resolve("jobmanager.log");
            Path outFile = logDir.resolve("jobmanager.out");

            Files.createFile(logFile);
            Files.createFile(outFile);

            config.set(WebOptions.LOG_PATH, logFile.toAbsolutePath().toString());
            config.set(
                    TaskManagerOptions.TASK_MANAGER_LOG_PATH, logFile.toAbsolutePath().toString());
        } catch (Exception e) {
            throw new AssertionError("Could not setup test.", e);
        }

        // !!DO NOT REMOVE!! next line is required for tests
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("12m"));

        return config;
    }

    @AfterEach
    void tearDown() {
        BlockingInvokable.reset();
    }

    @Test
    void getFrontPage(@InjectClusterRESTAddress URI restAddress) throws Exception {
        String fromHTTP = getFromHTTP("http://localhost:" + restAddress.getPort() + "/index.html");
        assertThat(fromHTTP).contains("Apache Flink Web Dashboard");
    }

    @Test
    void testResponseHeaders(@InjectClusterRESTAddress URI restAddress) throws Exception {
        // check headers for successful json response
        URL taskManagersUrl =
                new URL("http://localhost:" + restAddress.getPort() + "/taskmanagers");
        HttpURLConnection taskManagerConnection =
                (HttpURLConnection) taskManagersUrl.openConnection();
        taskManagerConnection.setConnectTimeout(100000);
        taskManagerConnection.connect();
        if (taskManagerConnection.getResponseCode() >= 400) {
            // error!
            InputStream is = taskManagerConnection.getErrorStream();
            String errorMessage = IOUtils.toString(is, ConfigConstants.DEFAULT_CHARSET);
            fail(errorMessage);
        }

        // we don't set the content-encoding header
        assertThat(taskManagerConnection.getContentEncoding()).isNull();
        assertThat(taskManagerConnection.getContentType())
                .isEqualTo("application/json; charset=UTF-8");

        // check headers in case of an error
        URL notFoundJobUrl =
                new URL("http://localhost:" + restAddress.getPort() + "/jobs/dontexist");
        HttpURLConnection notFoundJobConnection =
                (HttpURLConnection) notFoundJobUrl.openConnection();
        notFoundJobConnection.setConnectTimeout(100000);
        notFoundJobConnection.connect();
        assertThat(notFoundJobConnection)
                .satisfies(c -> assertThat(c.getResponseCode()).isGreaterThanOrEqualTo(400))
                // we don't set the content-encoding header
                .satisfies(c -> assertThat(c.getContentEncoding()).isNull())
                .satisfies(
                        c ->
                                assertThat(c.getContentType())
                                        .isEqualTo("application/json; charset=UTF-8"));
    }

    @Test
    void getNumberOfTaskManagers(@InjectClusterRESTAddress URI restAddress) throws Exception {
        String json = getFromHTTP("http://localhost:" + restAddress.getPort() + "/taskmanagers/");

        JsonNode response = OBJECT_MAPPER.readTree(json);
        ArrayNode taskManagers = (ArrayNode) response.get("taskmanagers");

        assertThat(taskManagers).hasSize(NUM_TASK_MANAGERS);
    }

    @Test
    void getTaskManagers(@InjectClusterRESTAddress URI restAddress) throws Exception {
        String json = getFromHTTP("http://localhost:" + restAddress.getPort() + "/taskmanagers/");

        JsonNode parsed = OBJECT_MAPPER.readTree(json);
        ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");

        assertThat(taskManagers).hasSize(NUM_TASK_MANAGERS);

        JsonNode taskManager = taskManagers.get(0);
        assertThat(taskManager).isNotNull();
        assertThat(taskManager.get("slotsNumber").asInt()).isEqualTo(NUM_SLOTS);
        assertThat(taskManager.get("freeSlots").asInt()).isLessThanOrEqualTo(NUM_SLOTS);
    }

    @Test
    void getLogAndStdoutFiles(@InjectClusterRESTAddress URI restAddress) throws Exception {
        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
        String logs = getFromHTTP("http://localhost:" + restAddress.getPort() + "/jobmanager/log");
        assertThat(logs).contains("job manager log");

        FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
        logs = getFromHTTP("http://localhost:" + restAddress.getPort() + "/jobmanager/stdout");
        assertThat(logs).contains("job manager out");
    }

    @Test
    void getCustomLogFiles(@InjectClusterRESTAddress URI restAddress) throws Exception {
        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        String customFileName = "test.log";
        final String logDir = logFiles.logFile.getParent();
        final String expectedLogContent = "job manager custom log";
        FileUtils.writeStringToFile(new File(logDir, customFileName), expectedLogContent);

        String logs =
                getFromHTTP(
                        "http://localhost:"
                                + restAddress.getPort()
                                + "/jobmanager/logs/"
                                + customFileName);
        assertThat(logs).contains(expectedLogContent);
    }

    @Test
    void getTaskManagerLogAndStdoutFiles(@InjectClusterRESTAddress URI restAddress)
            throws Exception {
        String json = getFromHTTP("http://localhost:" + restAddress.getPort() + "/taskmanagers/");

        JsonNode parsed = OBJECT_MAPPER.readTree(json);
        ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");
        JsonNode taskManager = taskManagers.get(0);
        String id = taskManager.get("id").asText();

        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        // we check for job manager log files, since no separate taskmanager logs exist
        FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
        String logs =
                getFromHTTP(
                        "http://localhost:"
                                + restAddress.getPort()
                                + "/taskmanagers/"
                                + id
                                + "/log");
        assertThat(logs).contains("job manager log");

        FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
        logs =
                getFromHTTP(
                        "http://localhost:"
                                + restAddress.getPort()
                                + "/taskmanagers/"
                                + id
                                + "/stdout");
        assertThat(logs).contains("job manager out");
    }

    @Test
    void getConfiguration(@InjectClusterRESTAddress URI restAddress) throws Exception {
        String config =
                getFromHTTP("http://localhost:" + restAddress.getPort() + "/jobmanager/config");
        Map<String, String> conf = fromKeyValueJsonArray(config);

        MemorySize expected = CLUSTER_CONFIGURATION.get(TaskManagerOptions.MANAGED_MEMORY_SIZE);
        MemorySize actual =
                MemorySize.parse(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE.key()));

        assertThat(actual).isEqualTo(expected);
    }

    private static Map<String, String> fromKeyValueJsonArray(String jsonString) {
        try {
            Map<String, String> map = new HashMap<>();
            ArrayNode array = (ArrayNode) OBJECT_MAPPER.readTree(jsonString);

            Iterator<JsonNode> elements = array.elements();
            while (elements.hasNext()) {
                JsonNode node = elements.next();
                String key = node.get("key").asText();
                String value = node.get("value").asText();
                map.put(key, value);
            }

            return map;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Test
    void testCancel(
            @InjectClusterClient ClusterClient<?> clusterClient,
            @InjectClusterRESTAddress URI restAddress)
            throws Exception {
        // this only works if there is no active job at this point
        assertThat(getRunningJobs(clusterClient).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobName("Stoppable streaming test job")
                        .addJobVertex(sender)
                        .build();
        final JobID jid = jobGraph.getJobID();

        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);
        final Deadline deadline = Deadline.fromNow(testTimeout);

        try (HttpTestClient client = new HttpTestClient("localhost", restAddress.getPort())) {
            // cancel the job
            client.sendPatchRequest("/jobs/" + jid + "/", deadline.timeLeft());
            HttpTestClient.SimpleHttpResponse response =
                    client.getNextResponse(deadline.timeLeft());

            assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.ACCEPTED);
            assertThat(response.getType()).isEqualTo("application/json; charset=UTF-8");
            assertThat(response.getContent()).isEqualTo("{}");
        }

        // wait for cancellation to finish
        while (!getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(20);
        }

        // ensure we can access job details when its finished (FLINK-4011)
        try (HttpTestClient client = new HttpTestClient("localhost", restAddress.getPort())) {
            Duration timeout = Duration.ofSeconds(30);
            client.sendGetRequest("/jobs/" + jid + "/config", timeout);
            HttpTestClient.SimpleHttpResponse response = client.getNextResponse(timeout);

            assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.OK);
            assertThat(response.getType()).isEqualTo("application/json; charset=UTF-8");
            assertThat(response.getContent())
                    .isEqualTo(
                            "{\"jid\":\""
                                    + jid
                                    + "\",\"name\":\"Stoppable streaming test job\","
                                    + "\"execution-config\":{\"restart-strategy\":\"Cluster level default restart strategy\","
                                    + "\"job-parallelism\":1,\"object-reuse-mode\":false,\"user-config\":{}}}");
        }

        BlockingInvokable.reset();
    }

    /** See FLINK-19518. This test ensures that the /jobs/overview handler shows a duration != 0. */
    @Test
    void testJobOverviewHandler(
            @InjectClusterClient ClusterClient<?> clusterClient,
            @InjectClusterRESTAddress URI restAddress)
            throws Exception {
        // this only works if there is no active job at this point
        assertThat(getRunningJobs(clusterClient).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender);

        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);

        String json = getFromHTTP("http://localhost:" + restAddress.getPort() + "/jobs/overview");

        JsonNode parsed = OBJECT_MAPPER.readTree(json);
        ArrayNode jsonJobs = (ArrayNode) parsed.get("jobs");
        assertThat(jsonJobs.size()).isEqualTo(1);
        assertThat(jsonJobs.get(0).get("duration").asInt()).isGreaterThan(0);

        clusterClient.cancel(jobGraph.getJobID()).get();

        // ensure cancellation is finished
        while (!getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(20);
        }

        BlockingInvokable.reset();
    }

    @Test
    void testClientCoordinationHandler(
            @InjectClusterClient ClusterClient<?> clusterClient,
            @InjectClusterRESTAddress URI restAddress)
            throws Exception {
        // this only works if there is no active job at this point
        assertThat(getRunningJobs(clusterClient).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        String operatorUid = "opId";
        OperatorID opId = StreamingJobGraphGenerator.generateOperatorID(operatorUid);
        sender.addOperatorCoordinator(
                new SerializedValue<>(new CollectSinkOperatorCoordinator.Provider(opId, 10_000)));

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender);

        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        SerializedValue<CoordinationRequest> serializedRequest =
                new SerializedValue<>(new CollectCoordinationRequest("version", 0L));
        ClientCoordinationRequestBody requestBody =
                new ClientCoordinationRequestBody(serializedRequest);
        String json =
                postAndGetHttpResponse(
                        "http://localhost:"
                                + restAddress.getPort()
                                + "/jobs/"
                                + jobGraph.getJobID()
                                + "/coordinators/"
                                + operatorUid,
                        OBJECT_MAPPER.writeValueAsString(requestBody));

        JsonNode parsed = OBJECT_MAPPER.readTree(json);
        assertThat(parsed.get("serializedCoordinationResult")).isNotNull();

        clusterClient.cancel(jobGraph.getJobID()).get();

        // ensure cancellation is finished
        while (!getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(20);
        }

        BlockingInvokable.reset();
    }

    @Test
    void testCancelYarn(
            @InjectClusterClient ClusterClient<?> clusterClient,
            @InjectClusterRESTAddress URI restAddress)
            throws Exception {
        // this only works if there is no active job at this point
        assertThat(getRunningJobs(clusterClient).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender);
        final JobID jid = jobGraph.getJobID();

        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);
        final Deadline deadline = Deadline.fromNow(testTimeout);

        try (HttpTestClient client = new HttpTestClient("localhost", restAddress.getPort())) {
            // Request the file from the web server
            client.sendGetRequest("/jobs/" + jid + "/yarn-cancel", deadline.timeLeft());

            HttpTestClient.SimpleHttpResponse response =
                    client.getNextResponse(deadline.timeLeft());

            assertThat(response.getStatus()).isEqualTo(HttpResponseStatus.ACCEPTED);
            assertThat(response.getType()).isEqualTo("application/json; charset=UTF-8");
            assertThat(response.getContent()).isEqualTo("{}");
        }

        // wait for cancellation to finish
        while (!getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(20);
        }

        BlockingInvokable.reset();
    }

    private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    private static String getFromHTTP(String url) throws Exception {
        final URL u = new URL(url);
        LOG.info("Accessing URL " + url + " as URL: " + u);

        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10L));

        while (deadline.hasTimeLeft()) {
            HttpURLConnection connection = (HttpURLConnection) u.openConnection();
            connection.setConnectTimeout(100000);
            connection.connect();

            if (Objects.equals(
                    HttpResponseStatus.SERVICE_UNAVAILABLE,
                    HttpResponseStatus.valueOf(connection.getResponseCode()))) {
                // service not available --> Sleep and retry
                LOG.debug("Web service currently not available. Retrying the request in a bit.");
                Thread.sleep(100L);
            } else {
                InputStream is;

                if (connection.getResponseCode() >= 400) {
                    // error!
                    LOG.warn(
                            "HTTP Response code when connecting to {} was {}",
                            url,
                            connection.getResponseCode());
                    is = connection.getErrorStream();
                } else {
                    is = connection.getInputStream();
                }

                return IOUtils.toString(is, ConfigConstants.DEFAULT_CHARSET);
            }
        }

        throw new TimeoutException(
                "Could not get HTTP response in time since the service is still unavailable.");
    }

    private static String postAndGetHttpResponse(String url, String jsonData) throws Exception {
        final URL u = new URL(url);
        LOG.info("Accessing URL " + url + " as URL: " + u);

        final Deadline deadline = Deadline.fromNow(Duration.ofSeconds(10L));

        while (deadline.hasTimeLeft()) {
            HttpURLConnection connection = (HttpURLConnection) u.openConnection();
            connection.setConnectTimeout(100000);
            connection.setRequestMethod(HttpMethod.POST.name());
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = jsonData.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            if (Objects.equals(
                    HttpResponseStatus.SERVICE_UNAVAILABLE,
                    HttpResponseStatus.valueOf(connection.getResponseCode()))) {
                // service not available --> Sleep and retry
                LOG.debug("Web service currently not available. Retrying the request in a bit.");
                Thread.sleep(100L);
            } else {
                InputStream is;

                if (connection.getResponseCode() >= 400) {
                    // error!
                    LOG.warn(
                            "HTTP Response code when connecting to {} was {}",
                            url,
                            connection.getResponseCode());
                    is = connection.getErrorStream();
                } else {
                    is = connection.getInputStream();
                }

                return IOUtils.toString(is, ConfigConstants.DEFAULT_CHARSET);
            }
        }

        throw new TimeoutException(
                "Could not get HTTP response in time since the service is still unavailable.");
    }

    /** Test invokable that allows waiting for all subtasks to be running. */
    public static class BlockingInvokable extends AbstractInvokable {

        private static CountDownLatch latch = new CountDownLatch(2);

        private volatile boolean isRunning = true;

        public BlockingInvokable(Environment environment) {
            super(environment);
        }

        @Override
        public void invoke() throws Exception {
            latch.countDown();
            while (isRunning) {
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            this.isRunning = false;
        }

        public static void reset() {
            latch = new CountDownLatch(2);
        }
    }
}
