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
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.runtime.webmonitor.testutils.HttpTestClient;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the WebFrontend. */
public class WebFrontendITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int NUM_SLOTS = 4;

    private static final Configuration CLUSTER_CONFIGURATION = getClusterConfiguration();

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(CLUSTER_CONFIGURATION)
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUM_SLOTS)
                            .build());

    private static Configuration getClusterConfiguration() {
        Configuration config = new Configuration();
        try {
            File logDir = File.createTempFile("TestBaseUtils-logdir", null);
            assertTrue("Unable to delete temp file", logDir.delete());
            assertTrue("Unable to create temp directory", logDir.mkdir());
            File logFile = new File(logDir, "jobmanager.log");
            File outFile = new File(logDir, "jobmanager.out");

            Files.createFile(logFile.toPath());
            Files.createFile(outFile.toPath());

            config.setString(WebOptions.LOG_PATH, logFile.getAbsolutePath());
            config.setString(ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, logFile.getAbsolutePath());
        } catch (Exception e) {
            throw new AssertionError("Could not setup test.", e);
        }

        // !!DO NOT REMOVE!! next line is required for tests
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("12m"));

        return config;
    }

    @After
    public void tearDown() {
        BlockingInvokable.reset();
    }

    @Test
    public void getFrontPage() throws Exception {
        String fromHTTP =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/index.html");
        assertThat(fromHTTP, containsString("Apache Flink Web Dashboard"));
    }

    private int getRestPort() {
        return CLUSTER.getRestAddres().getPort();
    }

    @Test
    public void testResponseHeaders() throws Exception {
        // check headers for successful json response
        URL taskManagersUrl = new URL("http://localhost:" + getRestPort() + "/taskmanagers");
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
        Assert.assertNull(taskManagerConnection.getContentEncoding());
        Assert.assertEquals(
                "application/json; charset=UTF-8", taskManagerConnection.getContentType());

        // check headers in case of an error
        URL notFoundJobUrl = new URL("http://localhost:" + getRestPort() + "/jobs/dontexist");
        HttpURLConnection notFoundJobConnection =
                (HttpURLConnection) notFoundJobUrl.openConnection();
        notFoundJobConnection.setConnectTimeout(100000);
        notFoundJobConnection.connect();
        if (notFoundJobConnection.getResponseCode() >= 400) {
            // we don't set the content-encoding header
            Assert.assertNull(notFoundJobConnection.getContentEncoding());
            Assert.assertEquals(
                    "application/json; charset=UTF-8", notFoundJobConnection.getContentType());
        } else {
            fail("Request for non-existing job did not return an error.");
        }
    }

    @Test
    public void getNumberOfTaskManagers() throws Exception {
        String json =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/taskmanagers/");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode response = mapper.readTree(json);
        ArrayNode taskManagers = (ArrayNode) response.get("taskmanagers");

        assertNotNull(taskManagers);
        assertEquals(NUM_TASK_MANAGERS, taskManagers.size());
    }

    @Test
    public void getTaskManagers() throws Exception {
        String json =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/taskmanagers/");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");

        assertNotNull(taskManagers);
        assertEquals(NUM_TASK_MANAGERS, taskManagers.size());

        JsonNode taskManager = taskManagers.get(0);
        assertNotNull(taskManager);
        assertEquals(NUM_SLOTS, taskManager.get("slotsNumber").asInt());
        assertTrue(taskManager.get("freeSlots").asInt() <= NUM_SLOTS);
    }

    @Test
    public void getLogAndStdoutFiles() throws Exception {
        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
        String logs =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/jobmanager/log");
        assertThat(logs, containsString("job manager log"));

        FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
        logs =
                TestBaseUtils.getFromHTTP(
                        "http://localhost:" + getRestPort() + "/jobmanager/stdout");
        assertThat(logs, containsString("job manager out"));
    }

    @Test
    public void getCustomLogFiles() throws Exception {
        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        String customFileName = "test.log";
        final String logDir = logFiles.logFile.getParent();
        final String expectedLogContent = "job manager custom log";
        FileUtils.writeStringToFile(new File(logDir, customFileName), expectedLogContent);

        String logs =
                TestBaseUtils.getFromHTTP(
                        "http://localhost:" + getRestPort() + "/jobmanager/logs/" + customFileName);
        assertThat(logs, containsString(expectedLogContent));
    }

    @Test
    public void getTaskManagerLogAndStdoutFiles() throws Exception {
        String json =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/taskmanagers/");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        ArrayNode taskManagers = (ArrayNode) parsed.get("taskmanagers");
        JsonNode taskManager = taskManagers.get(0);
        String id = taskManager.get("id").asText();

        WebMonitorUtils.LogFileLocation logFiles =
                WebMonitorUtils.LogFileLocation.find(CLUSTER_CONFIGURATION);

        // we check for job manager log files, since no separate taskmanager logs exist
        FileUtils.writeStringToFile(logFiles.logFile, "job manager log");
        String logs =
                TestBaseUtils.getFromHTTP(
                        "http://localhost:" + getRestPort() + "/taskmanagers/" + id + "/log");
        assertThat(logs, containsString("job manager log"));

        FileUtils.writeStringToFile(logFiles.stdOutFile, "job manager out");
        logs =
                TestBaseUtils.getFromHTTP(
                        "http://localhost:" + getRestPort() + "/taskmanagers/" + id + "/stdout");
        assertThat(logs, containsString("job manager out"));
    }

    @Test
    public void getConfiguration() throws Exception {
        String config =
                TestBaseUtils.getFromHTTP(
                        "http://localhost:" + getRestPort() + "/jobmanager/config");
        Map<String, String> conf = WebMonitorUtils.fromKeyValueJsonArray(config);

        MemorySize expected = CLUSTER_CONFIGURATION.get(TaskManagerOptions.MANAGED_MEMORY_SIZE);
        MemorySize actual =
                MemorySize.parse(conf.get(TaskManagerOptions.MANAGED_MEMORY_SIZE.key()));

        assertEquals(expected, actual);
    }

    @Test
    public void testCancel() throws Exception {
        // this only works if there is no active job at this point
        assertTrue(getRunningJobs(CLUSTER.getClusterClient()).isEmpty());

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

        ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);
        final Deadline deadline = Deadline.fromNow(testTimeout);

        try (HttpTestClient client = new HttpTestClient("localhost", getRestPort())) {
            // cancel the job
            client.sendPatchRequest("/jobs/" + jid + "/", deadline.timeLeft());
            HttpTestClient.SimpleHttpResponse response =
                    client.getNextResponse(deadline.timeLeft());

            assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
            assertEquals("application/json; charset=UTF-8", response.getType());
            assertEquals("{}", response.getContent());
        }

        // wait for cancellation to finish
        while (!getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
            Thread.sleep(20);
        }

        // ensure we can access job details when its finished (FLINK-4011)
        try (HttpTestClient client = new HttpTestClient("localhost", getRestPort())) {
            Duration timeout = Duration.ofSeconds(30);
            client.sendGetRequest("/jobs/" + jid + "/config", timeout);
            HttpTestClient.SimpleHttpResponse response = client.getNextResponse(timeout);

            assertEquals(HttpResponseStatus.OK, response.getStatus());
            assertEquals("application/json; charset=UTF-8", response.getType());
            assertEquals(
                    "{\"jid\":\""
                            + jid
                            + "\",\"name\":\"Stoppable streaming test job\","
                            + "\"execution-config\":{\"execution-mode\":\"PIPELINED\",\"restart-strategy\":\"Cluster level default restart strategy\","
                            + "\"job-parallelism\":1,\"object-reuse-mode\":false,\"user-config\":{}}}",
                    response.getContent());
        }

        BlockingInvokable.reset();
    }

    /** See FLINK-19518. This test ensures that the /jobs/overview handler shows a duration != 0. */
    @Test
    public void testJobOverviewHandler() throws Exception {
        // this only works if there is no active job at this point
        assertTrue(getRunningJobs(CLUSTER.getClusterClient()).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender);

        ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);

        String json =
                TestBaseUtils.getFromHTTP("http://localhost:" + getRestPort() + "/jobs/overview");

        ObjectMapper mapper = new ObjectMapper();
        JsonNode parsed = mapper.readTree(json);
        ArrayNode jsonJobs = (ArrayNode) parsed.get("jobs");
        assertEquals(1, jsonJobs.size());
        assertThat("Duration must be positive", jsonJobs.get(0).get("duration").asInt() > 0);

        clusterClient.cancel(jobGraph.getJobID()).get();

        // ensure cancellation is finished
        while (!getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
            Thread.sleep(20);
        }

        BlockingInvokable.reset();
    }

    @Test
    public void testCancelYarn() throws Exception {
        // this only works if there is no active job at this point
        assertTrue(getRunningJobs(CLUSTER.getClusterClient()).isEmpty());

        // Create a task
        final JobVertex sender = new JobVertex("Sender");
        sender.setParallelism(2);
        sender.setInvokableClass(BlockingInvokable.class);

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender);
        final JobID jid = jobGraph.getJobID();

        ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
        clusterClient.submitJob(jobGraph).get();

        // wait for job to show up
        while (getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
            Thread.sleep(10);
        }

        // wait for tasks to be properly running
        BlockingInvokable.latch.await();

        final Duration testTimeout = Duration.ofMinutes(2);
        final Deadline deadline = Deadline.fromNow(testTimeout);

        try (HttpTestClient client = new HttpTestClient("localhost", getRestPort())) {
            // Request the file from the web server
            client.sendGetRequest("/jobs/" + jid + "/yarn-cancel", deadline.timeLeft());

            HttpTestClient.SimpleHttpResponse response =
                    client.getNextResponse(deadline.timeLeft());

            assertEquals(HttpResponseStatus.ACCEPTED, response.getStatus());
            assertEquals("application/json; charset=UTF-8", response.getType());
            assertEquals("{}", response.getContent());
        }

        // wait for cancellation to finish
        while (!getRunningJobs(CLUSTER.getClusterClient()).isEmpty()) {
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
