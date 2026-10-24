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

package org.apache.flink.client.program.rest;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneClientHAServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.rest.messages.json.JobResultSerializer;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies, against the real {@link RestClusterClient}, that a REST response's {@code
 * serialized-throwable} bytes are never handed to {@code ObjectInputStream.readObject()} while
 * parsing a {@link JobResult} - only an explicit, later {@link
 * SerializedThrowable#deserializeError} call may do that.
 *
 * <p>{@code RestClusterClient} is used by callers that talk to a remote JobManager over the
 * network, so parsing a response must not assume the {@code serialized-throwable} bytes are
 * well-formed or otherwise trustworthy.
 */
class RestClusterClientJobResultSafetyTest {

    private static final AtomicBoolean MARKER_TRIGGERED = new AtomicBoolean(false);

    /** Fires {@link #MARKER_TRIGGERED} purely as a side effect of {@code readObject()}. */
    private static final class Marker implements Serializable {
        private static final long serialVersionUID = 1L;

        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            MARKER_TRIGGERED.set(true);
            in.defaultReadObject();
        }
    }

    @AfterEach
    void resetMarker() {
        MARKER_TRIGGERED.set(false);
    }

    @Test
    void parsingJobResultResponseNeverDeserializesSerializedThrowableBytes() throws Exception {
        assertThat(MARKER_TRIGGERED).isFalse();

        final byte[] markerBytes = serialize(new Marker());
        final String body =
                "{\"status\":{\"id\":\"COMPLETED\"},\"job-execution-result\":"
                        + "{\"id\":\"1bb5e8c7df49938733b7c6a73678de6a\",\"net-runtime\":0,"
                        + "\"application-status\":\"FAILED\","
                        + "\"failure-cause\":{"
                        + "\"class\":\"java.lang.RuntimeException\","
                        + "\"message\":\"boom\","
                        + "\"stack-trace\":\"java.lang.RuntimeException: boom\","
                        + "\"serialized-throwable\":\""
                        + Base64.getEncoder().encodeToString(markerBytes)
                        + "\"}}}";

        final JobResult jobResult = requestJobResultAgainstFakeServer(body);

        assertThat(MARKER_TRIGGERED)
                .as(
                        "parsing the response must not run ObjectInputStream.readObject() over "
                                + "serialized-throwable's bytes")
                .isFalse();

        assertThat(jobResult.getSerializedThrowable()).isPresent();
        final SerializedThrowable failureCause = jobResult.getSerializedThrowable().get();
        assertThat(failureCause.getMessage()).isEqualTo("boom");
        assertThat(failureCause.getOriginalErrorClassName())
                .isEqualTo("java.lang.RuntimeException");

        // Positive control: the marker mechanism does work, and the bytes are still there for
        // an explicit, later deserializeError() call to use.
        failureCause.deserializeError(ClassLoader.getSystemClassLoader());
        assertThat(MARKER_TRIGGERED)
                .as("deserializeError() is the one call site allowed to touch these bytes")
                .isTrue();
    }

    /** Negative control: a real, legitimate job failure response must not trigger the marker. */
    @Test
    void legitimateJobResultResponseDoesNotTriggerMarker() throws Exception {
        assertThat(MARKER_TRIGGERED).isFalse();

        final SerializedThrowable realFailure =
                new SerializedThrowable(new RuntimeException("job failed for real"));
        final JobResult realResult =
                new JobResult.Builder()
                        .jobId(new JobID())
                        .jobStatus(JobStatus.FAILED)
                        .netRuntime(1234L)
                        .serializedThrowable(realFailure)
                        .build();

        final ObjectMapper objectMapper = JacksonMapperFactory.createObjectMapper();
        final SimpleModule module = new SimpleModule();
        module.addSerializer(JobResult.class, new JobResultSerializer());
        objectMapper.registerModule(module);
        final String jobResultJson = objectMapper.writeValueAsString(realResult);
        final String body =
                "{\"status\":{\"id\":\"COMPLETED\"},\"job-execution-result\":"
                        + jobResultJson
                        + "}";

        final JobResult jobResult = requestJobResultAgainstFakeServer(body);

        assertThat(jobResult.getSerializedThrowable()).isPresent();
        assertThat(jobResult.getSerializedThrowable().get().getMessage())
                .isEqualTo("java.lang.RuntimeException: job failed for real");
        assertThat(MARKER_TRIGGERED).isFalse();
    }

    private static byte[] serialize(Serializable o) throws IOException {
        final java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(o);
        }
        return bos.toByteArray();
    }

    /**
     * Mirrors {@code AbstractFlinkService.getClusterClient(conf)} exactly: a fresh {@link
     * RestClusterClient} whose {@link StandaloneClientHAServices} points straight at our fake
     * server, then calls {@code requestJobResult(jobID)} - the same call {@code
     * AbstractFlinkService.requestJobResult()} makes.
     */
    private static JobResult requestJobResultAgainstFakeServer(String jsonBody) throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            final String restServerAddress = "http://localhost:" + serverSocket.getLocalPort();
            try (RestClusterClient<String> clusterClient =
                    new RestClusterClient<>(
                            new Configuration(),
                            "safety-test-cluster",
                            (c, e) -> new StandaloneClientHAServices(restServerAddress))) {

                final var responseFuture = clusterClient.requestJobResult(new JobID());

                serverSocket.setSoTimeout(10_000);
                try (Socket connection = serverSocket.accept()) {
                    writeHttpResponse(connection.getOutputStream(), jsonBody);
                    return responseFuture.get(10, TimeUnit.SECONDS);
                }
            }
        }
    }

    private static void writeHttpResponse(OutputStream out, String jsonBody) throws Exception {
        final byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
        final String headers =
                "HTTP/1.1 200 OK\r\n"
                        + "Content-Type: application/json; charset=UTF-8\r\n"
                        + "Content-Length: "
                        + bodyBytes.length
                        + "\r\n"
                        + "Connection: close\r\n"
                        + "\r\n";
        out.write(headers.getBytes(StandardCharsets.UTF_8));
        out.write(bodyBytes);
        out.flush();
    }
}
