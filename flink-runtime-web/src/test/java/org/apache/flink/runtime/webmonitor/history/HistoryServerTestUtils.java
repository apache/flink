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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.history.FsJsonArchivist;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.messages.JobsOverviewHeaders;
import org.apache.flink.runtime.webmonitor.history.retaining.ArchiveRetainedStrategy;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.webmonitor.history.HistoryServerArchiveFetcher.ArchiveEventType.CREATED;

/** Common utilities for history server tests. */
public class HistoryServerTestUtils {

    private static final JsonFactory JACKSON_FACTORY =
            new JsonFactory()
                    .enable(JsonGenerator.Feature.AUTO_CLOSE_TARGET)
                    .disable(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT);

    public static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();
    public static final ArchiveRetainedStrategy RETAIN_ALL = (file, index) -> true;

    static HistoryServer.RefreshLocation createRefreshLocation(File dir) throws Exception {
        Path path = new Path(dir.toURI().toString());
        FileSystem fs = path.getFileSystem();
        return new HistoryServer.RefreshLocation(path, fs);
    }

    /**
     * Create a job archive at {@code baseDir/<jobId>}.
     *
     * @param baseDir base directory under which the archive will be written
     * @param jobId job id used as the archive file name
     * @param withDetail whether to include the {@code /jobs/<jobId>/config} sub-archive
     * @return the archive {@link Path}
     */
    static Path createJobArchive(File baseDir, JobID jobId, boolean withDetail) throws Exception {
        MultipleJobsDetails overview =
                new MultipleJobsDetails(
                        Collections.singleton(
                                new JobDetails(
                                        jobId,
                                        "test-job",
                                        0L,
                                        1L,
                                        1L,
                                        JobStatus.FINISHED,
                                        1L,
                                        new int[ExecutionState.values().length],
                                        0)));
        String overviewJson = OBJECT_MAPPER.writeValueAsString(overview);
        ArchivedJson overviewArchive = new ArchivedJson(JobsOverviewHeaders.URL, overviewJson);

        // mock a simple /jobs/<jobid>.json
        String jobJson = "{\"jid\":\"" + jobId + "\"}";

        List<ArchivedJson> archives = new ArrayList<>();
        archives.add(overviewArchive);
        archives.add(new ArchivedJson("/jobs/" + jobId, jobJson));

        if (withDetail) {
            String detailJson = "{\"jid\":\"" + jobId + "\",\"name\":\"test-job\"}";
            archives.add(new ArchivedJson("/jobs/" + jobId + "/config", detailJson));
        }

        Path archivePath = new Path(baseDir.toURI().toString(), jobId.toString());
        FsJsonArchivist.writeArchivedJsons(archivePath, archives);
        return archivePath;
    }

    /** Create a legacy job archive at {@code directory/<jobId>} with only {@code /joboverview}. */
    public static JobID createLegacyArchive(java.nio.file.Path directory, boolean versionLessThan14)
            throws IOException {
        JobID jobId = JobID.generate();

        StringWriter sw = new StringWriter();
        try (JsonGenerator gen = JACKSON_FACTORY.createGenerator(sw)) {
            try (JsonObject root = new JsonObject(gen)) {
                try (JsonArray finished = new JsonArray(gen, "finished")) {
                    try (JsonObject job = new JsonObject(gen)) {
                        gen.writeStringField("jid", jobId.toString());
                        gen.writeStringField("name", "testjob");
                        gen.writeStringField("state", JobStatus.FINISHED.name());

                        gen.writeNumberField("start-time", 0L);
                        gen.writeNumberField("end-time", 1L);
                        gen.writeNumberField("duration", 1L);
                        gen.writeNumberField("last-modification", 1L);

                        try (JsonObject tasks = new JsonObject(gen, "tasks")) {
                            gen.writeNumberField("total", 0);

                            if (versionLessThan14) {
                                gen.writeNumberField("pending", 0);
                            } else {
                                gen.writeNumberField("created", 0);
                                gen.writeNumberField("deploying", 0);
                                gen.writeNumberField("scheduled", 0);
                            }
                            gen.writeNumberField("running", 0);
                            gen.writeNumberField("finished", 0);
                            gen.writeNumberField("canceling", 0);
                            gen.writeNumberField("canceled", 0);
                            gen.writeNumberField("failed", 0);
                        }
                    }
                }
            }
        }
        String json = sw.toString();
        ArchivedJson archivedJson = new ArchivedJson("/joboverview", json);
        FsJsonArchivist.writeArchivedJsons(
                new Path(directory.toAbsolutePath().toString(), jobId.toString()),
                Collections.singleton(archivedJson));

        return jobId;
    }

    public static JobID createLegacyArchiveWithModifiedDate(
            java.nio.file.Path directory, long fileModifiedDate, boolean versionLessThan14)
            throws IOException {
        JobID jobId = createLegacyArchive(directory, versionLessThan14);
        File jobArchive = directory.resolve(jobId.toString()).toFile();
        jobArchive.setLastModified(fileModifiedDate);
        return jobId;
    }

    /**
     * Wait until {@code cache.get(archiveId).eventType == CREATED}. Throws {@link AssertionError}
     * if the deadline is exceeded.
     */
    static void waitForArchiveLoaded(Map<String, ArchiveMetaInfo> cache, String archiveId)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            ArchiveMetaInfo metaInfo = cache.get(archiveId);
            if (metaInfo != null && metaInfo.getEventType() == CREATED) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError(
                "Timed out waiting for archive " + archiveId + " to reach CREATED state");
    }

    /**
     * Blocking archive storage for testing the LAZY load mode. Calls to {@link
     * #putArchiveContent(String, String)} whose key contains {@code blockOnKeyPrefix} will block
     * until {@link #releaseLatch} is counted down.
     *
     * <p>This is a decorator that delegates to a wrapped {@link ArchiveStorage}, so it can be used
     * with any storage backend (e.g. {@link FileArchiveStorage} or {@link RocksDBArchiveStorage}).
     */
    public static class BlockingArchiveStorage<Entry> implements ArchiveStorage<Entry> {

        public final CountDownLatch asyncStartLatch = new CountDownLatch(1);
        public final CountDownLatch releaseLatch = new CountDownLatch(1);

        private final ArchiveStorage<Entry> delegate;
        private final String blockOnKeyPrefix;

        public BlockingArchiveStorage(ArchiveStorage<Entry> delegate, String blockOnKeyPrefix) {
            this.delegate = delegate;
            this.blockOnKeyPrefix = blockOnKeyPrefix;
        }

        @Override
        public boolean exists(String key) throws IOException {
            return delegate.exists(key);
        }

        @Nullable
        @Override
        public Entry getEntry(String key) throws IOException {
            return delegate.getEntry(key);
        }

        @Override
        public void putArchiveContent(String key, String archiveContent) throws IOException {
            if (key.contains(blockOnKeyPrefix)) {
                asyncStartLatch.countDown();
                try {
                    boolean released = releaseLatch.await(10, TimeUnit.SECONDS);
                    if (!released) {
                        throw new IOException(
                                "BlockingArchiveStorage: timed out waiting for release latch");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("BlockingArchiveStorage: interrupted", e);
                }
            }
            delegate.putArchiveContent(key, archiveContent);
        }

        @Override
        public void delete(String key) throws IOException {
            delegate.delete(key);
        }

        @Override
        public void deleteEntriesByPrefix(String keyPrefix) throws IOException {
            delegate.deleteEntriesByPrefix(keyPrefix);
        }

        @Override
        public List<Entry> getEntriesByPrefix(String prefix) throws IOException {
            return delegate.getEntriesByPrefix(prefix);
        }

        @Override
        public String readArchiveContent(Entry entry) throws IOException {
            return delegate.readArchiveContent(entry);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static final class JsonObject implements AutoCloseable {

        private final JsonGenerator gen;

        JsonObject(JsonGenerator gen) throws IOException {
            this.gen = gen;
            gen.writeStartObject();
        }

        private JsonObject(JsonGenerator gen, String name) throws IOException {
            this.gen = gen;
            gen.writeObjectFieldStart(name);
        }

        @Override
        public void close() throws IOException {
            gen.writeEndObject();
        }
    }

    private static final class JsonArray implements AutoCloseable {

        private final JsonGenerator gen;

        JsonArray(JsonGenerator gen, String name) throws IOException {
            this.gen = gen;
            gen.writeArrayFieldStart(name);
        }

        @Override
        public void close() throws IOException {
            gen.writeEndArray();
        }
    }
}
