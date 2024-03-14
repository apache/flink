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

package org.apache.flink.runtime.jobmaster.event;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobEventStoreOptions;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link JobEventStore} that store all {@link JobEvent} on a {@link FileSystem}.
 */
public class FileSystemJobEventStore implements JobEventStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemJobEventStore.class);

    private static final String FILE_PREFIX = "event.";

    private static final int INITIAL_INDEX = 0;

    private final FileSystem fileSystem;
    private final Path workingDir;

    private FSDataOutputStream outputStream;
    private Path writeFile;
    private int writeIndex;
    private ScheduledExecutorService eventWriterExecutor;

    private DataInputStream inputStream;
    private int readIndex;
    private List<Path> readFiles;

    private volatile boolean corrupted = false;

    private final long flushIntervalInMs;
    private final int writeBufferSize;

    private final Map<Integer, SimpleVersionedSerializer<JobEvent>> jobEventSerializers =
            new HashMap<>();

    public FileSystemJobEventStore(JobID jobID, Configuration configuration) throws IOException {
        this(
                new Path(
                        HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(
                                configuration),
                        jobID.toString() + "/job-events"),
                configuration);
    }

    @VisibleForTesting
    public FileSystemJobEventStore(Path workingDir, Configuration configuration)
            throws IOException {
        this.workingDir = checkNotNull(workingDir);
        this.fileSystem = workingDir.getFileSystem();
        this.flushIntervalInMs = configuration.get(JobEventStoreOptions.FLUSH_INTERVAL).toMillis();
        this.writeBufferSize =
                (int) configuration.get(JobEventStoreOptions.WRITE_BUFFER_SIZE).getBytes();
        registerJobEventSerializers();
    }

    public void registerJobEventSerializer(
            int eventType, SimpleVersionedSerializer<JobEvent> simpleVersionedSerializer) {
        checkState(!jobEventSerializers.containsKey(eventType));
        jobEventSerializers.put(eventType, simpleVersionedSerializer);
    }

    public void registerJobEventSerializers() {
        registerJobEventSerializer(
                ExecutionVertexFinishedEvent.TYPE_ID,
                new ExecutionVertexFinishedEvent.Serializer());
        registerJobEventSerializer(
                ExecutionVertexResetEvent.TYPE_ID, new GenericJobEventSerializer());
        registerJobEventSerializer(
                ExecutionJobVertexInitializedEvent.TYPE_ID, new GenericJobEventSerializer());
    }

    @VisibleForTesting
    Path getWorkingDir() {
        return workingDir;
    }

    @VisibleForTesting
    ScheduledExecutorService getEventWriterExecutor() {
        return eventWriterExecutor;
    }

    @VisibleForTesting
    FSDataOutputStream getOutputStream() {
        return outputStream;
    }

    @Override
    public void start() throws IOException {
        // create job event dir.
        if (!fileSystem.exists(workingDir)) {
            fileSystem.mkdirs(workingDir);
            LOG.info("Create job event dir {}.", workingDir);
        }

        // get read files.
        try {
            readIndex = 0;
            readFiles = getAllJobEventPaths();
        } catch (IOException e) {
            throw new IOException("Cannot init filesystem job event store.", e);
        }
        // set write index
        writeIndex = readFiles.size();

        // create job event write executor.
        this.eventWriterExecutor = createJobEventWriterExecutor();
        eventWriterExecutor.scheduleAtFixedRate(
                () -> {
                    if (outputStream != null && !corrupted) {
                        try {
                            outputStream.flush();
                        } catch (Exception e) {
                            LOG.warn(
                                    "Flush event file {} meet error, will not record event any more.",
                                    writeFile,
                                    e);
                            corrupted = true;
                            closeOutputStream();
                        }
                    }
                },
                0L,
                flushIntervalInMs,
                TimeUnit.MILLISECONDS);

        corrupted = false;
    }

    /** Get all the paths of event files available in workingDir. */
    private List<Path> getAllJobEventPaths() throws IOException {
        List<Path> paths = new ArrayList<>();
        int index = INITIAL_INDEX;
        Path path = new Path(workingDir, FILE_PREFIX + index++);
        while (fileSystem.exists(path)) {
            paths.add(path);
            path = new Path(workingDir, FILE_PREFIX + index++);
        }
        return paths;
    }

    protected ScheduledExecutorService createJobEventWriterExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                new ExecutorThreadFactory("job-event-writer"));
    }

    @Override
    public void stop(boolean clear) {
        try {
            // close output stream
            eventWriterExecutor.execute(this::closeOutputStream);

            // close input stream
            closeInputStream();

            // close writer executor.
            if (eventWriterExecutor != null) {
                eventWriterExecutor.shutdown();
                try {
                    if (!eventWriterExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                        eventWriterExecutor.shutdownNow();
                    }
                } catch (InterruptedException ignored) {
                    eventWriterExecutor.shutdownNow();
                }
                eventWriterExecutor = null;
            }

            if (clear) {
                fileSystem.delete(workingDir, true);
            }
        } catch (Exception exception) {
            LOG.warn("Fail to stop filesystem job event store.", exception);
        }
    }

    void writeInt(FSDataOutputStream out, int num) throws IOException {
        out.write((num >>> 24) & 0xFF);
        out.write((num >>> 16) & 0xFF);
        out.write((num >>> 8) & 0xFF);
        out.write((num) & 0xFF);
    }

    @Override
    public void writeEvent(JobEvent event, boolean cutBlock) {
        checkNotNull(fileSystem);
        checkNotNull(event);

        eventWriterExecutor.execute(() -> writeEventRunnable(event, cutBlock));
    }

    @VisibleForTesting
    protected void writeEventRunnable(JobEvent event, boolean cutBlock) {
        if (corrupted) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Skip job event {} because write corrupted.", event);
            }
            return;
        }

        try {
            if (outputStream == null) {
                openNewOutputStream();
            }

            // write event.
            SimpleVersionedSerializer<JobEvent> serializer =
                    checkNotNull(
                            jobEventSerializers.get(event.getType()),
                            "There is no registered serializer for job event with type "
                                    + event.getType());
            byte[] binaryEvent = serializer.serialize(event);
            writeInt(outputStream, event.getType());
            writeInt(outputStream, binaryEvent.length);
            outputStream.write(binaryEvent);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Write job event {}.", event);
            }

            if (cutBlock) {
                closeOutputStream();
            }
        } catch (Throwable throwable) {
            LOG.warn(
                    "Write event {} into {} meet error, will not record event any more.",
                    event,
                    writeFile,
                    throwable);
            corrupted = true;
            closeOutputStream();
        }
    }

    @VisibleForTesting
    void writeEvent(JobEvent event) {
        writeEvent(event, false);
    }

    @Override
    public JobEvent readEvent() throws Exception {
        try {
            JobEvent event = null;
            while (event == null) {
                try {
                    if (inputStream == null && tryGetNewInputStream() == null) {
                        return null;
                    }
                    // read event.
                    int binaryEventType = inputStream.readInt();
                    int binaryEventSize = inputStream.readInt();
                    byte[] binaryEvent = new byte[binaryEventSize];
                    inputStream.readFully(binaryEvent);
                    SimpleVersionedSerializer<JobEvent> serializer =
                            checkNotNull(
                                    jobEventSerializers.get(binaryEventType),
                                    "There is no registered serializer for job event with type "
                                            + binaryEventType);
                    event = serializer.deserialize(1, binaryEvent);
                } catch (EOFException eof) {
                    closeInputStream();
                }
            }
            return event;
        } catch (Exception e) {
            throw new IOException("Cannot read next event from event store.", e);
        }
    }

    @Override
    public boolean isEmpty() throws Exception {
        return !fileSystem.exists(workingDir) || fileSystem.listStatus(workingDir).length == 0;
    }

    /** If current inputStream is null, try to get a new one. */
    private DataInputStream tryGetNewInputStream() throws IOException {
        if (inputStream == null) {
            if (readIndex < readFiles.size()) {
                Path file = readFiles.get(readIndex++);
                inputStream = new DataInputStream(fileSystem.open(file));
                LOG.info("Start reading job event file {}", file.getPath());
            }
        }
        return inputStream;
    }

    /** Try to open a new output stream. */
    private void openNewOutputStream() throws IOException {
        // get write file.
        writeFile = new Path(workingDir, FILE_PREFIX + writeIndex);
        outputStream =
                new FsBatchFlushOutputStream(
                        fileSystem, writeFile, FileSystem.WriteMode.NO_OVERWRITE, writeBufferSize);
        LOG.info("Job event will be written to {}.", writeFile);
        writeIndex++;
    }

    /** Close output stream. Should be invoked in {@link #eventWriterExecutor}. */
    @VisibleForTesting
    void closeOutputStream() {
        if (outputStream != null) {
            try {
                outputStream.close();
            } catch (IOException exception) {
                LOG.warn(
                        "Close the output stream for {} meet error, will not record event any more.",
                        writeFile,
                        exception);
                corrupted = true;
            } finally {
                outputStream = null;
            }
        }
    }

    /** Close input stream. */
    private void closeInputStream() throws IOException {
        if (inputStream != null) {
            inputStream.close();
            inputStream = null;
        }
    }
}
