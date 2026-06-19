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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Implementation of {@link JobEventStore} that stores all {@link JobEvent} instances in a {@link
 * FileSystem}. Events are written and read sequentially, ensuring a consistent order of events.
 *
 * <p>Write operations to the file system are primarily asynchronous, leveraging a {@link
 * ScheduledExecutorService} to periodically flush the buffered output stream, which executes write
 * tasks at an interval determined by the configure option {@link
 * JobEventStoreOptions#FLUSH_INTERVAL}.
 *
 * <p>Read operations are performed synchronously, with the calling thread directly interacting with
 * the file system to fetch event data.
 */
public class FileSystemJobEventStore implements JobEventStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemJobEventStore.class);

    private static final String FILE_PREFIX = "events.";

    private static final int INITIAL_FILE_INDEX = 0;

    private final FileSystem fileSystem;
    private final Path workingDir;

    private FsBatchFlushOutputStream outputStream;
    private Path writeFile;
    private int writeIndex;
    private ScheduledExecutorService eventWriterExecutor;

    private DataInputStream inputStream;
    private int readIndex;
    private List<Path> readFiles;

    /**
     * Flag indicating whether the event store has become corrupted. When this flag is true, the
     * store is considered to no longer be in a healthy state for enable writing new events.
     */
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

    void registerJobEventSerializer(
            int eventType, SimpleVersionedSerializer<JobEvent> simpleVersionedSerializer) {
        checkState(!jobEventSerializers.containsKey(eventType));
        jobEventSerializers.put(eventType, simpleVersionedSerializer);
    }

    private void registerJobEventSerializers() {
        registerJobEventSerializer(
                JobEvents.getTypeID(ExecutionVertexFinishedEvent.class),
                new ExecutionVertexFinishedEvent.Serializer());
        registerJobEventSerializer(
                JobEvents.getTypeID(ExecutionVertexResetEvent.class),
                new GenericJobEventSerializer());
        registerJobEventSerializer(
                JobEvents.getTypeID(ExecutionJobVertexInitializedEvent.class),
                new GenericJobEventSerializer());
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
    FsBatchFlushOutputStream getOutputStream() {
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
            readFiles = getAllJobEventFiles();
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
                                    "Error happens when flushing event file {}. Do not record events any more.",
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

    /** Get all event files available in workingDir. */
    private List<Path> getAllJobEventFiles() throws IOException {
        List<Path> paths = new ArrayList<>();
        int index = INITIAL_FILE_INDEX;

        Set<String> fileNames =
                Arrays.stream(fileSystem.listStatus(workingDir))
                        .map(fileStatus -> fileStatus.getPath().getName())
                        .filter(name -> name.startsWith(FILE_PREFIX))
                        .collect(Collectors.toSet());

        while (fileNames.contains(FILE_PREFIX + index)) {
            paths.add(new Path(workingDir, FILE_PREFIX + index));
            index++;
        }
        return paths;
    }

    protected ScheduledExecutorService createJobEventWriterExecutor() {
        return Executors.newSingleThreadScheduledExecutor(
                new ExecutorThreadFactory("job-event-writer"));
    }

    @Override
    public void stop(boolean clearEventLogs) {
        try {
            eventWriterExecutor.execute(this::closeOutputStream);

            closeInputStream();

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

            if (clearEventLogs) {
                fileSystem.delete(workingDir, true);
            }
        } catch (Exception exception) {
            LOG.warn("Fail to stop filesystem job event store.", exception);
        }
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
            outputStream.writeInt(event.getType());
            outputStream.writeInt(binaryEvent.length);
            outputStream.write(binaryEvent);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Write job event {}.", event);
            }

            if (cutBlock) {
                closeOutputStream();
            }
        } catch (Throwable throwable) {
            LOG.warn(
                    "Error happens when writing event {} into {}. Do not record events any more.",
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
                    event =
                            serializer.deserialize(
                                    GenericJobEventSerializer.INSTANCE.getVersion(), binaryEvent);
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
        LOG.info("Job events will be written to {}.", writeFile);
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
                        "Error happens when closing the output stream for {}. Do not record events any more.",
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
