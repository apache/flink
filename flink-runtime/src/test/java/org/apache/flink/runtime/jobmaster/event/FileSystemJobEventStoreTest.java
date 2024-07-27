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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link FileSystemJobEventStore}. */
class FileSystemJobEventStoreTest {

    @TempDir private java.nio.file.Path temporaryFolder;

    @Test
    void testReadAndWriteEvent() throws Exception {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        FileSystemJobEventStore store = new FileSystemJobEventStore(rootPath, new Configuration());
        store.registerJobEventSerializer(TestingJobEvent.TYPE_ID, new GenericJobEventSerializer());

        // Initial the store and test read write.
        store.start();
        JobEvent event0 = new TestingJobEvent(0);
        JobEvent event1 = new TestingJobEvent(1);
        JobEvent event2 = new TestingJobEvent(2);
        JobEvent event3 = new TestingJobEvent(3);
        JobEvent event4 = new TestingJobEvent(4);

        store.writeEvent(event0);
        store.writeEvent(event1);
        store.writeEvent(event2);
        store.writeEvent(event3);
        store.writeEvent(event4);
        store.stop(false);

        store.start();
        assertThat(store.readEvent()).isEqualTo(event0);
        assertThat(store.readEvent()).isEqualTo(event1);
        assertThat(store.readEvent()).isEqualTo(event2);
        assertThat(store.readEvent()).isEqualTo(event3);
        assertThat(store.readEvent()).isEqualTo(event4);
        assertThat(store.readEvent()).isNull();
        store.stop(false);

        // test restart a fs store can retrieve former events
        store.start();
        store.writeEvent(event0);
        store.writeEvent(event1);
        store.stop(false);

        store.start();
        assertThat(store.readEvent()).isEqualTo(event0);
        assertThat(store.readEvent()).isEqualTo(event1);
        assertThat(store.readEvent()).isEqualTo(event2);
        assertThat(store.readEvent()).isEqualTo(event3);
        assertThat(store.readEvent()).isEqualTo(event4);
        assertThat(store.readEvent()).isEqualTo(event0);
        assertThat(store.readEvent()).isEqualTo(event1);
        assertThat(store.readEvent()).isNull();

        // test clear the store
        store.stop(true);
        store.start();
        assertThat(store.readEvent()).isNull();
        store.stop(true);
    }

    @Test
    void testMultiThreadWriteEvent() throws Exception {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        FileSystemJobEventStore store = new FileSystemJobEventStore(rootPath, new Configuration());
        store.registerJobEventSerializer(TestingJobEvent.TYPE_ID, new GenericJobEventSerializer());

        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        store.start();
        JobEvent event0 = new TestingJobEvent(0);
        JobEvent event1 = new TestingJobEvent(1);
        JobEvent event2 = new TestingJobEvent(2);
        JobEvent event3 = new TestingJobEvent(3);
        JobEvent event4 = new TestingJobEvent(4);

        JobEvent event5 = new TestingJobEvent(5);
        JobEvent event6 = new TestingJobEvent(6);
        JobEvent event7 = new TestingJobEvent(7);
        JobEvent event8 = new TestingJobEvent(8);
        JobEvent event9 = new TestingJobEvent(9);

        executor.schedule(
                () -> {
                    store.writeEvent(event5);
                    store.writeEvent(event6);
                    store.writeEvent(event7);
                    store.writeEvent(event8);
                    store.writeEvent(event9);
                },
                0L,
                TimeUnit.MILLISECONDS);

        store.writeEvent(event0);
        store.writeEvent(event1);
        store.writeEvent(event2);
        store.writeEvent(event3);
        store.writeEvent(event4);

        // wait for the completion of the async task
        executor.shutdown();
        executor.awaitTermination(10000L, TimeUnit.MILLISECONDS);
        store.stop(false);

        store.start();
        List<JobEvent> eventList = new ArrayList<>();
        JobEvent event;
        while ((event = store.readEvent()) != null) {
            eventList.add(event);
        }

        assertThat(eventList).hasSize(10);
        assertThat(eventList)
                .containsExactlyInAnyOrder(
                        event0, event1, event2, event3, event4, event5, event6, event7, event8,
                        event9);

        store.stop(true);
    }

    /** Test stop with clear. The event files should be deleted. */
    @Test
    void testStopWithClear() throws IOException {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        FileSystemJobEventStore store = new FileSystemJobEventStore(rootPath, new Configuration());
        store.registerJobEventSerializer(TestingJobEvent.TYPE_ID, new GenericJobEventSerializer());

        assertThat(new File(rootPath.getPath()).listFiles().length).isZero();

        store.start();
        store.writeEvent(new TestingJobEvent(0));
        store.stop(false);
        assertThat(new File(rootPath.getPath()).listFiles().length).isEqualTo(1);

        store.start();
        store.stop(true);
        assertThat(new File(rootPath.getPath()).exists()).isFalse();
    }

    /**
     * Test when FileSystemJobEventStore read an unexpected length event, skip to the next event
     * file.
     */
    @Test
    void testReadUnexpectedLengthEvent() throws Exception {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        FileSystemJobEventStore store = new FileSystemJobEventStore(rootPath, new Configuration());
        store.registerJobEventSerializer(TestingJobEvent.TYPE_ID, new GenericJobEventSerializer());

        // write event to first event file.
        store.start();
        JobEvent event0 = new TestingJobEvent(0);
        JobEvent event1 = new TestingJobEvent(1);
        JobEvent event2 = new TestingJobEvent(2);
        JobEvent event3 = new TestingJobEvent(3);
        JobEvent event4 = new TestingJobEvent(4);
        store.writeEvent(event0);
        store.writeEvent(event1);
        store.writeEvent(event2);
        store.writeEvent(event3);
        store.writeEvent(event4);

        // write an extra integer
        store.getEventWriterExecutor()
                .submit(
                        () -> {
                            try {
                                store.getOutputStream().writeInt(TestingJobEvent.TYPE_ID);
                                store.getOutputStream().writeInt(5000);
                                store.writeEvent(new TestingJobEvent(100));
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        })
                .get();
        store.stop(false);

        // write event to second event file.
        store.start();
        JobEvent event5 = new TestingJobEvent(5);
        JobEvent event6 = new TestingJobEvent(6);
        JobEvent event7 = new TestingJobEvent(7);
        JobEvent event8 = new TestingJobEvent(8);
        JobEvent event9 = new TestingJobEvent(9);
        store.writeEvent(event5);
        store.writeEvent(event6);
        store.writeEvent(event7);
        store.writeEvent(event8);
        store.writeEvent(event9);

        store.stop(false);

        store.start();
        List<JobEvent> eventList = new ArrayList<>();
        JobEvent event;
        while ((event = store.readEvent()) != null) {
            eventList.add(event);
        }

        assertThat(eventList).hasSize(10);
        assertThat(eventList)
                .containsExactlyInAnyOrder(
                        event0, event1, event2, event3, event4, event5, event6, event7, event8,
                        event9);

        store.stop(true);
    }

    @Test
    void testGenerateWorkingDirCorrectly() throws IOException {
        final Configuration configuration = new Configuration();
        configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, "file:///tmp/flink");
        configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "cluster_id");

        final JobID jobID = new JobID();
        FileSystemJobEventStore store = new FileSystemJobEventStore(jobID, configuration);
        assertThat(store.getWorkingDir().getPath())
                .isEqualTo(String.format("/tmp/flink/cluster_id/%s/job-events", jobID));
    }

    @Test
    void testCutBlock() throws Exception {
        final Path rootPath = new Path(TempDirUtils.newFolder(temporaryFolder).getAbsolutePath());
        FileSystemJobEventStore store = new FileSystemJobEventStore(rootPath, new Configuration());
        store.registerJobEventSerializer(TestingJobEvent.TYPE_ID, new GenericJobEventSerializer());

        assertThat(new File(rootPath.getPath()).listFiles().length).isZero();

        store.start();

        // write one event.
        store.writeEvent(new TestingJobEvent(0));
        tryFlushOutputStream(store);
        assertThat(new File(rootPath.getPath()).listFiles().length).isEqualTo(1);

        // write one event and cut.
        store.writeEvent(new TestingJobEvent(1), true);
        assertThat(new File(rootPath.getPath()).listFiles().length).isEqualTo(1);

        store.writeEvent(new TestingJobEvent(2));
        tryFlushOutputStream(store);
        assertThat(new File(rootPath.getPath()).listFiles().length).isEqualTo(2);

        store.stop(true);
    }

    private void tryFlushOutputStream(FileSystemJobEventStore store) throws Exception {
        runInEventWriterExecutor(
                store,
                () -> {
                    try {
                        if (store.getOutputStream() != null) {
                            store.getOutputStream().flush();
                        }
                    } catch (IOException e) {
                    }
                });
    }

    private void runInEventWriterExecutor(FileSystemJobEventStore store, Runnable runnable)
            throws Exception {
        store.getEventWriterExecutor().submit(runnable).get();
    }
}
