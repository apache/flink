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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.AssumptionViolatedException;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test checkpointing while sourcing a continuous file processor. */
public class ContinuousFileProcessingCheckpointITCase extends StreamFaultToleranceTestBase {

    private static final int NO_OF_FILES = 5;
    private static final int LINES_PER_FILE = 150;
    private static final long INTERVAL = 100;

    private static File baseDir;
    private static org.apache.hadoop.fs.FileSystem localFs;
    private static String localFsURI;
    private FileCreator fc;

    private static Map<Integer, Set<String>> actualCollectedContent = new HashMap<>();

    @Before
    public void createHDFS() throws IOException {
        if (failoverStrategy.equals(FailoverStrategy.RestartPipelinedRegionFailoverStrategy)) {
            // TODO the 'NO_OF_RETRIES' is useless for current RestartPipelinedRegionStrategy,
            // for this ContinuousFileProcessingCheckpointITCase, using
            // RestartPipelinedRegionStrategy would result in endless running.
            throw new AssumptionViolatedException(
                    "ignored ContinuousFileProcessingCheckpointITCase when using RestartPipelinedRegionStrategy");
        }

        baseDir = new File("./target/localfs/fs_tests").getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);

        org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();

        localFsURI = "file:///" + baseDir + "/";
        localFs = new org.apache.hadoop.fs.Path(localFsURI).getFileSystem(hdConf);
    }

    @After
    public void destroyHDFS() {
        if (baseDir != null) {
            FileUtil.fullyDelete(baseDir);
        }
    }

    @Override
    public void testProgram(StreamExecutionEnvironment env) {

        env.enableCheckpointing(10);

        // create and start the file creating thread.
        fc = new FileCreator();
        fc.start();

        // create the monitoring source along with the necessary readers.
        TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(localFsURI));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        DataStream<String> inputStream =
                env.readFile(format, localFsURI, FileProcessingMode.PROCESS_CONTINUOUSLY, INTERVAL);

        TestingSinkFunction sink = new TestingSinkFunction();

        inputStream
                .flatMap(
                        new FlatMapFunction<String, String>() {
                            @Override
                            public void flatMap(String value, Collector<String> out)
                                    throws Exception {
                                out.collect(value);
                            }
                        })
                .addSink(sink)
                .setParallelism(1);
    }

    @Override
    public void postSubmit() throws Exception {

        // be sure that the file creating thread is done.
        fc.join();

        Map<Integer, Set<String>> collected = actualCollectedContent;
        Assert.assertEquals(collected.size(), fc.getFileContent().size());

        for (Integer fileIdx : fc.getFileContent().keySet()) {
            Assert.assertTrue(collected.keySet().contains(fileIdx));

            List<String> cntnt = new ArrayList<>(collected.get(fileIdx));
            Collections.sort(
                    cntnt,
                    new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            return getLineNo(o1) - getLineNo(o2);
                        }
                    });

            StringBuilder cntntStr = new StringBuilder();
            for (String line : cntnt) {
                cntntStr.append(line);
            }
            Assert.assertEquals(fc.getFileContent().get(fileIdx), cntntStr.toString());
        }

        collected.clear();
        actualCollectedContent.clear();
        fc.clean();
    }

    private int getLineNo(String line) {
        String[] tkns = line.split("\\s");
        return Integer.parseInt(tkns[tkns.length - 1]);
    }

    // --------------------------			Task Sink			------------------------------

    private static class TestingSinkFunction extends RichSinkFunction<String>
            implements ListCheckpointed<Tuple2<Long, Map<Integer, Set<String>>>>,
                    CheckpointListener {

        private boolean hasRestoredAfterFailure;

        private volatile int successfulCheckpoints;

        private long elementsToFailure;

        private long elementCounter;

        private Map<Integer, Set<String>> actualContent = new HashMap<>();

        TestingSinkFunction() {
            hasRestoredAfterFailure = false;
            elementCounter = 0;
            successfulCheckpoints = 0;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // this sink can only work with DOP 1
            assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());

            long failurePosMin = (long) (0.4 * LINES_PER_FILE);
            long failurePosMax = (long) (0.7 * LINES_PER_FILE);

            elementsToFailure =
                    (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
        }

        @Override
        public void invoke(String value) throws Exception {
            int fileIdx = getFileIdx(value);

            Set<String> content = actualContent.get(fileIdx);
            if (content == null) {
                content = new HashSet<>();
                actualContent.put(fileIdx, content);
            }

            // detect duplicate lines.
            if (!content.add(value + "\n")) {
                fail("Duplicate line: " + value);
                System.exit(0);
            }

            elementCounter++;

            // this is termination
            if (elementCounter >= NO_OF_FILES * LINES_PER_FILE) {
                actualCollectedContent = actualContent;
                throw new SuppressRestartsException(new SuccessException());
            }

            // add some latency so that we have at least two checkpoint in
            if (!hasRestoredAfterFailure && successfulCheckpoints < 2) {
                Thread.sleep(5);
            }

            // simulate a node failure
            if (!hasRestoredAfterFailure
                    && successfulCheckpoints >= 2
                    && elementCounter >= elementsToFailure) {
                throw new Exception(
                        "Task Failure @ elem: " + elementCounter + " / " + elementsToFailure);
            }
        }

        @Override
        public List<Tuple2<Long, Map<Integer, Set<String>>>> snapshotState(
                long checkpointId, long checkpointTimestamp) throws Exception {
            Tuple2<Long, Map<Integer, Set<String>>> state =
                    new Tuple2<>(elementCounter, actualContent);
            return Collections.singletonList(state);
        }

        @Override
        public void restoreState(List<Tuple2<Long, Map<Integer, Set<String>>>> state)
                throws Exception {
            Tuple2<Long, Map<Integer, Set<String>>> s = state.get(0);
            this.elementCounter = s.f0;
            this.actualContent = s.f1;
            this.hasRestoredAfterFailure =
                    this.elementCounter
                            != 0; // because now restore is also called at initialization
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            this.successfulCheckpoints++;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        private int getFileIdx(String line) {
            String[] tkns = line.split(":");
            return Integer.parseInt(tkns[0]);
        }
    }

    // -------------------------			FILE CREATION			-------------------------------

    /**
     * A separate thread creating {@link #NO_OF_FILES} files, one file every {@link #INTERVAL}
     * milliseconds. It serves for testing the file monitoring functionality of the {@link
     * ContinuousFileMonitoringFunction}. The files are filled with data by the {@link
     * #fillWithData(String, String, int, String)} method.
     */
    private class FileCreator extends Thread {

        private final Set<Path> filesCreated = new HashSet<>();
        private final Map<Integer, String> fileContents = new HashMap<>();

        /** The modification time of the last created file. */
        private long lastCreatedModTime = Long.MIN_VALUE;

        public void run() {
            try {
                for (int i = 0; i < NO_OF_FILES; i++) {
                    Tuple2<org.apache.hadoop.fs.Path, String> tmpFile;
                    long modTime;
                    do {

                        // give it some time so that the files have
                        // different modification timestamps.
                        Thread.sleep(50);

                        tmpFile = fillWithData(localFsURI, "file", i, "This is test line.");

                        modTime = localFs.getFileStatus(tmpFile.f0).getModificationTime();
                        if (modTime <= lastCreatedModTime) {
                            // delete the last created file to recreate it with a different
                            // timestamp
                            localFs.delete(tmpFile.f0, false);
                        }
                    } while (modTime <= lastCreatedModTime);
                    lastCreatedModTime = modTime;

                    // rename the file
                    org.apache.hadoop.fs.Path file =
                            new org.apache.hadoop.fs.Path(localFsURI + "/file" + i);
                    localFs.rename(tmpFile.f0, file);
                    Assert.assertTrue(localFs.exists(file));

                    filesCreated.add(file);
                    fileContents.put(i, tmpFile.f1);
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        void clean() throws IOException {
            assert (localFs != null);
            for (org.apache.hadoop.fs.Path path : filesCreated) {
                localFs.delete(path, false);
            }
            fileContents.clear();
        }

        Map<Integer, String> getFileContent() {
            return this.fileContents;
        }
    }

    /** Fill the file with content and put the content in the {@code hdPathContents} list. */
    private Tuple2<Path, String> fillWithData(
            String base, String fileName, int fileIdx, String sampleLine)
            throws IOException, InterruptedException {

        assert (localFs != null);

        org.apache.hadoop.fs.Path tmp =
                new org.apache.hadoop.fs.Path(base + "/." + fileName + fileIdx);

        FSDataOutputStream stream = localFs.create(tmp);
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < LINES_PER_FILE; i++) {
            String line = fileIdx + ": " + sampleLine + " " + i + "\n";
            str.append(line);
            stream.write(line.getBytes(ConfigConstants.DEFAULT_CHARSET));
        }
        stream.close();
        return new Tuple2<>(tmp, str.toString());
    }
}
