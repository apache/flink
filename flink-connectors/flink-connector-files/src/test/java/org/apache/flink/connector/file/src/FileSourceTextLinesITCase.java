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

package org.apache.flink.connector.file.src;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.nonha.embedded.HaLeadershipControl;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.ClientAndIterator;
import org.apache.flink.test.junit5.InjectMiniCluster;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

/** MiniCluster-based integration test for the {@link FileSource}. */
class FileSourceTextLinesITCase {

    private static final int PARALLELISM = 4;

    @TempDir private static java.nio.file.Path tmpDir;

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER_RESOURCE =
            new MiniClusterExtension(createMiniClusterConfiguration());

    // ------------------------------------------------------------------------
    //  test cases
    // ------------------------------------------------------------------------

    /** This test runs a job reading bounded input with a stream record format (text lines). */
    @Test
    void testBoundedTextFileSource(
            @TempDir java.nio.file.Path tmpTestDir, @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        testBoundedTextFileSource(tmpTestDir, FailoverType.NONE, miniCluster);
    }

    /**
     * This test runs a job reading bounded input with a stream record format (text lines) and
     * restarts TaskManager.
     */
    @Test
    void testBoundedTextFileSourceWithTaskManagerFailover(@TempDir java.nio.file.Path tmpTestDir)
            throws Exception {
        // This test will kill TM, so we run it in a new cluster to avoid affecting other tests
        runTestWithNewMiniCluster(
                miniCluster -> testBoundedTextFileSource(tmpTestDir, FailoverType.TM, miniCluster));
    }

    /**
     * This test runs a job reading bounded input with a stream record format (text lines) and
     * triggers JobManager failover.
     */
    @Test
    void testBoundedTextFileSourceWithJobManagerFailover(@TempDir java.nio.file.Path tmpTestDir)
            throws Exception {
        // This test will kill JM, so we run it in a new cluster to avoid affecting other tests
        runTestWithNewMiniCluster(
                miniCluster -> testBoundedTextFileSource(tmpTestDir, FailoverType.JM, miniCluster));
    }

    private void testBoundedTextFileSource(
            java.nio.file.Path tmpTestDir, FailoverType failoverType, MiniCluster miniCluster)
            throws Exception {
        final File testDir = tmpTestDir.toFile();

        // our main test data
        writeAllFiles(testDir);

        // write some junk to hidden files test that common hidden file patterns are filtered by
        // default
        writeHiddenJunkFiles(testDir);

        final FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(), Path.fromLocalFile(testDir))
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        final DataStream<String> streamFailingInTheMiddleOfReading =
                RecordCounterToFail.wrapWithFailureAfter(stream, LINES.length / 2);

        final ClientAndIterator<String> client =
                DataStreamUtils.collectWithClient(
                        streamFailingInTheMiddleOfReading, "Bounded TextFiles Test");
        final JobID jobId = client.client.getJobID();

        RecordCounterToFail.waitToFail();
        triggerFailover(failoverType, jobId, RecordCounterToFail::continueProcessing, miniCluster);

        final List<String> result = new ArrayList<>();
        while (client.iterator.hasNext()) {
            result.add(client.iterator.next());
        }

        verifyResult(result);
    }

    /**
     * This test runs a job reading continuous input (files appearing over time) with a stream
     * record format (text lines).
     */
    @Test
    void testContinuousTextFileSource(
            @TempDir java.nio.file.Path tmpTestDir, @InjectMiniCluster MiniCluster miniCluster)
            throws Exception {
        testContinuousTextFileSource(tmpTestDir, FailoverType.NONE, miniCluster);
    }

    /**
     * This test runs a job reading continuous input (files appearing over time) with a stream
     * record format (text lines) and restarts TaskManager.
     */
    @Test
    @Tag("org.apache.flink.testutils.junit.FailsWithAdaptiveScheduler") // FLINK-21450
    void testContinuousTextFileSourceWithTaskManagerFailover(@TempDir java.nio.file.Path tmpTestDir)
            throws Exception {
        // This test will kill TM, so we run it in a new cluster to avoid affecting other tests
        runTestWithNewMiniCluster(
                miniCluster ->
                        testContinuousTextFileSource(tmpTestDir, FailoverType.TM, miniCluster));
    }

    /**
     * This test runs a job reading continuous input (files appearing over time) with a stream
     * record format (text lines) and triggers JobManager failover.
     */
    @Test
    void testContinuousTextFileSourceWithJobManagerFailover(@TempDir java.nio.file.Path tmpTestDir)
            throws Exception {
        // This test will kill JM, so we run it in a new cluster to avoid affecting other tests
        runTestWithNewMiniCluster(
                miniCluster ->
                        testContinuousTextFileSource(tmpTestDir, FailoverType.JM, miniCluster));
    }

    private void testContinuousTextFileSource(
            java.nio.file.Path tmpTestDir, FailoverType type, MiniCluster miniCluster)
            throws Exception {
        final File testDir = tmpTestDir.toFile();

        final FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(), Path.fromLocalFile(testDir))
                        .monitorContinuously(Duration.ofMillis(5))
                        .build();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(10L);

        final DataStream<String> stream =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "file-source");

        final ClientAndIterator<String> client =
                DataStreamUtils.collectWithClient(stream, "Continuous TextFiles Monitoring Test");
        final JobID jobId = client.client.getJobID();

        // write one file, execute, and wait for its result
        // that way we know that the application was running and the source has
        // done its first chunk of work already

        final int numLinesFirst = LINES_PER_FILE[0].length;
        final int numLinesAfter = LINES.length - numLinesFirst;

        writeFile(testDir, 0);
        final List<String> result1 =
                DataStreamUtils.collectRecordsFromUnboundedStream(client, numLinesFirst);

        // write the remaining files over time, after that collect the final result
        for (int i = 1; i < LINES_PER_FILE.length; i++) {
            Thread.sleep(10);
            writeFile(testDir, i);
            final boolean failAfterHalfOfInput = i == LINES_PER_FILE.length / 2;
            if (failAfterHalfOfInput) {
                triggerFailover(type, jobId, () -> {}, miniCluster);
            }
        }

        final List<String> result2 =
                DataStreamUtils.collectRecordsFromUnboundedStream(client, numLinesAfter);

        // shut down the job, now that we have all the results we expected.
        client.client.cancel().get();

        result1.addAll(result2);
        verifyResult(result1);
    }

    // ------------------------------------------------------------------------
    //  test utilities
    // ------------------------------------------------------------------------

    private enum FailoverType {
        NONE,
        TM,
        JM
    }

    private static MiniClusterResourceConfiguration createMiniClusterConfiguration() {
        return new MiniClusterResourceConfiguration.Builder()
                .setNumberTaskManagers(1)
                .setNumberSlotsPerTaskManager(PARALLELISM)
                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                .withHaLeadershipControl()
                .build();
    }

    private static void runTestWithNewMiniCluster(
            ThrowingConsumer<MiniCluster, Exception> testMethod) throws Exception {
        MiniClusterWithClientResource miniCluster = null;
        try {
            miniCluster = new MiniClusterWithClientResource(createMiniClusterConfiguration());
            miniCluster.before();
            testMethod.accept(miniCluster.getMiniCluster());
        } finally {
            if (miniCluster != null) {
                miniCluster.after();
            }
        }
    }

    private static void triggerFailover(
            FailoverType type, JobID jobId, Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        switch (type) {
            case NONE:
                afterFailAction.run();
                break;
            case TM:
                restartTaskManager(afterFailAction, miniCluster);
                break;
            case JM:
                triggerJobManagerFailover(jobId, afterFailAction, miniCluster);
                break;
        }
    }

    private static void triggerJobManagerFailover(
            JobID jobId, Runnable afterFailAction, MiniCluster miniCluster) throws Exception {
        final HaLeadershipControl haLeadershipControl = miniCluster.getHaLeadershipControl().get();
        haLeadershipControl.revokeJobMasterLeadership(jobId).get();
        afterFailAction.run();
        haLeadershipControl.grantJobMasterLeadership(jobId).get();
    }

    private static void restartTaskManager(Runnable afterFailAction, MiniCluster miniCluster)
            throws Exception {
        miniCluster.terminateTaskManager(0).get();
        afterFailAction.run();
        miniCluster.startTaskManager();
    }

    // ------------------------------------------------------------------------
    //  verification
    // ------------------------------------------------------------------------

    private static void verifyResult(List<String> lines) {
        final String[] expected = Arrays.copyOf(LINES, LINES.length);
        final String[] actual = lines.toArray(new String[0]);

        Arrays.sort(expected);
        Arrays.sort(actual);

        assertThat(actual).isEqualTo(expected);
    }

    // ------------------------------------------------------------------------
    //  test data
    // ------------------------------------------------------------------------

    private static final String[] FILE_PATHS =
            new String[] {
                "text.2",
                "nested1/text.1",
                "text.1",
                "text.3",
                "nested2/nested21/text",
                "nested1/text.2",
                "nested2/text"
            };

    private static final String[] HIDDEN_JUNK_PATHS =
            new String[] {
                // all file names here start with '.' or '_'
                "_something",
                ".junk",
                "nested1/.somefile",
                "othernested/_ignoredfile",
                "_nested/file",
                "nested1/.intermediate/somefile"
            };

    private static final String[] LINES =
            new String[] {
                "To be, or not to be,--that is the question:--",
                "Whether 'tis nobler in the mind to suffer",
                "The slings and arrows of outrageous fortune",
                "Or to take arms against a sea of troubles,",
                "And by opposing end them?--To die,--to sleep,--",
                "No more; and by a sleep to say we end",
                "The heartache, and the thousand natural shocks",
                "That flesh is heir to,--'tis a consummation",
                "Devoutly to be wish'd. To die,--to sleep;--",
                "To sleep! perchance to dream:--ay, there's the rub;",
                "For in that sleep of death what dreams may come,",
                "When we have shuffled off this mortal coil,",
                "Must give us pause: there's the respect",
                "That makes calamity of so long life;",
                "For who would bear the whips and scorns of time,",
                "The oppressor's wrong, the proud man's contumely,",
                "The pangs of despis'd love, the law's delay,",
                "The insolence of office, and the spurns",
                "That patient merit of the unworthy takes,",
                "When he himself might his quietus make",
                "With a bare bodkin? who would these fardels bear,",
                "To grunt and sweat under a weary life,",
                "But that the dread of something after death,--",
                "The undiscover'd country, from whose bourn",
                "No traveller returns,--puzzles the will,",
                "And makes us rather bear those ills we have",
                "Than fly to others that we know not of?",
                "Thus conscience does make cowards of us all;",
                "And thus the native hue of resolution",
                "Is sicklied o'er with the pale cast of thought;",
                "And enterprises of great pith and moment,",
                "With this regard, their currents turn awry,",
                "And lose the name of action.--Soft you now!",
                "The fair Ophelia!--Nymph, in thy orisons",
                "Be all my sins remember'd."
            };

    private static final String[][] LINES_PER_FILE = splitLinesForFiles();

    private static String[][] splitLinesForFiles() {
        final String[][] result = new String[FILE_PATHS.length][];

        final int linesPerFile = LINES.length / FILE_PATHS.length;
        final int linesForLastFile = LINES.length - ((FILE_PATHS.length - 1) * linesPerFile);

        int pos = 0;
        for (int i = 0; i < FILE_PATHS.length - 1; i++) {
            String[] lines = new String[linesPerFile];
            result[i] = lines;
            for (int k = 0; k < lines.length; k++) {
                lines[k] = LINES[pos++];
            }
        }
        String[] lines = new String[linesForLastFile];
        result[result.length - 1] = lines;
        for (int k = 0; k < lines.length; k++) {
            lines[k] = LINES[pos++];
        }
        return result;
    }

    private static void writeFile(File testDir, int num) throws IOException {
        final File file = new File(testDir, FILE_PATHS[num]);
        writeFileAtomically(file, LINES_PER_FILE[num]);
    }

    private static void writeCompressedFile(File testDir, int num) throws IOException {
        final File file = new File(testDir, FILE_PATHS[num] + ".gz");
        writeFileAtomically(file, LINES_PER_FILE[num], GZIPOutputStream::new);
    }

    private static void writeAllFiles(File testDir) throws IOException {
        for (int i = 0; i < FILE_PATHS.length; i++) {
            // we write half of the files regularly, half compressed
            if (i % 2 == 0) {
                writeFile(testDir, i);
            } else {
                writeCompressedFile(testDir, i);
            }
        }
    }

    private static void writeHiddenJunkFiles(File testDir) throws IOException {
        final String[] junkContents =
                new String[] {"This should not end up in the test result.", "Foo bar bazzl junk"};

        for (String junkPath : HIDDEN_JUNK_PATHS) {
            final File file = new File(testDir, junkPath);
            writeFileAtomically(file, junkContents);
        }
    }

    private static void writeFileAtomically(final File file, final String[] lines)
            throws IOException {
        writeFileAtomically(file, lines, (v) -> v);
    }

    private static void writeFileAtomically(
            final File file,
            final String[] lines,
            final FunctionWithException<OutputStream, OutputStream, IOException>
                    streamEncoderFactory)
            throws IOException {

        // we don't use TMP_FOLDER.newFile() here because we don't want this to actually create a
        // file,
        // but just construct the file path
        final File stagingFile =
                new File(tmpDir.getParent().toFile(), ".tmp-" + UUID.randomUUID().toString());

        try (final FileOutputStream fileOut = new FileOutputStream(stagingFile);
                final OutputStream out = streamEncoderFactory.apply(fileOut);
                final OutputStreamWriter encoder =
                        new OutputStreamWriter(out, StandardCharsets.UTF_8);
                final PrintWriter writer = new PrintWriter(encoder)) {

            for (String line : lines) {
                writer.println(line);
            }
        }

        final File parent = file.getParentFile();
        assertThat(parent.mkdirs() || parent.exists()).isTrue();

        assertThat(stagingFile.renameTo(file)).isTrue();
    }

    // ------------------------------------------------------------------------
    //  mini cluster failover utilities
    // ------------------------------------------------------------------------

    private static class RecordCounterToFail {

        private static AtomicInteger records;
        private static CompletableFuture<Void> fail;
        private static CompletableFuture<Void> continueProcessing;

        private static <T> DataStream<T> wrapWithFailureAfter(DataStream<T> stream, int failAfter) {

            records = new AtomicInteger();
            fail = new CompletableFuture<>();
            continueProcessing = new CompletableFuture<>();
            return stream.map(
                    record -> {
                        final boolean halfOfInputIsRead = records.incrementAndGet() > failAfter;
                        final boolean notFailedYet = !fail.isDone();
                        if (notFailedYet && halfOfInputIsRead) {
                            fail.complete(null);
                            continueProcessing.get();
                        }
                        return record;
                    });
        }

        private static void waitToFail() throws ExecutionException, InterruptedException {
            fail.get();
        }

        private static void continueProcessing() {
            continueProcessing.complete(null);
        }
    }
}
