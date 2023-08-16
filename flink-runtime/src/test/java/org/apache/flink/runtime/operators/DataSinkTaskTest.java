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

package org.apache.flink.runtime.operators;

import org.apache.flink.api.common.io.FileOutputFormat;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.consumer.IteratorWrappingTestSingleInputGate;
import org.apache.flink.runtime.operators.testutils.InfiniteInputIterator;
import org.apache.flink.runtime.operators.testutils.TaskCancelThread;
import org.apache.flink.runtime.operators.testutils.TaskTestBase;
import org.apache.flink.runtime.operators.testutils.UniformRecordGenerator;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.runtime.testutils.recordutils.RecordComparatorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

class DataSinkTaskTest extends TaskTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(DataSinkTaskTest.class);

    private static final int MEMORY_MANAGER_SIZE = 3 * 1024 * 1024;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    @Test
    void testDataSinkTask() {
        FileReader fr = null;
        BufferedReader br = null;
        try {
            int keyCnt = 100;
            int valCnt = 20;

            super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
            super.addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);

            DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);

            File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
            super.registerFileOutputTask(
                    MockOutputFormat.class, tempTestFile.toURI().toString(), new Configuration());

            testTask.invoke();

            assertThat(tempTestFile).withFailMessage("Temp output file does not exist").exists();

            fr = new FileReader(tempTestFile);
            br = new BufferedReader(fr);

            HashMap<Integer, HashSet<Integer>> keyValueCountMap = new HashMap<>(keyCnt);

            while (br.ready()) {
                String line = br.readLine();

                Integer key = Integer.parseInt(line.substring(0, line.indexOf("_")));
                Integer val =
                        Integer.parseInt(line.substring(line.indexOf("_") + 1, line.length()));

                if (!keyValueCountMap.containsKey(key)) {
                    keyValueCountMap.put(key, new HashSet<Integer>());
                }
                keyValueCountMap.get(key).add(val);
            }

            assertThat(keyValueCountMap)
                    .withFailMessage(
                            "Invalid key count in out file. Expected: %d Actual: %d",
                            keyCnt, keyValueCountMap.size())
                    .hasSize(keyCnt);

            for (Integer key : keyValueCountMap.keySet()) {
                assertThat(keyValueCountMap.get(key))
                        .withFailMessage(
                                "Invalid value count for key: %d. Expected: %d Actual: %d",
                                key, valCnt, keyValueCountMap.get(key).size())
                        .hasSize(valCnt);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Throwable t) {
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (Throwable t) {
                }
            }
        }
    }

    @Test
    void testUnionDataSinkTask() {
        int keyCnt = 10;
        int valCnt = 20;

        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

        final IteratorWrappingTestSingleInputGate<?>[] readers =
                new IteratorWrappingTestSingleInputGate[4];
        readers[0] =
                super.addInput(new UniformRecordGenerator(keyCnt, valCnt, 0, 0, false), 0, false);
        readers[1] =
                super.addInput(
                        new UniformRecordGenerator(keyCnt, valCnt, keyCnt, 0, false), 0, false);
        readers[2] =
                super.addInput(
                        new UniformRecordGenerator(keyCnt, valCnt, keyCnt * 2, 0, false), 0, false);
        readers[3] =
                super.addInput(
                        new UniformRecordGenerator(keyCnt, valCnt, keyCnt * 3, 0, false), 0, false);

        DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
        super.registerFileOutputTask(
                MockOutputFormat.class, tempTestFile.toURI().toString(), new Configuration());

        try {
            // For the union reader to work, we need to start notifications *after* the union reader
            // has been initialized. This is accomplished via a mockito hack in TestSingleInputGate,
            // which checks forwards existing notifications on registerListener calls.
            for (IteratorWrappingTestSingleInputGate<?> reader : readers) {
                reader.notifyNonEmpty();
            }

            testTask.invoke();
        } catch (Exception e) {
            LOG.debug("Exception while invoking the test task.", e);
            fail("Invoke method caused exception.");
        }

        assertThat(tempTestFile).withFailMessage("Temp output file does not exist").exists();

        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(tempTestFile);
            br = new BufferedReader(fr);

            HashMap<Integer, HashSet<Integer>> keyValueCountMap = new HashMap<>(keyCnt);

            while (br.ready()) {
                String line = br.readLine();

                Integer key = Integer.parseInt(line.substring(0, line.indexOf("_")));
                Integer val =
                        Integer.parseInt(line.substring(line.indexOf("_") + 1, line.length()));

                if (!keyValueCountMap.containsKey(key)) {
                    keyValueCountMap.put(key, new HashSet<Integer>());
                }
                keyValueCountMap.get(key).add(val);
            }

            assertThat(keyValueCountMap)
                    .withFailMessage(
                            "Invalid key count in out file. Expected: %d Actual: %d",
                            keyCnt * 4, keyValueCountMap.size())
                    .hasSize(keyCnt * 4);

            for (Integer key : keyValueCountMap.keySet()) {
                assertThat(keyValueCountMap.get(key))
                        .withFailMessage(
                                "Invalid value count for key: %d. Expected: %d Actual: %d",
                                key, valCnt, keyValueCountMap.get(key).size())
                        .hasSize(valCnt);
            }

        } catch (FileNotFoundException e) {
            fail("Out file got lost...");
        } catch (IOException ioe) {
            fail("Caught IOE while reading out file");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Throwable t) {
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (Throwable t) {
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSortingDataSinkTask() {

        int keyCnt = 100;
        int valCnt = 20;
        double memoryFraction = 1.0;

        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);

        super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);

        DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);

        // set sorting
        super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
        super.getTaskConfig()
                .setInputComparator(
                        new RecordComparatorFactory(new int[] {1}, (new Class[] {IntValue.class})),
                        0);
        super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
        super.getTaskConfig().setFilehandlesInput(0, 8);
        super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
        super.registerFileOutputTask(
                MockOutputFormat.class, tempTestFile.toURI().toString(), new Configuration());

        try {
            testTask.invoke();
        } catch (Exception e) {
            LOG.debug("Exception while invoking the test task.", e);
            fail("Invoke method caused exception.");
        }

        assertThat(tempTestFile).withFailMessage("Temp output file does not exist").exists();

        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(tempTestFile);
            br = new BufferedReader(fr);

            Set<Integer> keys = new HashSet<>();

            int curVal = -1;
            while (br.ready()) {
                String line = br.readLine();

                Integer key = Integer.parseInt(line.substring(0, line.indexOf("_")));
                Integer val =
                        Integer.parseInt(line.substring(line.indexOf("_") + 1, line.length()));

                // check that values are in correct order
                assertThat(val)
                        .withFailMessage("Values not in ascending order")
                        .isGreaterThanOrEqualTo(curVal);
                // next value hit
                if (val > curVal) {
                    if (curVal != -1) {
                        // check that we saw 100 distinct keys for this values
                        assertThat(keys).withFailMessage("Keys missing for value").hasSize(100);
                    }
                    // empty keys set
                    keys.clear();
                    // update current value
                    curVal = val;
                }

                assertThat(keys.add(key)).withFailMessage("Duplicate key for value").isTrue();
            }

        } catch (FileNotFoundException e) {
            fail("Out file got lost...");
        } catch (IOException ioe) {
            fail("Caught IOE while reading out file");
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Throwable t) {
                }
            }
            if (fr != null) {
                try {
                    fr.close();
                } catch (Throwable t) {
                }
            }
        }
    }

    @Test
    void testFailingDataSinkTask() {

        int keyCnt = 100;
        int valCnt = 20;

        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        super.addInput(new UniformRecordGenerator(keyCnt, valCnt, false), 0);

        DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);
        Configuration stubParams = new Configuration();

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
        super.registerFileOutputTask(
                MockFailingOutputFormat.class, tempTestFile.toURI().toString(), stubParams);

        boolean stubFailed = false;

        try {
            testTask.invoke();
        } catch (Exception e) {
            stubFailed = true;
        }
        assertThat(stubFailed).withFailMessage("Function exception was not forwarded.").isTrue();

        // assert that temp file was removed
        assertThat(tempTestFile)
                .withFailMessage("Temp output file has not been removed")
                .doesNotExist();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testFailingSortingDataSinkTask() {

        int keyCnt = 100;
        int valCnt = 20;
        double memoryFraction = 1.0;

        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        super.addInput(new UniformRecordGenerator(keyCnt, valCnt, true), 0);

        DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);
        Configuration stubParams = new Configuration();

        // set sorting
        super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
        super.getTaskConfig()
                .setInputComparator(
                        new RecordComparatorFactory(new int[] {1}, (new Class[] {IntValue.class})),
                        0);
        super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
        super.getTaskConfig().setFilehandlesInput(0, 8);
        super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
        super.registerFileOutputTask(
                MockFailingOutputFormat.class, tempTestFile.toURI().toString(), stubParams);

        boolean stubFailed = false;

        try {
            testTask.invoke();
        } catch (Exception e) {
            stubFailed = true;
        }
        assertThat(stubFailed).withFailMessage("Function exception was not forwarded.").isTrue();

        // assert that temp file was removed
        assertThat(tempTestFile)
                .withFailMessage("Temp output file has not been removed")
                .doesNotExist();
    }

    @Test
    void testCancelDataSinkTask() throws Exception {
        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        super.addInput(new InfiniteInputIterator(), 0);

        final DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);
        Configuration stubParams = new Configuration();

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());

        super.registerFileOutputTask(
                MockOutputFormat.class, tempTestFile.toURI().toString(), stubParams);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testTask.invoke();
                        } catch (Exception ie) {
                            ie.printStackTrace();
                            fail("Task threw exception although it was properly canceled");
                        }
                    }
                };
        taskRunner.start();

        // wait until the task created the file
        long deadline = System.currentTimeMillis() + 60000;
        while (!tempTestFile.exists() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(tempTestFile)
                .withFailMessage("Task did not create file within 60 seconds")
                .exists();

        // cancel the task
        Thread.sleep(500);
        testTask.cancel();
        taskRunner.interrupt();

        // wait for the canceling to complete
        taskRunner.join();

        // assert that temp file was created
        assertThat(tempTestFile)
                .withFailMessage("Task did not create file within 60 seconds")
                .doesNotExist();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCancelSortingDataSinkTask() {
        double memoryFraction = 1.0;

        super.initEnvironment(MEMORY_MANAGER_SIZE, NETWORK_BUFFER_SIZE);
        super.addInput(new InfiniteInputIterator(), 0);

        final DataSinkTask<Record> testTask = new DataSinkTask<>(this.mockEnv);
        Configuration stubParams = new Configuration();

        // set sorting
        super.getTaskConfig().setInputLocalStrategy(0, LocalStrategy.SORT);
        super.getTaskConfig()
                .setInputComparator(
                        new RecordComparatorFactory(new int[] {1}, (new Class[] {IntValue.class})),
                        0);
        super.getTaskConfig().setRelativeMemoryInput(0, memoryFraction);
        super.getTaskConfig().setFilehandlesInput(0, 8);
        super.getTaskConfig().setSpillingThresholdInput(0, 0.8f);

        File tempTestFile = new File(tempFolder.toFile(), UUID.randomUUID().toString());
        super.registerFileOutputTask(
                MockOutputFormat.class, tempTestFile.toURI().toString(), stubParams);

        Thread taskRunner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            testTask.invoke();
                        } catch (Exception ie) {
                            ie.printStackTrace();
                            fail("Task threw exception although it was properly canceled");
                        }
                    }
                };
        taskRunner.start();

        TaskCancelThread tct = new TaskCancelThread(2, taskRunner, testTask);
        tct.start();

        try {
            tct.join();
            taskRunner.join();
        } catch (InterruptedException ie) {
            fail("Joining threads failed");
        }
    }

    public static class MockOutputFormat extends FileOutputFormat<Record> {
        private static final long serialVersionUID = 1L;

        final StringBuilder bld = new StringBuilder();

        @Override
        public void configure(Configuration parameters) {
            super.configure(parameters);
        }

        @Override
        public void writeRecord(Record rec) throws IOException {
            IntValue key = rec.getField(0, IntValue.class);
            IntValue value = rec.getField(1, IntValue.class);

            this.bld.setLength(0);
            this.bld.append(key.getValue());
            this.bld.append('_');
            this.bld.append(value.getValue());
            this.bld.append('\n');

            byte[] bytes = this.bld.toString().getBytes(ConfigConstants.DEFAULT_CHARSET);

            this.stream.write(bytes);
        }
    }

    public static class MockFailingOutputFormat extends MockOutputFormat {
        private static final long serialVersionUID = 1L;

        int cnt = 0;

        @Override
        public void configure(Configuration parameters) {
            super.configure(parameters);
        }

        @Override
        public void writeRecord(Record rec) throws IOException {
            if (++this.cnt >= 10) {
                throw new RuntimeException("Expected Test Exception");
            }
            super.writeRecord(rec);
        }
    }
}
