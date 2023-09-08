/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.state.api.functions.KeyedStateBootstrapFunction;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.state.api.utils.JobResultRetriever;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.configuration.CheckpointingOptions.FS_SMALL_FILE_THRESHOLD;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertThat;

/** Test the savepoint deep copy. */
@RunWith(value = Parameterized.class)
public class SavepointDeepCopyTest extends AbstractTestBase {

    private static final MemorySize FILE_STATE_SIZE_THRESHOLD = new MemorySize(1);

    private static final String TEXT = "The quick brown fox jumps over the lazy dog";
    private static final String RANDOM_VALUE = RandomStringUtils.randomAlphanumeric(120);

    private final StateBackend backend;

    public SavepointDeepCopyTest(StateBackend backend) throws Exception {
        this.backend = backend;
    }

    @Parameterized.Parameters(name = "State Backend: {0}")
    public static Collection<StateBackend> data() {
        return Arrays.asList(new HashMapStateBackend(), new EmbeddedRocksDBStateBackend());
    }

    /** To bootstrapper a savepoint for testing. */
    static class WordMapBootstrapper extends KeyedStateBootstrapFunction<String, String> {
        private ValueState<Tuple2<String, String>> state;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Tuple2<String, String>> descriptor =
                    new ValueStateDescriptor<>("state", Types.TUPLE(Types.STRING, Types.STRING));
            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(String value, Context ctx) throws Exception {
            if (state.value() == null) {
                state.update(new Tuple2<>(value, RANDOM_VALUE));
            }
        }
    }

    /** To read the state back from the newly created savepoint. */
    static class ReadFunction extends KeyedStateReaderFunction<String, Tuple2<String, String>> {

        private ValueState<Tuple2<String, String>> state;

        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Tuple2<String, String>> stateDescriptor =
                    new ValueStateDescriptor<>("state", Types.TUPLE(Types.STRING, Types.STRING));
            state = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            throw new UnsupportedOperationException(
                    "This method is deprecated and shouldn't be invoked. Please use open(OpenContext) instead.");
        }

        @Override
        public void readKey(String key, Context ctx, Collector<Tuple2<String, String>> out)
                throws Exception {
            out.collect(state.value());
        }
    }

    /**
     * Test savepoint deep copy. This method tests the savepoint deep copy by:
     *
     * <ul>
     *   <li>create {@code savepoint1} with operator {@code Operator1}, make sure it has more state
     *       files in addition to _metadata
     *   <li>create {@code savepoint2} from {@code savepoint1} by adding a new operator {@code
     *       Operator2}
     *   <li>check all state files in {@code savepoint1}'s directory are copied over to {@code
     *       savepoint2}'s directory
     *   <li>read the state of {@code Operator1} from {@code savepoint2} and make sure the number of
     *       the keys remain same
     * </ul>
     *
     * @throws Exception throw exceptions when anything goes wrong
     */
    @Test
    public void testSavepointDeepCopy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> words = env.fromElements(TEXT.split(" "));

        StateBootstrapTransformation<String> transformation =
                OperatorTransformation.bootstrapWith(words)
                        .keyBy(e -> e)
                        .transform(new WordMapBootstrapper());

        File savepointUrl1 = createAndRegisterTempFile(new AbstractID().toHexString());
        String savepointPath1 = savepointUrl1.getPath();

        SavepointWriter.newSavepoint(env, backend, 128)
                .withConfiguration(FS_SMALL_FILE_THRESHOLD, FILE_STATE_SIZE_THRESHOLD)
                .withOperator(OperatorIdentifier.forUid("Operator1"), transformation)
                .write(savepointPath1);

        env.execute("bootstrap savepoint1");

        Set<String> stateFiles1 = getFileNamesInDirectory(Paths.get(savepointPath1));

        Assert.assertTrue(
                "Failed to bootstrap savepoint1 with additional state files",
                stateFiles1.size() > 1);

        // create savepoint2 from savepoint1 created above
        File savepointUrl2 = createAndRegisterTempFile(new AbstractID().toHexString());
        String savepointPath2 = savepointUrl2.getPath();

        SavepointWriter savepoint2 =
                SavepointWriter.fromExistingSavepoint(env, savepointPath1, backend)
                        .withConfiguration(FS_SMALL_FILE_THRESHOLD, FILE_STATE_SIZE_THRESHOLD);

        savepoint2
                .withOperator(OperatorIdentifier.forUid("Operator2"), transformation)
                .write(savepointPath2);
        env.execute("create savepoint2");

        Set<String> stateFiles2 = getFileNamesInDirectory(Paths.get(savepointPath1));

        Assert.assertTrue(
                "Failed to create savepoint2 from savepoint1 with additional state files",
                stateFiles2.size() > 1);

        assertThat(
                "At least one state file in savepoint1 are not in savepoint2",
                stateFiles1,
                everyItem(isIn(stateFiles2)));

        // Try to fromExistingSavepoint savepoint2 and read the state of "Operator1" (which has not
        // been
        // touched/changed when savepoint2
        // was created) and make sure the number of keys remain same
        long actuallyKeyNum =
                JobResultRetriever.collect(
                                SavepointReader.read(env, savepointPath2, backend)
                                        .readKeyedState(
                                                OperatorIdentifier.forUid("Operator1"),
                                                new ReadFunction()))
                        .size();

        long expectedKeyNum = Arrays.stream(TEXT.split(" ")).distinct().count();
        Assert.assertEquals(
                "Unexpected number of keys in the state of Operator1",
                expectedKeyNum,
                actuallyKeyNum);
    }

    private static Set<String> getFileNamesInDirectory(Path path) throws IOException {
        try (Stream<Path> files = Files.list(path)) {
            return files.map(file -> file.getFileName().toString()).collect(Collectors.toSet());
        }
    }
}
