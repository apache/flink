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

package org.apache.flink.api.functions;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.Serializable;

/* The test cases are originally from the Apache Spark project. Like the ClosureCleaner itself. */
class ClosureCleanerITCase {

    @TempDir private java.nio.file.Path tempPath;

    private String resultPath;

    private String result;

    @BeforeEach
    void before() throws Exception {
        File tempFolder = TempDirUtils.newFolder(tempPath);
        resultPath = tempFolder.toURI().toString();
    }

    @AfterEach
    void after() throws Exception {
        TestBaseUtils.compareResultsByLinesInMemory(result, resultPath);
    }

    @Test
    void testObject() throws Exception {
        TestObject.run(resultPath);
        result = "30";
    }

    @Test
    void testClass() throws Exception {
        new TestClass().run(resultPath);
        result = "30";
    }

    @Test
    void testClassWithoutDefaulConstructor() throws Exception {
        new TestClassWithoutDefaultConstructor(5).run(resultPath);
        result = "30";
    }

    @Test
    void testClassWithoutFieldAccess() throws Exception {
        new TestClassWithoutFieldAccess().run(resultPath);
        result = "30";
    }

    static class NonSerializable {}

    static class TestObject {
        public static void run(String resultPath) throws Exception {
            NonSerializable nonSer = new NonSerializable();
            int x = 5;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStreamSource<Long> nums = env.fromSequence(1, 4);
            nums.map(num -> num + x)
                    .setParallelism(1)
                    .fullWindowPartition()
                    .reduce((ReduceFunction<Long>) Long::sum)
                    .sinkTo(
                            FileSink.forRowFormat(
                                            new Path(resultPath), new SimpleStringEncoder<Long>())
                                    .build());

            env.execute();
        }
    }

    static class TestClass implements Serializable {

        private int x = 5;

        private int getX() {
            return x;
        }

        public void run(String resultPath) throws Exception {
            NonSerializable nonSer = new NonSerializable();
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStreamSource<Long> nums = env.fromSequence(1, 4);
            nums.map(num -> num + getX())
                    .setParallelism(1)
                    .fullWindowPartition()
                    .reduce((ReduceFunction<Long>) Long::sum)
                    .sinkTo(
                            FileSink.forRowFormat(
                                            new Path(resultPath), new SimpleStringEncoder<Long>())
                                    .build());
            env.execute();
        }
    }

    static class TestClassWithoutDefaultConstructor {

        private int x;

        private NonSerializable nonSer = new NonSerializable();

        public TestClassWithoutDefaultConstructor(int x) {
            this.x = x;
        }

        public void run(String resultPath) throws Exception {
            NonSerializable nonSer2 = new NonSerializable();
            int x = 5;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStreamSource<Long> nums = env.fromSequence(1, 4);
            nums.map(num -> num + x)
                    .setParallelism(1)
                    .fullWindowPartition()
                    .reduce((ReduceFunction<Long>) Long::sum)
                    .sinkTo(
                            FileSink.forRowFormat(
                                            new Path(resultPath), new SimpleStringEncoder<Long>())
                                    .build());
            env.execute();
        }
    }

    static class TestClassWithoutFieldAccess {

        private NonSerializable nonSer = new NonSerializable();

        public void run(String resultPath) throws Exception {
            NonSerializable nonSer2 = new NonSerializable();
            int x = 5;
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStreamSource<Long> nums = env.fromSequence(1, 4);
            nums.map(num -> num + x)
                    .setParallelism(1)
                    .fullWindowPartition()
                    .reduce((ReduceFunction<Long>) Long::sum)
                    .sinkTo(
                            FileSink.forRowFormat(
                                            new Path(resultPath), new SimpleStringEncoder<Long>())
                                    .build());

            env.execute();
        }
    }
}
