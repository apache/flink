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

package org.apache.flink.state.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/** Test Rescaling benchmark. */
public class RescalingBenchmarkTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testScalingOut() throws Exception {

        RescalingBenchmark<Integer> benchmark =
                new RescalingBenchmarkBuilder<Integer>()
                        .setMaxParallelism(128)
                        .setParallelismBefore(1)
                        .setParallelismAfter(2)
                        .setManagedMemorySize(512 * 1024 * 1024)
                        .setCheckpointStorageAccess(
                                new FileSystemCheckpointStorage(temporaryFolder.newFolder().toURI())
                                        .createCheckpointStorage(new JobID()))
                        .setStateBackend(new EmbeddedRocksDBStateBackend(true))
                        .setStreamRecordGenerator(new IntegerRecordGenerator())
                        .setStateProcessFunctionSupplier(TestKeyedFunction::new)
                        .build();
        benchmark.setUp();
        benchmark.prepareStateForOperator(0);
        benchmark.rescale();
        benchmark.closeOperator();
        benchmark.tearDown();
    }

    @Test
    public void testScalingIn() throws Exception {
        RescalingBenchmark<Integer> benchmark =
                new RescalingBenchmarkBuilder<Integer>()
                        .setMaxParallelism(128)
                        .setParallelismBefore(2)
                        .setParallelismAfter(1)
                        .setManagedMemorySize(512 * 1024 * 1024)
                        .setCheckpointStorageAccess(
                                new FileSystemCheckpointStorage(temporaryFolder.newFolder().toURI())
                                        .createCheckpointStorage(new JobID()))
                        .setStateBackend(new EmbeddedRocksDBStateBackend(true))
                        .setStreamRecordGenerator(new IntegerRecordGenerator())
                        .setStateProcessFunctionSupplier(TestKeyedFunction::new)
                        .build();
        benchmark.setUp();
        benchmark.prepareStateForOperator(0);
        benchmark.rescale();
        benchmark.closeOperator();
        benchmark.tearDown();
    }

    private static class IntegerRecordGenerator
            implements RescalingBenchmark.StreamRecordGenerator<Integer> {
        private final int numberOfKeys = 1000;
        private int count = 0;

        @Override
        public Iterator<StreamRecord<Integer>> generate() {
            return new Iterator<StreamRecord<Integer>>() {
                @Override
                public boolean hasNext() {
                    return count < numberOfKeys;
                }

                @Override
                public StreamRecord<Integer> next() {
                    count += 1;
                    return new StreamRecord<>(ThreadLocalRandom.current().nextInt(), 0);
                }
            };
        }

        @Override
        public TypeInformation getTypeInformation() {
            return BasicTypeInfo.INT_TYPE_INFO;
        }
    }

    private static class TestKeyedFunction extends KeyedProcessFunction<Integer, Integer, Void> {

        private static final long serialVersionUID = 1L;
        private ValueState<Integer> randomState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            randomState =
                    this.getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("RandomState", Integer.class));
        }

        @Override
        public void processElement(
                Integer value,
                KeyedProcessFunction<Integer, Integer, Void>.Context ctx,
                Collector<Void> out)
                throws Exception {
            randomState.update(ThreadLocalRandom.current().nextInt());
        }
    }
}
