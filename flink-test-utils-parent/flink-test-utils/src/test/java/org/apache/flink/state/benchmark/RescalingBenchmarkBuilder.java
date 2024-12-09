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
 * limitations under the License
 */

package org.apache.flink.state.benchmark;

import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.state.rocksdb.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Preconditions;

import java.util.function.Supplier;

/** Builder for rescalingBenchmark. */
public class RescalingBenchmarkBuilder<KEY> {
    private int maxParallelism = 128;
    private int parallelismBefore = 2;
    private int parallelismAfter = 1;
    private int managedMemorySize = 512 * 1024 * 1024;
    private StateBackend stateBackend = new EmbeddedRocksDBStateBackend();
    private RescalingBenchmark.StreamRecordGenerator<KEY> streamRecordGenerator;
    private Supplier<KeyedProcessFunction<KEY, KEY, Void>> stateProcessFunctionSupplier;
    private CheckpointStorageAccess checkpointStorageAccess;

    public RescalingBenchmarkBuilder<KEY> setMaxParallelism(int maxParallelism) {
        this.maxParallelism = maxParallelism;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setParallelismBefore(int parallelismBefore) {
        this.parallelismBefore = parallelismBefore;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setParallelismAfter(int parallelismAfter) {
        this.parallelismAfter = parallelismAfter;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setManagedMemorySize(int managedMemorySize) {
        this.managedMemorySize = managedMemorySize;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setStateBackend(StateBackend stateBackend) {
        this.stateBackend = stateBackend;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setStreamRecordGenerator(
            RescalingBenchmark.StreamRecordGenerator<KEY> generator) {
        this.streamRecordGenerator = generator;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setStateProcessFunctionSupplier(
            Supplier<KeyedProcessFunction<KEY, KEY, Void>> supplier) {
        this.stateProcessFunctionSupplier = supplier;
        return this;
    }

    public RescalingBenchmarkBuilder<KEY> setCheckpointStorageAccess(
            CheckpointStorageAccess checkpointStorageAccess) {
        this.checkpointStorageAccess = checkpointStorageAccess;
        return this;
    }

    public RescalingBenchmark<KEY> build() {
        return new RescalingBenchmark<KEY>(
                parallelismBefore,
                parallelismAfter,
                maxParallelism,
                managedMemorySize,
                stateBackend,
                Preconditions.checkNotNull(checkpointStorageAccess),
                Preconditions.checkNotNull(streamRecordGenerator),
                Preconditions.checkNotNull(stateProcessFunctionSupplier));
    }
}
