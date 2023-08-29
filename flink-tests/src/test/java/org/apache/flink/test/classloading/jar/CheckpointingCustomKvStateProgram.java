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

package org.apache.flink.test.classloading.jar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/** Test class used by the {@link org.apache.flink.test.classloading.ClassLoaderITCase}. */
public class CheckpointingCustomKvStateProgram {

    public static void main(String[] args) throws Exception {
        final String checkpointPath = args[0];
        final String outputPath = args[1];
        final int parallelism = 1;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);
        env.enableCheckpointing(100);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 1000));
        env.setStateBackend(new FsStateBackend(checkpointPath));

        DataStream<Integer> source = env.addSource(new InfiniteIntegerSource());
        source.map(
                        new MapFunction<Integer, Tuple2<Integer, Integer>>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Tuple2<Integer, Integer> map(Integer value) throws Exception {
                                return new Tuple2<>(
                                        ThreadLocalRandom.current().nextInt(parallelism), value);
                            }
                        })
                .keyBy(
                        new KeySelector<Tuple2<Integer, Integer>, Integer>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public Integer getKey(Tuple2<Integer, Integer> value) throws Exception {
                                return value.f0;
                            }
                        })
                .flatMap(new ReducingStateFlatMap())
                .writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);

        env.execute();
    }

    private static class InfiniteIntegerSource
            implements ParallelSourceFunction<Integer>, ListCheckpointed<Integer> {
        private static final long serialVersionUID = -7517574288730066280L;
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            int counter = 0;
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(counter++);
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(0);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {}
    }

    private static class ReducingStateFlatMap
            extends RichFlatMapFunction<Tuple2<Integer, Integer>, Integer>
            implements ListCheckpointed<ReducingStateFlatMap>, CheckpointListener {

        private static final long serialVersionUID = -5939722892793950253L;
        private transient ReducingState<Integer> kvState;

        private boolean atLeastOneSnapshotComplete = false;
        private boolean restored = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ReducingStateDescriptor<Integer> stateDescriptor =
                    new ReducingStateDescriptor<>(
                            "reducing-state", new ReduceSum(), CustomIntSerializer.INSTANCE);

            this.kvState = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Integer> out)
                throws Exception {
            kvState.add(value.f1);

            if (atLeastOneSnapshotComplete) {
                if (restored) {
                    throw new SuccessException();
                } else {
                    throw new RuntimeException("Intended failure, to trigger restore");
                }
            }
        }

        @Override
        public List<ReducingStateFlatMap> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this);
        }

        @Override
        public void restoreState(List<ReducingStateFlatMap> state) throws Exception {
            restored = true;
            atLeastOneSnapshotComplete = true;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            atLeastOneSnapshotComplete = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}

        private static class ReduceSum implements ReduceFunction<Integer> {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;
            }
        }
    }

    private static final class CustomIntSerializer extends TypeSerializerSingleton<Integer> {

        private static final long serialVersionUID = 4572452915892737448L;

        public static final TypeSerializer<Integer> INSTANCE = new CustomIntSerializer();

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public Integer createInstance() {
            return 0;
        }

        @Override
        public Integer copy(Integer from) {
            return from;
        }

        @Override
        public Integer copy(Integer from, Integer reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return 4;
        }

        @Override
        public void serialize(Integer record, DataOutputView target) throws IOException {
            target.writeInt(record.intValue());
        }

        @Override
        public Integer deserialize(DataInputView source) throws IOException {
            return Integer.valueOf(source.readInt());
        }

        @Override
        public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
            return Integer.valueOf(source.readInt());
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            target.writeInt(source.readInt());
        }

        // -----------------------------------------------------------------------------------

        @Override
        public TypeSerializerSnapshot<Integer> snapshotConfiguration() {
            return new CustomIntSerializerSnapshot();
        }

        /** Serializer configuration snapshot for compatibility and format evolution. */
        @SuppressWarnings("WeakerAccess")
        public static final class CustomIntSerializerSnapshot
                extends SimpleTypeSerializerSnapshot<Integer> {

            public CustomIntSerializerSnapshot() {
                super(() -> INSTANCE);
            }
        }
    }
}
