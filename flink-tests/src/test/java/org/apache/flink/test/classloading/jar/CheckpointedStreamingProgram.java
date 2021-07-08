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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Collections;
import java.util.List;

/**
 * A simple streaming program, which is using the state checkpointing of Flink. It is using a user
 * defined class as the state.
 */
@SuppressWarnings("serial")
public class CheckpointedStreamingProgram {

    private static final int CHECKPOINT_INTERVALL = 100;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(CHECKPOINT_INTERVALL);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 100L));
        env.disableOperatorChaining();

        DataStream<String> text = env.addSource(new SimpleStringGenerator());
        text.map(new StatefulMapper()).addSink(new DiscardingSink<>());
        env.setParallelism(1);
        env.execute("Checkpointed Streaming Program");
    }

    // with Checkpointing
    private static class SimpleStringGenerator
            implements SourceFunction<String>, ListCheckpointed<Integer> {
        public boolean running = true;

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while (running) {
                Thread.sleep(1);
                ctx.collect("someString");
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.emptyList();
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {}
    }

    private static class StatefulMapper
            implements MapFunction<String, String>,
                    ListCheckpointed<StatefulMapper>,
                    CheckpointListener {

        private String someState;
        private boolean atLeastOneSnapshotComplete = false;
        private boolean restored = false;

        @Override
        public List<StatefulMapper> snapshotState(long checkpointId, long timestamp)
                throws Exception {
            return Collections.singletonList(this);
        }

        @Override
        public void restoreState(List<StatefulMapper> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            restored = true;
            StatefulMapper s = state.get(0);
            this.someState = s.someState;
            this.atLeastOneSnapshotComplete = s.atLeastOneSnapshotComplete;
        }

        @Override
        public String map(String value) throws Exception {
            if (!atLeastOneSnapshotComplete) {
                // throttle consumption by the checkpoint interval until we have one snapshot.
                Thread.sleep(CHECKPOINT_INTERVALL);
            }
            if (atLeastOneSnapshotComplete && !restored) {
                throw new RuntimeException("Intended failure, to trigger restore");
            }
            if (restored) {
                throw new SuccessException();
                // throw new RuntimeException("All good");
            }
            someState = value; // update our state
            return value;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            atLeastOneSnapshotComplete = true;
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {}
    }
    // --------------------------------------------------------------------------------------------

    /** We intentionally use a user specified failure exception. */
    private static class SuccessException extends Exception {}
}
