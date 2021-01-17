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

package org.apache.flink.yarn.testjob;

import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Testing job for {@link org.apache.flink.runtime.jobmaster.JobMaster} failover. Covering stream
 * case that have a infinite source and a sink, scheduling by EAGER mode, with PIPELINED edges.
 */
public class YarnTestJob {

    public static JobGraph stoppableJob(final StopJobSignal stopJobSignal) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new InfiniteSourceFunction(stopJobSignal))
                .setParallelism(2)
                .shuffle()
                .addSink(new DiscardingSink<>())
                .setParallelism(2);

        return env.getStreamGraph().getJobGraph();
    }

    /** Helper class to signal between multiple processes that a job should stop. */
    public static class StopJobSignal implements Serializable {

        private final String stopJobMarkerFile;

        public static StopJobSignal usingMarkerFile(final Path stopJobMarkerFile) {
            return new StopJobSignal(stopJobMarkerFile.toString());
        }

        private StopJobSignal(final String stopJobMarkerFile) {
            this.stopJobMarkerFile = stopJobMarkerFile;
        }

        /** Signals that the job should stop. */
        public void signal() {
            try {
                Files.delete(Paths.get(stopJobMarkerFile));
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }

        /** True if job should stop. */
        public boolean isSignaled() {
            return !Files.exists(Paths.get(stopJobMarkerFile));
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    private static final class InfiniteSourceFunction extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = -8758033916372648233L;

        private boolean running;

        private final StopJobSignal stopJobSignal;

        InfiniteSourceFunction(final StopJobSignal stopJobSignal) {
            this.running = true;
            this.stopJobSignal = stopJobSignal;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running && !stopJobSignal.isSignaled()) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(0);
                }

                Thread.sleep(5L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
