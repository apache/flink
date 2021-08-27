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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Network throughput benchmarks for data skew scenario executed by the external <a
 * href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class DataSkewStreamNetworkThroughputBenchmark extends StreamNetworkThroughputBenchmark {

    @Override
    protected void setChannelSelector(
            RecordWriterBuilder recordWriterBuilder, boolean broadcastMode) {
        checkArgument(!broadcastMode, "Combining broadcasting with data skew doesn't make sense");
        recordWriterBuilder.setChannelSelector(new DataSkewChannelSelector());
    }

    /**
     * A {@link ChannelSelector} which selects channel 0 for nearly all records. And all other
     * channels except for channel 0 will be only selected at most once.
     */
    private static class DataSkewChannelSelector implements ChannelSelector {
        private int numberOfChannels;
        private int channelIndex = 0;

        @Override
        public void setup(int numberOfChannels) {
            this.numberOfChannels = numberOfChannels;
        }

        @Override
        public int selectChannel(IOReadableWritable record) {
            if (channelIndex >= numberOfChannels) {
                return 0;
            }
            return channelIndex++;
        }

        @Override
        public boolean isBroadcast() {
            return false;
        }
    }
}
