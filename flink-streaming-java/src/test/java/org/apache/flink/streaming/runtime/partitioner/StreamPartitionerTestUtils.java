/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;

/** Utility class to test {@link StreamPartitioner}. */
public class StreamPartitionerTestUtils {

    public static JobGraph createJobGraph(
            String sourceSlotSharingGroup,
            String sinkSlotSharingGroup,
            StreamPartitioner<Long> streamPartitioner) {
        return createJobGraph(
                sourceSlotSharingGroup,
                sinkSlotSharingGroup,
                streamPartitioner,
                StreamExchangeMode.UNDEFINED);
    }

    public static JobGraph createJobGraph(
            String sourceSlotSharingGroup,
            String sinkSlotSharingGroup,
            StreamPartitioner<Long> streamPartitioner,
            StreamExchangeMode exchangeMode) {

        Configuration configuration = new Configuration();
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final DataStream<Long> source =
                env.fromSequence(0, 99).slotSharingGroup(sourceSlotSharingGroup).name("source");

        setPartitioner(source, streamPartitioner, exchangeMode)
                .sinkTo(new DiscardingSink<>())
                .slotSharingGroup(sinkSlotSharingGroup)
                .name("sink");

        return env.getStreamGraph().getJobGraph();
    }

    private static <T> DataStream<T> setPartitioner(
            DataStream<T> dataStream,
            StreamPartitioner<T> partitioner,
            StreamExchangeMode exchangeMode) {
        return new DataStream<T>(
                dataStream.getExecutionEnvironment(),
                new PartitionTransformation<T>(
                        dataStream.getTransformation(), partitioner, exchangeMode));
    }

    /** Utility class, should not be instantiated. */
    private StreamPartitionerTestUtils() {}
}
