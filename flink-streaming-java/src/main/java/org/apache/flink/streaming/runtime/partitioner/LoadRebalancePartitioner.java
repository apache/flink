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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.io.network.partition.SubpartitionStatistic;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Partitioner that distributes the data equally by cycling through the output channels.
 *
 * @param <T> Type of the elements in the Stream being rebalanced
 */
@Internal
public class LoadRebalancePartitioner<T> extends StreamPartitioner<T> {
    private static final long serialVersionUID = 1L;

    private int nextChannelToSendTo;

    private List<Tuple2<Integer, SubpartitionStatistic>> subpartitionStatistics;

    @Override
    public void optionalSetup(ResultPartitionWriter writer) {
        super.optionalSetup(writer);

        List<SubpartitionStatistic> statistics = writer.getSubpartitionsStatistics();
        subpartitionStatistics = new ArrayList<>(statistics.size());
        for (int i = 0; i < statistics.size(); i++) {
            subpartitionStatistics.add(new Tuple2<>(i, statistics.get(i)));
        }

        nextChannelToSendTo =
                ThreadLocalRandom.current().nextInt(writer.getNumberOfSubpartitions());
    }

    @Override
    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        nextChannel();

        SubpartitionStatistic currentSubpart = subpartitionStatistics.get(0).f1;
        SubpartitionStatistic newSubpart = subpartitionStatistics.get(nextChannelToSendTo).f1;

        chooseLessLoadedSubPartition(currentSubpart, newSubpart);

        return subpartitionStatistics.get(0).f0;
    }

    private void chooseLessLoadedSubPartition(
            SubpartitionStatistic currentSubpart, SubpartitionStatistic newSubpart) {
        long bytesInQueueForCurrent =
                currentSubpart.getTotalNumberOfReceivedBytes()
                        - currentSubpart.getTotalNumberOfSentBytes();
        long bytesInQueueForNew =
                newSubpart.getTotalNumberOfReceivedBytes() - newSubpart.getTotalNumberOfSentBytes();
        if (bytesInQueueForNew <= bytesInQueueForCurrent) {
            swap(subpartitionStatistics, 0, nextChannelToSendTo);
        }
    }

    private static void swap(
            List<Tuple2<Integer, SubpartitionStatistic>> statistics, int first, int second) {
        Tuple2<Integer, SubpartitionStatistic> firstStat = statistics.get(first);
        statistics.set(first, statistics.get(second));
        statistics.set(second, firstStat);
    }

    private void nextChannel() {
        nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels;
        if (nextChannelToSendTo == 0 && subpartitionStatistics.size() > 1) {
            nextChannel();
        }
    }

    @Override
    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.ROUND_ROBIN;
    }

    public StreamPartitioner<T> copy() {
        return this;
    }

    @Override
    public boolean isPointwise() {
        return false;
    }

    @Override
    public String toString() {
        return "LOAD_REBALANCE";
    }
}
