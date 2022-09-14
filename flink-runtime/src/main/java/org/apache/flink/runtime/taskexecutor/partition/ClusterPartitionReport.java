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

package org.apache.flink.runtime.taskexecutor.partition;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current status of all cluster partitions of the TaskExecutor, describing which
 * partitions are available.
 */
public class ClusterPartitionReport implements Serializable {

    private static final long serialVersionUID = -3150175198722481689L;

    private final Collection<ClusterPartitionReportEntry> entries;

    public ClusterPartitionReport(final Collection<ClusterPartitionReportEntry> entries) {
        this.entries = checkNotNull(entries);
    }

    public Collection<ClusterPartitionReportEntry> getEntries() {
        return entries;
    }

    @Override
    public String toString() {
        return "PartitionReport{" + "entries=" + entries + '}';
    }

    /** An entry describing all partitions belonging to one dataset. */
    public static class ClusterPartitionReportEntry implements Serializable {

        private static final long serialVersionUID = -666517548300250601L;

        private final IntermediateDataSetID dataSetId;
        private final Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors;
        private final int numTotalPartitions;

        public ClusterPartitionReportEntry(
                IntermediateDataSetID dataSetId,
                int numTotalPartitions,
                Map<ResultPartitionID, ShuffleDescriptor> shuffleDescriptors) {
            Preconditions.checkNotNull(dataSetId);
            Preconditions.checkNotNull(shuffleDescriptors);
            Preconditions.checkArgument(!shuffleDescriptors.isEmpty());
            Preconditions.checkArgument(numTotalPartitions > 0);
            Preconditions.checkState(shuffleDescriptors.size() <= numTotalPartitions);

            this.dataSetId = dataSetId;
            this.numTotalPartitions = numTotalPartitions;
            this.shuffleDescriptors = shuffleDescriptors;
        }

        public IntermediateDataSetID getDataSetId() {
            return dataSetId;
        }

        public Set<ResultPartitionID> getHostedPartitions() {
            return shuffleDescriptors.keySet();
        }

        public int getNumTotalPartitions() {
            return numTotalPartitions;
        }

        public Map<ResultPartitionID, ShuffleDescriptor> getShuffleDescriptors() {
            return shuffleDescriptors;
        }
    }
}
