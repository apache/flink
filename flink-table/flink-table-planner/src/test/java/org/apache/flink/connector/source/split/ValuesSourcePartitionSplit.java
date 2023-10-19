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

package org.apache.flink.connector.source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.connector.source.DynamicFilteringValuesSource;
import org.apache.flink.connector.source.FinishingLogic;

import java.util.Map;

/** The split of the {@link DynamicFilteringValuesSource}. */
public class ValuesSourcePartitionSplit implements SourceSplit {

    private final Map<String, String> partition;
    private final boolean isInfinite;

    public ValuesSourcePartitionSplit(Map<String, String> partition) {
        this(partition, FinishingLogic.FINITE);
    }

    public ValuesSourcePartitionSplit(
            Map<String, String> partition, FinishingLogic finishingLogic) {
        this.partition = partition;
        this.isInfinite = finishingLogic == FinishingLogic.INFINITE;
    }

    public Map<String, String> getPartition() {
        return partition;
    }

    public boolean isInfinite() {
        return isInfinite;
    }

    @Override
    public String splitId() {
        return partition.toString();
    }

    @Override
    public String toString() {
        return "ValuesSourcePartitionSplit{" + "partition='" + partition + '\'' + '}';
    }
}
